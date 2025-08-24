/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.converters;

import org.apache.calcite.adapter.file.cache.SourceFileLockManager;
import org.apache.calcite.adapter.file.util.SmartCasing;
import org.apache.calcite.util.trace.CalciteLogger;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * Utility class for converting Excel files to JSON format.
 * Supports reading XLSX files and converting each sheet to a separate JSON file.
 */
public final class ExcelToJsonConverter {
  private static final CalciteLogger LOGGER =
      new CalciteLogger(LoggerFactory.getLogger(ExcelToJsonConverter.class));

  private ExcelToJsonConverter() {
    // Prevent instantiation
  }

  public static void convertFileToJson(File inputFile, File outputDir, String tableNameCasing, String columnNameCasing, File baseDirectory) throws IOException {
    convertFileToJson(inputFile, outputDir, tableNameCasing, columnNameCasing, baseDirectory, null);
  }
  
  public static void convertFileToJson(File inputFile, File outputDir, String tableNameCasing, String columnNameCasing, File baseDirectory, String relativePath) throws IOException {
    // Acquire read lock on source file
    SourceFileLockManager.LockHandle lockHandle = null;
    try {
      lockHandle = SourceFileLockManager.acquireReadLock(inputFile);
      LOGGER.debug("Acquired read lock on Excel file: " + inputFile.getPath());
    } catch (IOException e) {
      LOGGER.warn("Could not acquire lock on file: " + inputFile.getPath()
          + " - proceeding without lock");
      // Continue without lock
    }

    FileInputStream file = new FileInputStream(inputFile);
    Workbook workbook = WorkbookFactory.create(file);
    ObjectMapper mapper = new ObjectMapper();
    FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
    String fileName = inputFile.getName();
    String rawBaseName = fileName.substring(0, fileName.lastIndexOf('.'));
    
    // If relativePath is provided, include directory structure in the base name
    if (relativePath != null && relativePath.contains(File.separator)) {
      String dirPrefix = relativePath.substring(0, relativePath.lastIndexOf(File.separator))
          .replace(File.separator, "_");
      rawBaseName = dirPrefix + "_" + rawBaseName;
    }
    
    String baseName = SmartCasing.applyCasing(rawBaseName, tableNameCasing);
    for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
      Sheet sheet = workbook.getSheetAt(i);
      ArrayNode sheetData = mapper.createArrayNode();
      Iterator<Row> rowIterator = sheet.iterator();
      // Get the header row
      Row headerRow = null;
      while (rowIterator.hasNext()) {
        headerRow = rowIterator.next();
        if (isNonEmptyRow(headerRow)) {
          break;
        }
      }
      if (headerRow == null) {
        continue; // Skip empty sheets
      }
      // Process data rows
      while (rowIterator.hasNext()) {
        Row row = rowIterator.next();
        if (!isNonEmptyRow(row)) {
          continue;
        }
        ObjectNode rowData = mapper.createObjectNode();

        // Iterate through all columns in header to maintain consistent structure
        int lastColNum = headerRow.getLastCellNum();

        for (int colIndex = 0; colIndex < lastColNum; colIndex++) {
          Cell headerCell = headerRow.getCell(colIndex);
          if (headerCell == null || headerCell.getCellType() == CellType.BLANK) {
            continue; // Skip columns with blank header cells
          }

          String rawKey = getCellValue(headerCell, evaluator);
          String key = ConverterUtils.sanitizeIdentifier(SmartCasing.applyCasing(rawKey, columnNameCasing));
          Cell dataCell = row.getCell(colIndex);
          Object value = getCellValueAsObject(dataCell, evaluator);

          if (value != null) {
            rowData.putPOJO(key, value);
          } else {
            // Use null for empty cells to maintain consistent JSON structure
            // This is semantically correct and allows proper nullable column handling
            rowData.putNull(key);
          }
        }
        sheetData.add(rowData);
      }
      // Write JSON file
      String rawSheetName = sheet.getSheetName();
      String sheetName = SmartCasing.applyCasing(rawSheetName, tableNameCasing);
      String jsonFileName = baseName + "__" + sheetName + ".json";
      File jsonFile = new File(outputDir, jsonFileName);
      FileWriter fileWriter =
          new FileWriter(jsonFile, StandardCharsets.UTF_8);
      mapper.writerWithDefaultPrettyPrinter().writeValue(fileWriter, sheetData);
      fileWriter.close();

      // Record the conversion for refresh tracking
      ConversionRecorder.recordExcelConversion(inputFile, jsonFile, baseDirectory);
    }
    workbook.close();
    file.close();

    // Release the lock
    if (lockHandle != null) {
      lockHandle.close();
      LOGGER.debug("Released read lock on Excel file");
    }
  }

  private static boolean isNonEmptyRow(Row row) {
    for (Cell cell : row) {
      if (cell.getCellType() != CellType.BLANK) {
        return true;
      }
    }
    return false;
  }

  private static String getCellValue(Cell cell, FormulaEvaluator evaluator) {
    if (cell == null) {
      return "";
    }
    switch (cell.getCellType()) {
    case STRING:
      return cell.getStringCellValue();
    case NUMERIC:
      if (DateUtil.isCellDateFormatted(cell)) {
        return cell.getDateCellValue().toString();
      } else {
        return String.valueOf(cell.getNumericCellValue());
      }
    case BOOLEAN:
      return String.valueOf(cell.getBooleanCellValue());
    case FORMULA:
      return getCellValue(evaluator.evaluateInCell(cell), evaluator);
    default:
      return "";
    }
  }

  private static Object getCellValueAsObject(Cell cell, FormulaEvaluator evaluator) {
    if (cell == null) {
      return null;
    }
    switch (cell.getCellType()) {
    case STRING:
      String stringValue = cell.getStringCellValue();
      return stringValue.isEmpty() ? null : stringValue;
    case NUMERIC:
      if (DateUtil.isCellDateFormatted(cell)) {
        return cell.getDateCellValue();
      } else {
        double numericValue = cell.getNumericCellValue();
        // Return as integer if it's a whole number, otherwise as double
        if (numericValue == (long) numericValue) {
          return (long) numericValue;
        } else {
          return numericValue;
        }
      }
    case BOOLEAN:
      return cell.getBooleanCellValue();
    case FORMULA:
      return getCellValueAsObject(evaluator.evaluateInCell(cell), evaluator);
    case BLANK:
      return null;
    default:
      return null;
    }
  }

  private static String toPascalCase(String input) {
    if (input == null || input.isEmpty()) {
      return input;
    }

    StringBuilder result = new StringBuilder();
    boolean nextUpper = true;

    for (char c : input.toCharArray()) {
      if (Character.isLetterOrDigit(c)) {
        if (nextUpper) {
          result.append(Character.toUpperCase(c));
          nextUpper = false;
        } else {
          result.append(Character.toLowerCase(c));
        }
      } else {
        nextUpper = true;
      }
    }

    return result.toString();
  }

}
