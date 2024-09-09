package org.apache.calcite.adapter.file;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

public class ExcelToJsonConverter {

  public static void convertFileToJson(File inputFile) throws IOException {
    FileInputStream file = new FileInputStream(inputFile);
    Workbook workbook = new XSSFWorkbook(file);
    ObjectMapper mapper = new ObjectMapper();
    FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
    String fileName = inputFile.getName();
    String baseName = toPascalCase(fileName.substring(0, fileName.lastIndexOf('.')));
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
        for (Cell cell : row) {
          Cell headerCell = headerRow.getCell(cell.getColumnIndex());
          if (headerCell == null || headerCell.getCellType() == CellType.BLANK) {
            continue; // Skip columns with blank header cells
          }
          String key = getCellValue(headerCell, evaluator);
          String value = getCellValue(cell, evaluator);
          if (!value.isEmpty()) {
            rowData.put(key, value);
          }
        }
        sheetData.add(rowData);
      }
      // Write JSON file
      String sheetName = toPascalCase(sheet.getSheetName());
      String jsonFileName = baseName + "__" + sheetName + ".json";
      FileWriter fileWriter = new FileWriter(new File(inputFile.getParent(), jsonFileName));
      mapper.writerWithDefaultPrettyPrinter().writeValue(fileWriter, sheetData);
      fileWriter.close();
    }
    workbook.close();
    file.close();
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

  private static String toPascalCase(String input) {
    StringBuilder result = new StringBuilder();
    boolean capitalizeNext = true;
    for (char c : input.toCharArray()) {
      if (Character.isWhitespace(c) || c == '_' || c == '-') {
        capitalizeNext = true;
      } else if (capitalizeNext) {
        result.append(Character.toUpperCase(c));
        capitalizeNext = false;
      } else {
        result.append(Character.toLowerCase(c));
      }
    }
    return result.toString();
  }
}
