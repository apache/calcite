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

import org.apache.calcite.adapter.file.metadata.ConversionMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Utility to record file conversions for all converters.
 * This ensures that refresh mechanisms can track back to original sources.
 */
public class ConversionRecorder {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConversionRecorder.class);
  
  /**
   * Records a file conversion for refresh tracking.
   * This should be called by all converters after successfully creating output files.
   * 
   * @param originalFile The source file (e.g., Excel, HTML, XML)
   * @param convertedFile The output file (typically JSON)
   * @param conversionType Type of conversion (e.g., "EXCEL_TO_JSON", "HTML_TO_JSON")
   */
  public static void recordConversion(File originalFile, File convertedFile, String conversionType) {
    try {
      ConversionMetadata metadata = new ConversionMetadata(originalFile.getParentFile());
      metadata.recordConversion(originalFile, convertedFile, conversionType);
      LOGGER.debug("Recorded {} conversion: {} -> {}", 
          conversionType, originalFile.getName(), convertedFile.getName());
    } catch (Exception e) {
      // Don't fail the conversion if metadata recording fails
      LOGGER.warn("Failed to record conversion metadata for {}: {}", 
          convertedFile.getName(), e.getMessage());
    }
  }
  
  /**
   * Records an Excel to JSON conversion.
   */
  public static void recordExcelConversion(File excelFile, File jsonFile) {
    recordConversion(excelFile, jsonFile, "EXCEL_TO_JSON");
  }
  
  /**
   * Records an HTML to JSON conversion.
   */
  public static void recordHtmlConversion(File htmlFile, File jsonFile) {
    recordConversion(htmlFile, jsonFile, "HTML_TO_JSON");
  }
  
  /**
   * Records an XML to JSON conversion.
   */
  public static void recordXmlConversion(File xmlFile, File jsonFile) {
    recordConversion(xmlFile, jsonFile, "XML_TO_JSON");
  }
  
  /**
   * Records a JSONPath extraction (JSON to JSON).
   */
  public static void recordJsonPathExtraction(File sourceJson, File extractedJson, String jsonPath) {
    recordConversion(sourceJson, extractedJson, "JSONPATH_EXTRACTION[" + jsonPath + "]");
  }
}