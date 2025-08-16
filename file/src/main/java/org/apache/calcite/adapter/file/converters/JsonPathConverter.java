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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Converter that extracts data from JSON files using path expressions.
 * This allows creating derived JSON files from source JSON files.
 * 
 * Note: This is a simplified implementation using Jackson's JsonNode navigation.
 * For full JSONPath support, the jayway jsonpath library would need to be added as a dependency.
 */
public class JsonPathConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonPathConverter.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  
  /**
   * Extracts data from a JSON file using a path expression and writes it to an output file.
   * 
   * @param sourceJson The source JSON file to extract from
   * @param outputFile The output JSON file
   * @param path The path expression to apply (simplified JSONPath-like syntax)
   * @throws IOException if extraction fails
   */
  public static void extract(File sourceJson, File outputFile, String path) throws IOException {
    LOGGER.debug("Extracting from {} using path: {}", sourceJson.getName(), path);
    
    // Read JSON file into JsonNode
    JsonNode jsonData = JSON_MAPPER.readTree(sourceJson);
    
    // Apply path extraction
    JsonNode extractedData = extractPath(jsonData, path);
    
    if (extractedData == null || extractedData.isNull()) {
      LOGGER.warn("Path {} not found in {}", path, sourceJson.getName());
      extractedData = JSON_MAPPER.createObjectNode(); // Empty object
    }
    
    // Write output as JSON using atomic write operation
    String jsonOutput = JSON_MAPPER.writerWithDefaultPrettyPrinter()
        .writeValueAsString(extractedData);
    
    // Write to temporary file first for atomic operation
    File tempFile = new File(outputFile.getAbsolutePath() + ".tmp." + Thread.currentThread().threadId());
    
    try (FileWriter writer = new FileWriter(tempFile, StandardCharsets.UTF_8)) {
      writer.write(jsonOutput);
      writer.flush();
    }
    
    // Atomic rename (on most filesystems)
    java.nio.file.Files.move(tempFile.toPath(), outputFile.toPath(),
        java.nio.file.StandardCopyOption.REPLACE_EXISTING,
        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    
    LOGGER.debug("Wrote extracted JSON data to {} ({} bytes)", outputFile.getName(), jsonOutput.length());
    
    // Record the conversion for refresh tracking
    ConversionRecorder.recordConversion(sourceJson, outputFile, 
        "JSONPATH_EXTRACTION[" + path + "]");
  }
  
  /**
   * Extracts data from a JsonNode using a path expression.
   * This is a shared implementation used by both JSON and YAML converters.
   * 
   * Supports basic paths like:
   * - "fieldName" - extracts a top-level field
   * - "field.nested" - extracts a nested field using dot notation
   * - "field[0]" - extracts array element
   * - "$.field.nested" - JSONPath-style with root operator
   * 
   * @param node The JsonNode to extract from
   * @param path The path expression
   * @return The extracted JsonNode, or null if not found
   */
  public static JsonNode extractPath(JsonNode node, String path) {
    // Remove leading $ if present (JSONPath compatibility)
    if (path.startsWith("$.")) {
      path = path.substring(2);
    } else if (path.startsWith("$")) {
      path = path.substring(1);
    }
    
    // Handle empty path (return entire node)
    if (path.isEmpty()) {
      return node;
    }
    
    // Split by dots but handle array notation
    String[] parts = path.split("\\.");
    JsonNode current = node;
    
    for (String part : parts) {
      if (current == null || current.isNull()) {
        return null;
      }
      
      // Check for array notation
      if (part.contains("[") && part.contains("]")) {
        int bracketIndex = part.indexOf('[');
        String fieldName = part.substring(0, bracketIndex);
        String indexStr = part.substring(bracketIndex + 1, part.indexOf(']'));
        
        // Navigate to field
        if (!fieldName.isEmpty()) {
          current = current.get(fieldName);
        }
        
        // Navigate to array index
        if (current != null && current.isArray()) {
          try {
            int index = Integer.parseInt(indexStr);
            current = current.get(index);
          } catch (NumberFormatException e) {
            return null;
          }
        } else {
          return null;
        }
      } else {
        // Simple field navigation
        current = current.get(part);
      }
    }
    
    return current;
  }
}