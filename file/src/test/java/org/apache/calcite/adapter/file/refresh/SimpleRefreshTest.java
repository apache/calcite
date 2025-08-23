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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.converters.HtmlToJsonConverter;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple test to verify HTML conversion and re-conversion works.
 */
@Tag("unit")
public class SimpleRefreshTest {

  @TempDir
  Path tempDir;
  
  private File schemaDir;
  
  @BeforeEach
  public void setup() throws Exception {
    schemaDir = tempDir.toFile();
    // Metadata now stored directly in schemaDir
  }
  
  @AfterEach
  public void cleanup() throws Exception {
    // No longer need to reset central metadata directory
  }
  
  @Test
  public void testHtmlConversionAndReconversion() throws Exception {
    System.out.println("\n=== TEST: Simple HTML Conversion and Re-conversion ===");
    
    // Create initial HTML file
    File htmlFile = new File(schemaDir, "data.html");
    try (FileWriter writer = new FileWriter(htmlFile, StandardCharsets.UTF_8)) {
      writer.write("<html><body>\n");
      writer.write("<table>\n");
      writer.write("<tr><th>id</th><th>name</th></tr>\n");
      writer.write("<tr><td>1</td><td>Alice</td></tr>\n");
      writer.write("</table>\n");
      writer.write("</body></html>\n");
    }
    
    // Convert to JSON
    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, schemaDir);
    assertEquals(1, jsonFiles.size());
    File jsonFile = jsonFiles.get(0);
    
    // Read and verify initial JSON content
    String jsonContent = Files.readString(jsonFile.toPath());
    ObjectMapper mapper = new ObjectMapper();
    List<Map<String, Object>> data = mapper.readValue(jsonContent, List.class);
    assertEquals(1, data.size());
    assertEquals(1, data.get(0).get("id")); // HTML converter infers numeric types
    assertEquals("Alice", data.get(0).get("name"));
    
    System.out.println("Initial conversion: 1 row with Alice");
    
    // Update HTML file
    Thread.sleep(100); // Small delay to ensure timestamp changes
    try (FileWriter writer = new FileWriter(htmlFile, StandardCharsets.UTF_8)) {
      writer.write("<html><body>\n");
      writer.write("<table>\n");
      writer.write("<tr><th>id</th><th>name</th></tr>\n");
      writer.write("<tr><td>2</td><td>Bob</td></tr>\n");
      writer.write("<tr><td>3</td><td>Charlie</td></tr>\n");
      writer.write("</table>\n");
      writer.write("</body></html>\n");
    }
    
    // Re-convert to JSON
    jsonFiles = HtmlToJsonConverter.convert(htmlFile, schemaDir);
    assertEquals(1, jsonFiles.size());
    jsonFile = jsonFiles.get(0);
    
    // Read and verify updated JSON content
    jsonContent = Files.readString(jsonFile.toPath());
    data = mapper.readValue(jsonContent, List.class);
    assertEquals(2, data.size());
    assertEquals(2, data.get(0).get("id")); // HTML converter infers numeric types
    assertEquals("Bob", data.get(0).get("name"));
    assertEquals(3, data.get(1).get("id")); // HTML converter infers numeric types
    assertEquals("Charlie", data.get(1).get("name"));
    
    System.out.println("✅ Re-conversion successful: 2 rows with Bob and Charlie");
    
    // Verify metadata is updated
    ConversionMetadata metadata = new ConversionMetadata(schemaDir);
    File foundSource = metadata.findOriginalSource(jsonFile);
    assertNotNull(foundSource);
    assertEquals(htmlFile.getCanonicalPath(), foundSource.getCanonicalPath());
    
    System.out.println("✅ Metadata correctly tracks source after re-conversion");
  }
}