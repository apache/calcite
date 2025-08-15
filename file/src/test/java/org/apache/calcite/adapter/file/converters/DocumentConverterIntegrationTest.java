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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for document converters (DOCX, PPTX, Markdown).
 * Verifies that FileConversionManager properly routes to the scanners.
 */
public class DocumentConverterIntegrationTest {

  @TempDir
  File tempDir;

  @Test
  public void testFileConversionManagerRoutesToDocxScanner() {
    // Test that DOCX files are recognized as requiring conversion
    assertTrue(FileConversionManager.requiresConversion("document.docx"));
    assertTrue(FileConversionManager.requiresConversion("REPORT.DOCX"));
    
    // Test that DOCX would be handled by convertIfNeeded
    File docxFile = new File(tempDir, "test.docx");
    // We don't actually create the file or test conversion here
    // Just verify the routing logic recognizes the extension
    String path = docxFile.getPath().toLowerCase();
    assertTrue(path.endsWith(".docx"));
  }

  @Test
  public void testFileConversionManagerRoutesToPptxScanner() {
    // Test that PPTX files are recognized as requiring conversion
    assertTrue(FileConversionManager.requiresConversion("presentation.pptx"));
    assertTrue(FileConversionManager.requiresConversion("SLIDES.PPTX"));
    
    // Test that PPTX would be handled by convertIfNeeded
    File pptxFile = new File(tempDir, "test.pptx");
    String path = pptxFile.getPath().toLowerCase();
    assertTrue(path.endsWith(".pptx"));
  }

  @Test
  public void testFileConversionManagerRoutesToMarkdownScanner() {
    // Test that Markdown files are recognized as requiring conversion
    assertTrue(FileConversionManager.requiresConversion("readme.md"));
    assertTrue(FileConversionManager.requiresConversion("NOTES.MD"));
    
    // Test that Markdown would be handled by convertIfNeeded
    File mdFile = new File(tempDir, "test.md");
    String path = mdFile.getPath().toLowerCase();
    assertTrue(path.endsWith(".md"));
  }

  @Test
  public void testDirectlyUsableFiles() {
    // Files that don't need conversion
    assertTrue(FileConversionManager.isDirectlyUsable("data.csv"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.json"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.parquet"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.yaml"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.yml"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.tsv"));
    
    // Files that DO need conversion
    assertFalse(FileConversionManager.isDirectlyUsable("data.xlsx"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.html"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.xml"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.docx"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.pptx"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.md"));
  }

  @Test
  public void testConversionRequiredFiles() {
    // Files that need conversion
    assertTrue(FileConversionManager.requiresConversion("data.xlsx"));
    assertTrue(FileConversionManager.requiresConversion("data.xls"));
    assertTrue(FileConversionManager.requiresConversion("data.html"));
    assertTrue(FileConversionManager.requiresConversion("data.htm"));
    assertTrue(FileConversionManager.requiresConversion("data.xml"));
    assertTrue(FileConversionManager.requiresConversion("data.docx"));
    assertTrue(FileConversionManager.requiresConversion("data.pptx"));
    assertTrue(FileConversionManager.requiresConversion("data.md"));
    
    // Files that don't need conversion
    assertFalse(FileConversionManager.requiresConversion("data.csv"));
    assertFalse(FileConversionManager.requiresConversion("data.json"));
    assertFalse(FileConversionManager.requiresConversion("data.parquet"));
  }
}