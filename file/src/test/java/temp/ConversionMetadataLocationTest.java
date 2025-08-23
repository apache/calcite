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
package temp;

import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test to verify that .conversions.json metadata files are written to
 * schema directories (baseDirectory) rather than conversions subdirectories.
 */
@Tag("temp")
public class ConversionMetadataLocationTest {

  @TempDir
  Path tempDir;

  private File schemaDir;
  private File conversionsDir;

  @BeforeEach
  void setUp() throws Exception {
    // Create directory structure: tempDir/.aperio/myschema/
    File aperioDir = tempDir.resolve(".aperio").toFile();
    aperioDir.mkdirs();
    
    schemaDir = new File(aperioDir, "myschema");
    schemaDir.mkdirs();
    
    // Create conversions subdirectory
    conversionsDir = new File(schemaDir, "conversions");
    conversionsDir.mkdirs();
  }

  @Test
  void testConversionsJsonInSchemaDirectory() throws Exception {
    // Create ConversionMetadata using schemaDir as baseDirectory
    ConversionMetadata metadata = new ConversionMetadata(schemaDir);
    
    // Record a conversion to trigger .conversions.json creation
    File sourceFile = new File(schemaDir, "source.xlsx");
    File convertedFile = new File(conversionsDir, "converted.json");
    
    // Create dummy files
    sourceFile.createNewFile();
    convertedFile.createNewFile();
    
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        sourceFile.getAbsolutePath(), convertedFile.getAbsolutePath(), "EXCEL_TO_JSON");
    
    metadata.recordConversion(convertedFile, record);
    
    // Check that .conversions.json is created in schema directory, not conversions directory
    File conversionsJsonInSchema = new File(schemaDir, ".conversions.json");
    File conversionsJsonInConversions = new File(conversionsDir, ".conversions.json");
    
    // The metadata file should be in the schema directory
    assertTrue(conversionsJsonInSchema.exists(), 
        "conversions.json should be in schema directory: " + schemaDir.getAbsolutePath());
    
    // The metadata file should NOT be in the conversions directory
    assertFalse(conversionsJsonInConversions.exists(),
        "conversions.json should NOT be in conversions directory: " + conversionsDir.getAbsolutePath());
  }
  
  @Test
  void testDirectoryStructure() {
    // Verify our test setup is correct
    assertTrue(schemaDir.exists(), "Schema directory should exist");
    assertTrue(conversionsDir.exists(), "Conversions directory should exist");
    
    // Verify the directory relationship
    assertTrue(conversionsDir.getParentFile().equals(schemaDir),
        "Conversions dir should be child of schema dir");
  }
}