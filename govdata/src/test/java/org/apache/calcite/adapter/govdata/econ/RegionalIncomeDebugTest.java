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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Debug test for BEA regional income download issues.
 */
@Tag("integration")
public class RegionalIncomeDebugTest {
  
  @BeforeAll
  static void setUp() {
    TestEnvironmentLoader.ensureLoaded();
  }
  
  @Test
  public void testRegionalIncomeDownload() throws Exception {
    // Use a temp directory for testing
    Path tempDir = Files.createTempDirectory("regional-test");
    String cacheDir = tempDir.toString();
    
    String apiKey = System.getenv("BEA_API_KEY");
    if (apiKey == null) {
      apiKey = System.getProperty("BEA_API_KEY");
    }
    
    if (apiKey == null) {
      fail("BEA_API_KEY not found in environment or system properties");
    }
    
    System.out.println("Testing with BEA API key: " + apiKey.substring(0, 4) + "...");
    
    BeaDataDownloader downloader = new BeaDataDownloader(
        cacheDir,
        apiKey,
        StorageProviderFactory.createFromUrl(cacheDir)
    );
    
    // Test download for 2023
    System.out.println("Downloading regional income for 2023...");
    downloader.downloadRegionalIncomeForYear(2023);
    
    // Check if file was created
    File jsonFile = new File(cacheDir, "source=econ/type=indicators/year=2023/regional_income.json");
    assertTrue(jsonFile.exists(), "Regional income JSON file should exist");
    
    // Check file size
    long fileSize = jsonFile.length();
    System.out.println("JSON file size: " + fileSize + " bytes");
    
    // Read and check content
    String content = Files.readString(jsonFile.toPath());
    System.out.println("First 500 chars of content: " + content.substring(0, Math.min(500, content.length())));
    
    // Check if it has actual data
    assertTrue(content.contains("regional_income"), "File should contain regional_income field");
    assertTrue(fileSize > 100, "File should have substantial data (not just empty array)");
    
    // Test parquet conversion
    File parquetDir = new File(tempDir.toString(), "parquet");
    parquetDir.mkdirs();
    File parquetFile = new File(parquetDir, "regional_income.parquet");
    
    System.out.println("Converting to parquet...");
    downloader.convertRegionalIncomeToParquet(
        new File(cacheDir, "source=econ/type=indicators/year=2023"),
        parquetFile
    );
    
    assertTrue(parquetFile.exists(), "Parquet file should be created");
    System.out.println("Parquet file size: " + parquetFile.length() + " bytes");
  }
}