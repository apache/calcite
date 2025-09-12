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
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test EDGAR downloading functionality.
 */
@Tag("integration")
public class EdgarDownloadTest {

  private static final String BASE_DIR = "apple-edgar-test-data";

  @Test public void testDownloadApple10K() throws Exception {
    System.out.println("\n=== Testing SEC EDGAR Download for Apple ===");

    // Use a constant directory that persists across test runs
    File baseDir = new File(BASE_DIR);
    if (!baseDir.exists()) {
      baseDir.mkdirs();
    }

    // Configure EDGAR source - download all filing types for 10 years
    Map<String, Object> edgarConfig = new HashMap<>();
    edgarConfig.put("cik", "0000320193"); // Apple
    edgarConfig.put("filingTypes", Arrays.asList("10-K", "10-Q", "8-K", "DEF 14A", "20-F"));
    edgarConfig.put("startYear", 2014);
    edgarConfig.put("endYear", 2023);
    edgarConfig.put("autoDownload", true);

    File targetDir = new File(baseDir, "sec");
    if (!targetDir.exists()) {
      targetDir.mkdirs();
    }

    System.out.println("Downloading to: " + targetDir.getAbsolutePath());

    // Download filings
    EdgarDownloader downloader = new EdgarDownloader(edgarConfig, targetDir);
    List<File> downloaded = downloader.downloadFilings();

    System.out.println("Downloaded " + downloaded.size() + " files:");
    for (File file : downloaded) {
      System.out.println("  - " + file.getName() + " (" + file.length() + " bytes)");
      assertTrue(file.exists(), "File should exist: " + file.getName());
      assertTrue(file.length() > 0, "File should not be empty: " + file.getName());
    }

    assertFalse(downloaded.isEmpty(), "Should have downloaded files");

    // Process to Parquet
    File parquetDir = new File(baseDir, "parquet");
    SecToParquetConverter converter = new SecToParquetConverter(targetDir, parquetDir);
    int processed = converter.processAllSecFiles();

    System.out.println("\nProcessed " + processed + " XBRL files to Parquet");

    // Check Parquet files
    System.out.println("\nParquet files created:");
    checkParquetFiles(parquetDir, "");

    assertTrue(processed > 0, "Should have processed files");
  }

  private void checkParquetFiles(File dir, String indent) {
    if (!dir.exists()) {
      return;
    }

    File[] files = dir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          System.out.println(indent + file.getName() + "/");
          checkParquetFiles(file, indent + "  ");
        } else if (file.getName().endsWith(".parquet")) {
          System.out.println(indent + file.getName() + " (" + file.length() + " bytes)");
        }
      }
    }
  }
}
