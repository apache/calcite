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
package org.apache.calcite.adapter.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Dow 30 SEC filing downloads.
 */
@Tag("integration")
public class Dow30DownloadTest {
  @Test public void testDow30Downloads() throws Exception {
    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("DOW 30 SEC FILING DOWNLOAD PERFORMANCE TEST");
    System.out.println("Base Directory: /Volumes/T9/sec-data");
    System.out.println("=".repeat(80) + "\n");

    Instant start = Instant.now();

    // Configuration
    File targetDir = new File("/Volumes/T9/sec-data/dow30-test");
    targetDir.mkdirs();

    Map<String, Object> edgarConfig = new HashMap<>();
    edgarConfig.put("ciks", "DOW30");
    edgarConfig.put("startDate", "2023-01-01");
    edgarConfig.put("endDate", "2023-12-31");
    edgarConfig.put("filingTypes", Arrays.asList("10-K", "10-Q", "8-K"));

    System.out.println("Downloading filings for Dow 30 companies for 2023...");
    System.out.println("Filing types: 10-K, 10-Q, 8-K");
    System.out.println("");

    // Download filings
    EdgarDownloader downloader = new EdgarDownloader(edgarConfig, targetDir);
    List<File> downloadedFiles = downloader.downloadFilings();

    Duration elapsed = Duration.between(start, Instant.now());

    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("RESULTS:");
    System.out.println("  Files downloaded: " + downloadedFiles.size());
    System.out.println("  Total time: " + elapsed.toSeconds() + " seconds");
    System.out.println("  Average per file: " +
                       String.format("%.2f", elapsed.toMillis() / 1000.0 / Math.max(1, downloadedFiles.size())) +
                       " seconds");
    System.out.println("  Data saved to: " + targetDir.getAbsolutePath());
    System.out.println("=".repeat(80) + "\n");
  }
}
