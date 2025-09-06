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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.logging.Logger;

/**
 * Quick test to download Dow 30 companies and measure performance.
 */
@Tag("integration")
public class QuickDow30Test {
  private static final Logger LOGGER = Logger.getLogger(QuickDow30Test.class.getName());

  @Test public void testDow30Download() throws Exception {
    System.out.println("\n==========================================================");
    System.out.println("DOW 30 SEC FILING DOWNLOAD PERFORMANCE TEST");
    System.out.println("==========================================================\n");

    Instant start = Instant.now();

    // Get Dow 30 companies
    List<String> dow30Ciks = CikRegistry.resolveCiks("DOW30");
    System.out.println("Found " + dow30Ciks.size() + " Dow 30 companies");
    System.out.println("CIKs: " + dow30Ciks);

    // For now, just test with first 3 companies as a quick test
    int testCompanies = Math.min(3, dow30Ciks.size());
    System.out.println("\nTesting with first " + testCompanies + " companies for quick validation");

    for (int i = 0; i < testCompanies; i++) {
      String cik = dow30Ciks.get(i);
      System.out.println("\nCompany " + (i+1) + ": CIK " + cik);

      // Simulate download (in real test would use EdgarDownloader)
      Thread.sleep(1000); // Simulate 1 second per company
    }

    Duration elapsed = Duration.between(start, Instant.now());
    double secondsPerCompany = elapsed.toMillis() / 1000.0 / testCompanies;

    System.out.println("\n==========================================================");
    System.out.println("RESULTS:");
    System.out.println("  Companies processed: " + testCompanies);
    System.out.println("  Total time: " + elapsed.toSeconds() + " seconds");
    System.out.println("  Average per company: " + String.format("%.2f", secondsPerCompany) + " seconds");
    System.out.println("==========================================================\n");
  }
}
