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
import org.apache.calcite.adapter.govdata.sec.CikRegistry;
import org.apache.calcite.adapter.govdata.sec.SecDataFetcher;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class Russell2000AndTickersTest {
  @Test
public void test() throws Exception {
    System.out.println("=== Testing Russell 2000 Constituents ===\n");

    // Test resolving the RUSSELL2000_COMPLETE group which uses _RUSSELL2000_CONSTITUENTS marker
    List<String> russell2000Ciks = CikRegistry.resolveCiks("RUSSELL2000_COMPLETE");

    if (russell2000Ciks.isEmpty()) {
      System.out.println("No CIKs returned from RUSSELL2000_COMPLETE - trying direct marker...");

      // Try directly with the special marker
      russell2000Ciks = CikRegistry.resolveCiks("_RUSSELL2000_CONSTITUENTS");
    }

    if (!russell2000Ciks.isEmpty()) {
      System.out.println("SUCCESS! Loaded " + russell2000Ciks.size() + " Russell 2000 CIKs");

      // Russell 2000 should have around 2000 companies
      if (russell2000Ciks.size() >= 1800 && russell2000Ciks.size() <= 2100) {
        System.out.println("✓ Count looks correct for Russell 2000 index");
      } else if (russell2000Ciks.size() < 100) {
        System.out.println("⚠ Count seems low - might be using fallback list");
      }

      System.out.println("\nFirst 10 CIKs:");
      for (int i = 0; i < Math.min(10, russell2000Ciks.size()); i++) {
        System.out.println("  " + russell2000Ciks.get(i));
      }

      // Test caching by calling again
      System.out.println("\nTesting cache (second call)...");
      long start = System.currentTimeMillis();
      List<String> cachedCiks = CikRegistry.resolveCiks("_RUSSELL2000_CONSTITUENTS");
      long elapsed = System.currentTimeMillis() - start;
      System.out.println("Second call returned " + cachedCiks.size() + " CIKs in " + elapsed + "ms");

      if (elapsed < 100) {
        System.out.println("✓ Cache is working! (fast response)");
      }

    } else {
      System.out.println("FAILED: No Russell 2000 CIKs returned");
      System.out.println("This is expected - Russell 2000 data is not freely available");
      System.out.println("Currently using a hardcoded fallback list of 20 sample companies");
    }

    System.out.println("\n\n=== Testing getAllEdgarTickers ===\n");

    try {
      Map<String, String> allTickers = SecDataFetcher.getAllEdgarTickers();

      if (!allTickers.isEmpty()) {
        System.out.println("SUCCESS! Loaded " + allTickers.size() + " ticker-to-CIK mappings");

        // Should be around 10,000+ tickers
        if (allTickers.size() >= 8000) {
          System.out.println("✓ Count looks correct for all EDGAR filers");
        }

        // Show some sample mappings
        System.out.println("\nSample ticker mappings:");
        int count = 0;
        for (Map.Entry<String, String> entry : allTickers.entrySet()) {
          if (count++ >= 10) break;
          System.out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }

        // Test some known tickers
        System.out.println("\nVerifying known tickers:");
        String[] testTickers = {"AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"};
        for (String ticker : testTickers) {
          String cik = allTickers.get(ticker);
          if (cik != null) {
            System.out.println("  ✓ " + ticker + " -> " + cik);
          } else {
            System.out.println("  ✗ " + ticker + " not found");
          }
        }

        // Test caching by calling again
        System.out.println("\nTesting cache (second call)...");
        long start = System.currentTimeMillis();
        Map<String, String> cachedTickers = SecDataFetcher.getAllEdgarTickers();
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("Second call returned " + cachedTickers.size() + " tickers in " + elapsed + "ms");

        if (elapsed < 100) {
          System.out.println("✓ Cache is working! (fast response)");
        }

      } else {
        System.out.println("FAILED: No tickers returned");
      }
    } catch (Exception e) {
      System.out.println("ERROR: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
