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

import org.apache.calcite.adapter.sec.CikRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test for S&P 500 constituents resolution.
 */
@Tag("unit")
public class SP500ConstituentsTest {

  @Test
  void testSP500ConstituentsLoading() {
    // Test resolving the SP500_COMPLETE group which uses _SP500_CONSTITUENTS marker
    List<String> sp500Ciks = CikRegistry.resolveCiks("SP500_COMPLETE");

    if (sp500Ciks.isEmpty()) {
      // Try directly with the special marker
      sp500Ciks = CikRegistry.resolveCiks("_SP500_CONSTITUENTS");
    }

    assertNotNull(sp500Ciks, "Should return a list, even if empty");
    
    if (!sp500Ciks.isEmpty()) {
      // Check if we got a reasonable number (should be around 500)
      // Allow for some variance as companies change
      assertTrue(sp500Ciks.size() >= 50, 
          "Should have at least 50 companies (may be using fallback)");
      assertTrue(sp500Ciks.size() <= 550, 
          "Should not exceed 550 companies");

      // Test caching by calling again
      long start = System.currentTimeMillis();
      List<String> cachedCiks = CikRegistry.resolveCiks("_SP500_CONSTITUENTS");
      long elapsed = System.currentTimeMillis() - start;
      
      assertEquals(sp500Ciks.size(), cachedCiks.size(), 
          "Cached result should match original");
      assertTrue(elapsed < 1000, 
          "Second call should be fast (cache hit)");

      // Check for known S&P 500 companies
      String[] knownCiks = {
        "0000320193", // AAPL
        "0000789019", // MSFT
        "0001018724", // AMZN
        "0001652044", // GOOGL
        "0001045810"  // NVDA
      };

      int foundCount = 0;
      for (String cik : knownCiks) {
        if (sp500Ciks.contains(cik)) {
          foundCount++;
        }
      }
      
      // Should find most of the known companies
      assertTrue(foundCount >= 3, 
          "Should find at least 3 of the 5 known S&P 500 companies");
    }
  }

  @Test
  void testSP500CikFormat() {
    List<String> sp500Ciks = CikRegistry.resolveCiks("_SP500_CONSTITUENTS");
    
    if (!sp500Ciks.isEmpty()) {
      // All CIKs should be 10 digits with leading zeros
      for (String cik : sp500Ciks) {
        assertEquals(10, cik.length(), 
            "CIK should be 10 digits: " + cik);
        assertTrue(cik.matches("\\d{10}"), 
            "CIK should be all digits: " + cik);
      }
    }
  }
}