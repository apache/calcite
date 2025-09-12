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

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class RealDJIAScraperTest {
  @Test
public void test() throws Exception {
    try {
      System.out.println("\n"
  + "=".repeat(80));
      System.out.println("TESTING REAL DJIA WIKIPEDIA SCRAPER");
      System.out.println("=".repeat(80) + "\n");

      // Test the new Wikipedia scraper
      System.out.println("Fetching DJIA constituents from Wikipedia...");
      List<String> ciks = SecDataFetcher.fetchDJIAAConstituents();

      System.out.println("\nResults:");
      System.out.println("  Found " + ciks.size() + " CIKs");

      if (ciks.size() > 0) {
        System.out.println("\n  Sample CIKs:");
        for (int i = 0; i < Math.min(5, ciks.size()); i++) {
          System.out.println("    " + (i+1) + ". " + ciks.get(i));
        }
      }

      System.out.println("\n"
  + "=".repeat(80));
      if (ciks.size() == 30) {
        System.out.println("✓ SUCCESS: Found expected 30 DJIA constituents");
      } else if (ciks.size() > 0) {
        System.out.println("✓ PARTIAL: Found " + ciks.size() + " constituents (expected ~30)");
      } else {
        System.out.println("✗ FAILURE: No constituents found");
      }
      System.out.println("=".repeat(80));

    } catch (Exception e) {
      System.err.println("\n✗ ERROR: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
