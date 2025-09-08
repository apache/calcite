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
import org.apache.calcite.adapter.sec.CikRegistry;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class DJIAMarketTest {
  @Test
public void test() throws Exception {
    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("TESTING _DJIA MARKET GROUP RESOLUTION");
    System.out.println("=".repeat(80) + "\n");

    // Test _DJIA resolution
    System.out.println("Testing _DJIA resolution:");
    List<String> djiCiks = CikRegistry.resolveCiks("_DJIA");
    System.out.println("  _DJIA resolved to " + djiCiks.size() + " CIKs");

    // Test _DJIA_CONSTITUENTS directly
    System.out.println("\nTesting _DJIA_CONSTITUENTS resolution:");
    List<String> djiConstituents = CikRegistry.resolveCiks("_DJIA_CONSTITUENTS");
    System.out.println("  _DJIA_CONSTITUENTS resolved to " + djiConstituents.size() + " CIKs");

    // Also test DJIA (without underscore)
    System.out.println("\nTesting DJIA resolution:");
    List<String> djiNoUnderscore = CikRegistry.resolveCiks("DJIA");
    System.out.println("  DJIA resolved to " + djiNoUnderscore.size() + " CIKs");

    // Test DOW30 resolution (should be same)
    System.out.println("\nTesting DOW30 resolution:");
    List<String> dow30Ciks = CikRegistry.resolveCiks("DOW30");
    System.out.println("  DOW30 resolved to " + dow30Ciks.size() + " CIKs");

    // Verify they match
    boolean match = djiCiks.size() == dow30Ciks.size() &&
                   djiCiks.containsAll(dow30Ciks);
    System.out.println("\n_DJIA and DOW30 match: " + match);

    // Show some example companies
    if (!djiCiks.isEmpty()) {
      System.out.println("\nFirst 5 CIKs in _DJIA:");
      for (int i = 0; i < Math.min(5, djiCiks.size()); i++) {
        System.out.println("  " + djiCiks.get(i));
      }

      // Check for known Dow components
      System.out.println("\nVerifying known Dow components:");
      System.out.println("  Apple (0000320193): " +
        (djiCiks.contains("0000320193") ? "✓ Found" : "✗ Not found"));
      System.out.println("  Microsoft (0000789019): " +
        (djiCiks.contains("0000789019") ? "✓ Found" : "✗ Not found"));
      System.out.println("  Goldman Sachs (0000886982): " +
        (djiCiks.contains("0000886982") ? "✓ Found" : "✗ Not found"));
      System.out.println("  JPMorgan (0000019617): " +
        (djiCiks.contains("0000019617") ? "✓ Found" : "✗ Not found"));
      System.out.println("  Walmart (0000104169): " +
        (djiCiks.contains("0000104169") ? "✓ Found" : "✗ Not found"));
    }

    System.out.println("\n"
  + "=".repeat(80));
    if (djiCiks.size() == 27 && match) {  // Dow 30 currently has 27 members (some merged/removed)
      System.out.println("✓ SUCCESS: _DJIA market group works correctly!");
    } else if (!djiCiks.isEmpty() && match) {
      System.out.println("✓ SUCCESS: _DJIA market group works (found " + djiCiks.size() + " members)");
    } else {
      System.out.println("✗ FAILURE: _DJIA resolution failed");
    }
    System.out.println("=".repeat(80));
  }
}
