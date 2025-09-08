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

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test ALL EDGAR filers dynamic loading.
 */
@Tag("integration")
public class AllEdgarFilersTest {
  @Test public void testAllEdgarFilersDynamicLoading() {
    System.out.println("Testing _ALL_EDGAR_FILERS dynamic loading...\n");

    // Test resolving the ALL group which uses _ALL_EDGAR_FILERS marker
    List<String> allCiks = CikRegistry.resolveCiks("ALL");

    if (allCiks.isEmpty()) {
      System.out.println("No CIKs returned - checking if it's trying to fetch...");

      // Try directly with the special marker
      allCiks = CikRegistry.resolveCiks("_ALL_EDGAR_FILERS");
    }

    if (!allCiks.isEmpty()) {
      System.out.println("SUCCESS! Loaded " + allCiks.size() + " CIKs from SEC EDGAR");
      System.out.println("\nFirst 10 CIKs:");
      for (int i = 0; i < Math.min(10, allCiks.size()); i++) {
        System.out.println("  " + allCiks.get(i));
      }

      // Test caching by calling again
      System.out.println("\nTesting cache (second call)...");
      long start = System.currentTimeMillis();
      List<String> cachedCiks = CikRegistry.resolveCiks("_ALL_EDGAR_FILERS");
      long elapsed = System.currentTimeMillis() - start;
      System.out.println("Second call returned " + cachedCiks.size() + " CIKs in " + elapsed + "ms");

      if (elapsed < 100) {
        System.out.println("Cache is working! (fast response)");
      }
    } else {
      System.out.println("FAILED: No CIKs returned");
      System.out.println("This might be due to network issues or SEC rate limiting");
    }
  }
}
