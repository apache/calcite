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
import org.apache.calcite.adapter.govdata.sec.SecDataFetcher;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class DJIAWikipediaTest {
  @Test
public void test() throws Exception {
    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("TESTING DJIA WIKIPEDIA SCRAPING");
    System.out.println("=".repeat(80) + "\n");

    // Clear cache to force fresh fetch
    java.io.File cacheDir = new java.io.File(System.getProperty("user.home"), ".calcite/sec-cache");
    java.io.File cacheFile = new java.io.File(cacheDir, "_DJIA_CONSTITUENTS.json");
    if (cacheFile.exists()) {
      cacheFile.delete();
      System.out.println("Cleared cache file: " + cacheFile);
    }

    System.out.println("Fetching DJIA constituents (should try Wikipedia first)...\n");

    // Enable detailed logging
    java.util.logging.Logger.getLogger("org.apache.calcite.adapter.sec").setLevel(java.util.logging.Level.FINE);

    List<String> djiCiks = SecDataFetcher.fetchDJIAAConstituents();

    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("RESULTS:");
    System.out.println("  Total CIKs fetched: " + djiCiks.size());

    if (djiCiks.size() > 0) {
      System.out.println("\nFirst 5 CIKs:");
      for (int i = 0; i < Math.min(5, djiCiks.size()); i++) {
        System.out.println("  " + (i+1) + ". " + djiCiks.get(i));
      }
    }

    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("Check the logs above to see if Wikipedia was used or hardcoded fallback");
    System.out.println("=".repeat(80));
  }
}
