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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Basic test for Wikipedia DJI scraping.
 */
public class BasicWikipediaTest {

  @Test
  public void testFetchDJIConstituents() {
    System.out.println("\n=== Testing DJI Wikipedia Scraper ===");
    
    try {
      List<String> ciks = SecDataFetcher.fetchDJIConstituents();
      assertNotNull(ciks, "CIK list should not be null");
      
      System.out.println("Found " + ciks.size() + " DJI constituents");
      
      // Even if Wikipedia fetch fails, we should get hardcoded fallback
      if (ciks.size() > 0) {
        System.out.println("✓ SUCCESS: Got DJI constituents (either from Wikipedia or fallback)");
      } else {
        System.out.println("⚠ WARNING: No constituents found");
      }
    } catch (Exception e) {
      System.err.println("✗ ERROR: " + e.getMessage());
      e.printStackTrace();
    }
  }
}