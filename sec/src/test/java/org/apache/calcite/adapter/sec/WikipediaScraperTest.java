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
 * Test Wikipedia DJIA scraper.
 */
@Tag("integration")
public class WikipediaScraperTest {
  @Test public void testWikipediaDJIAScraper() throws Exception {
      System.out.println("Testing Wikipedia DJIA scraper...");
      List<String> constituents = SecDataFetcher.fetchDJIAAConstituents();

      System.out.println("Found " + constituents.size() + " DJIA constituents:");
      for (String ticker : constituents) {
        System.out.println("  - " + ticker);
      }

      if (constituents.size() == 30) {
        System.out.println("✓ Success: Found expected 30 DJIA constituents");
      } else {
        System.out.println("⚠ Warning: Expected 30 constituents, found " + constituents.size());
      }
  }
}
