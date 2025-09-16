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

/**
 * Test Wikipedia DJIA scraper.
 */
@Tag("integration")
public class WikipediaScraperTest {
  @Test public void testWikipediaDJIAScraper() throws Exception {
      List<String> constituents = SecDataFetcher.fetchDJIAAConstituents();

      assertNotNull(constituents, "Constituents list should not be null");
      assertTrue(constituents.size() > 0, "Should find at least one constituent");
      
      // Verify each constituent is a valid ticker/CIK
      for (String constituent : constituents) {
        assertNotNull(constituent, "Constituent should not be null");
        assertFalse(constituent.trim().isEmpty(), "Constituent should not be empty");
      }
      
      // Should find approximately 30 DJIA constituents
      assertTrue(constituents.size() >= 25 && constituents.size() <= 35,
          "Expected 25-35 DJIA constituents, found " + constituents.size());
  }
}
