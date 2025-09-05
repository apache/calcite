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
import org.junit.jupiter.api.Tag;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test to check DJI ticker to CIK mapping.
 */
@Tag("unit")
public class DJIMappingTest {
  
  @Test
  public void testDJIConstituentsMapping() {
    System.out.println("DEBUG TEST: Starting DJI fetch");
    // Enable fallback mode for this test since Wikipedia parsing may fail
    System.setProperty("sec.fallback.enabled", "true");
    
    // This will fetch from Wikipedia and map to CIKs, with fallback if needed
    List<String> ciks = SecDataFetcher.fetchDJIConstituents();
    System.out.println("DEBUG TEST: Got " + ciks.size() + " CIKs from fetch");
    
    System.out.println("Found " + ciks.size() + " DJI companies mapped to CIKs");
    for (String cik : ciks) {
      System.out.println("CIK: " + cik);
    }
    
    // DJI should have exactly 30 companies
    assertEquals(30, ciks.size(), "DJI should have exactly 30 companies");
  }
}