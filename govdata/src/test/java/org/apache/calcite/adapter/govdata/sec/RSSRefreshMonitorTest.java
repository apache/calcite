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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RSS refresh monitor functionality.
 */
@Tag("unit")
public class RSSRefreshMonitorTest {

  @Test
  void testRSSMonitorDisabledByDefault() {
    Map<String, Object> operand = new HashMap<>();
    
    RSSRefreshMonitor monitor = new RSSRefreshMonitor(operand);
    
    // Should not start when not configured
    assertDoesNotThrow(() -> monitor.start());
    assertDoesNotThrow(() -> monitor.shutdown());
  }

  @Test
  void testRSSMonitorConfiguration() {
    Map<String, Object> operand = new HashMap<>();
    Map<String, Object> refreshConfig = new HashMap<>();
    refreshConfig.put("enabled", true);
    refreshConfig.put("checkIntervalMinutes", 5);
    refreshConfig.put("debounceMinutes", 10);
    refreshConfig.put("maxDebounceMinutes", 30);
    operand.put("refreshMonitoring", refreshConfig);
    
    RSSRefreshMonitor monitor = new RSSRefreshMonitor(operand);
    
    // Should start when properly configured
    assertDoesNotThrow(() -> monitor.start());
    assertDoesNotThrow(() -> monitor.shutdown());
  }

  @Test
  void testRSSMonitorWithCiks() {
    Map<String, Object> operand = new HashMap<>();
    Map<String, Object> refreshConfig = new HashMap<>();
    refreshConfig.put("enabled", true);
    operand.put("refreshMonitoring", refreshConfig);
    operand.put("ciks", new String[]{"AAPL", "MSFT"});
    
    RSSRefreshMonitor monitor = new RSSRefreshMonitor(operand);
    
    assertDoesNotThrow(() -> monitor.start());
    assertDoesNotThrow(() -> monitor.shutdown());
  }
}