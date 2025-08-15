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
package org.apache.calcite.adapter.file.statistics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for statistics configuration system, ensuring HLL can be properly
 * enabled/disabled and configured through various mechanisms.
 */
@Tag("unit")
@TestMethodOrder(MethodOrderer.MethodName.class)
public class StatisticsConfigurationTest {

  private String originalHllEnabled;
  private String originalHllPrecision;
  private String originalHllThreshold;

  @BeforeEach
  void saveSystemProperties() {
    // Save original system properties to restore later
    originalHllEnabled = System.getProperty("calcite.file.statistics.hll.enabled");
    originalHllPrecision = System.getProperty("calcite.file.statistics.hll.precision");
    originalHllThreshold = System.getProperty("calcite.file.statistics.hll.threshold");
    
    // Clear all properties to ensure clean state for each test
    System.clearProperty("calcite.file.statistics.hll.enabled");
    System.clearProperty("calcite.file.statistics.hll.precision");
    System.clearProperty("calcite.file.statistics.hll.threshold");
    System.clearProperty("calcite.file.statistics.cache.maxAge");
    System.clearProperty("calcite.file.statistics.backgroundGeneration");
  }

  @AfterEach
  void restoreSystemProperties() {
    // Clear all statistics-related system properties to avoid test interference
    System.clearProperty("calcite.file.statistics.hll.enabled");
    System.clearProperty("calcite.file.statistics.hll.precision");
    System.clearProperty("calcite.file.statistics.hll.threshold");
    System.clearProperty("calcite.file.statistics.cache.maxAge");
    System.clearProperty("calcite.file.statistics.backgroundGeneration");
    
    // Restore original system properties if they existed
    if (originalHllEnabled != null) {
      System.setProperty("calcite.file.statistics.hll.enabled", originalHllEnabled);
    }
    
    if (originalHllPrecision != null) {
      System.setProperty("calcite.file.statistics.hll.precision", originalHllPrecision);
    }
    
    if (originalHllThreshold != null) {
      System.setProperty("calcite.file.statistics.hll.threshold", originalHllThreshold);
    }
  }

  @Test
  @DisplayName("Default configuration should have HLL enabled by default")
  void testDefaultConfiguration() {
    StatisticsConfig config = StatisticsConfig.DEFAULT;
    
    assertTrue(config.isHllEnabled(), "HLL should be enabled by default");
    assertEquals(14, config.getHllPrecision(), "Default HLL precision should be 14 for better accuracy");
    assertEquals(1000, config.getHllThreshold(), "Default HLL threshold should be 1000");
    assertTrue(config.isBackgroundGeneration(), "Background generation should be enabled by default");
    assertEquals(7 * 24 * 60 * 60 * 1000L, config.getMaxCacheAge(), "Default cache age should be 7 days");
  }

  @Test
  @DisplayName("NO_HLL configuration should disable HLL")
  void testNoHllConfiguration() {
    StatisticsConfig config = StatisticsConfig.NO_HLL;
    
    assertFalse(config.isHllEnabled(), "NO_HLL config should disable HLL");
    // Other settings should still be reasonable
    assertEquals(14, config.getHllPrecision(), "HLL precision should still be set to default 14-bit");
    assertEquals(1000, config.getHllThreshold(), "HLL threshold should still be set");
  }

  @Test
  @DisplayName("System properties should override default configuration")
  void testSystemPropertyOverrides() {
    // Set system properties
    System.setProperty("calcite.file.statistics.hll.enabled", "false");
    System.setProperty("calcite.file.statistics.hll.precision", "8");
    System.setProperty("calcite.file.statistics.hll.threshold", "5000");
    System.setProperty("calcite.file.statistics.backgroundGeneration", "false");
    
    StatisticsConfig config = StatisticsConfig.fromSystemProperties();
    
    assertFalse(config.isHllEnabled(), "System property should disable HLL");
    assertEquals(8, config.getHllPrecision(), "System property should set precision to 8");
    assertEquals(5000, config.getHllThreshold(), "System property should set threshold to 5000");
    assertFalse(config.isBackgroundGeneration(), "System property should disable background generation");
  }

  @Test
  @DisplayName("Builder should validate HLL precision boundaries")
  void testBuilderValidation() {
    StatisticsConfig.Builder builder = new StatisticsConfig.Builder();
    
    // Valid precision values should work
    assertDoesNotThrow(() -> builder.hllPrecision(4).build(), "Precision 4 should be valid");
    assertDoesNotThrow(() -> builder.hllPrecision(16).build(), "Precision 16 should be valid");
    
    // Invalid precision values should throw
    assertThrows(IllegalArgumentException.class, 
        () -> builder.hllPrecision(3).build(), "Precision 3 should be invalid");
    assertThrows(IllegalArgumentException.class, 
        () -> builder.hllPrecision(17).build(), "Precision 17 should be invalid");
    
    // Negative threshold should throw
    assertThrows(IllegalArgumentException.class, 
        () -> builder.hllThreshold(-1).build(), "Negative threshold should be invalid");
    
    // Negative cache age should throw
    assertThrows(IllegalArgumentException.class, 
        () -> builder.maxCacheAge(-1).build(), "Negative cache age should be invalid");
  }

  @Test
  @DisplayName("Statistics builder should respect HLL configuration")
  void testStatisticsBuilderConfiguration() {
    // Test with HLL enabled
    StatisticsConfig hllEnabled = new StatisticsConfig.Builder()
        .hllEnabled(true)
        .hllPrecision(10)
        .hllThreshold(500)
        .build();
    
    StatisticsBuilder builderEnabled = new StatisticsBuilder(hllEnabled);
    assertNotNull(builderEnabled, "Statistics builder should be created with HLL enabled");
    
    // Test with HLL disabled
    StatisticsConfig hllDisabled = new StatisticsConfig.Builder()
        .hllEnabled(false)
        .build();
    
    StatisticsBuilder builderDisabled = new StatisticsBuilder(hllDisabled);
    assertNotNull(builderDisabled, "Statistics builder should be created with HLL disabled");
  }

  @Test
  @DisplayName("Effective configuration should prioritize system properties over environment")
  void testEffectiveConfigurationPrecedence() {
    // This test simulates the precedence: system properties > environment > defaults
    // Since we can't easily set environment variables in unit tests, we'll test system properties
    
    System.setProperty("calcite.file.statistics.hll.enabled", "false");
    System.setProperty("calcite.file.statistics.hll.precision", "14");
    
    StatisticsConfig config = StatisticsConfig.fromSystemProperties();
    
    assertFalse(config.isHllEnabled(), "System property should override default HLL setting");
    assertEquals(14, config.getHllPrecision(), "System property should override default precision");
    
    // Properties not set should use defaults
    assertEquals(1000, config.getHllThreshold(), "Unset properties should use defaults");
    assertTrue(config.isBackgroundGeneration(), "Unset properties should use defaults");
  }

  @Test
  @DisplayName("Configuration toString should provide useful information")
  void testConfigurationToString() {
    StatisticsConfig config = new StatisticsConfig.Builder()
        .hllEnabled(true)
        .hllPrecision(12)
        .hllThreshold(1000)
        .maxCacheAge(86400000L) // 1 day
        .backgroundGeneration(true)
        .build();
    
    String configString = config.toString();
    
    assertTrue(configString.contains("hllEnabled=true"), "toString should show HLL enabled");
    assertTrue(configString.contains("hllPrecision=12"), "toString should show HLL precision");
    assertTrue(configString.contains("hllThreshold=1000"), "toString should show HLL threshold");
    assertTrue(configString.contains("maxCacheAge=86400000"), "toString should show cache age");
    assertTrue(configString.contains("backgroundGeneration=true"), "toString should show background generation");
  }

  @Test
  @DisplayName("Different configurations should produce different builders")
  void testConfigurationImpactOnBuilder() {
    // Create two different configurations
    StatisticsConfig highPrecisionConfig = new StatisticsConfig.Builder()
        .hllEnabled(true)
        .hllPrecision(14)
        .hllThreshold(100)
        .build();
    
    StatisticsConfig lowPrecisionConfig = new StatisticsConfig.Builder()
        .hllEnabled(true)
        .hllPrecision(8)
        .hllThreshold(10000)
        .build();
    
    StatisticsConfig disabledConfig = new StatisticsConfig.Builder()
        .hllEnabled(false)
        .build();
    
    // All should create valid builders
    assertDoesNotThrow(() -> new StatisticsBuilder(highPrecisionConfig), 
        "High precision config should create valid builder");
    assertDoesNotThrow(() -> new StatisticsBuilder(lowPrecisionConfig), 
        "Low precision config should create valid builder");
    assertDoesNotThrow(() -> new StatisticsBuilder(disabledConfig), 
        "Disabled config should create valid builder");
  }

  @Test
  @DisplayName("Configuration should handle edge case values properly")
  void testConfigurationEdgeCases() {
    StatisticsConfig.Builder builder = new StatisticsConfig.Builder();
    
    // Test minimum valid values
    StatisticsConfig minConfig = builder
        .hllPrecision(4)
        .hllThreshold(0)
        .maxCacheAge(0)
        .build();
    
    assertEquals(4, minConfig.getHllPrecision(), "Minimum precision should be accepted");
    assertEquals(0, minConfig.getHllThreshold(), "Zero threshold should be accepted");
    assertEquals(0, minConfig.getMaxCacheAge(), "Zero cache age should be accepted");
    
    // Test maximum valid values
    StatisticsConfig maxConfig = new StatisticsConfig.Builder()
        .hllPrecision(16)
        .hllThreshold(Long.MAX_VALUE)
        .maxCacheAge(Long.MAX_VALUE)
        .build();
    
    assertEquals(16, maxConfig.getHllPrecision(), "Maximum precision should be accepted");
    assertEquals(Long.MAX_VALUE, maxConfig.getHllThreshold(), "Maximum threshold should be accepted");
    assertEquals(Long.MAX_VALUE, maxConfig.getMaxCacheAge(), "Maximum cache age should be accepted");
  }

  @Test
  @DisplayName("Default builder should match default configuration")
  void testDefaultBuilderMatchesDefaultConfig() {
    StatisticsConfig builderDefault = new StatisticsConfig.Builder().build();
    StatisticsConfig staticDefault = StatisticsConfig.DEFAULT;
    
    assertEquals(staticDefault.isHllEnabled(), builderDefault.isHllEnabled(), 
        "Builder default should match static default for HLL enabled");
    assertEquals(staticDefault.getHllPrecision(), builderDefault.getHllPrecision(), 
        "Builder default should match static default for HLL precision");
    assertEquals(staticDefault.getHllThreshold(), builderDefault.getHllThreshold(), 
        "Builder default should match static default for HLL threshold");
    assertEquals(staticDefault.getMaxCacheAge(), builderDefault.getMaxCacheAge(), 
        "Builder default should match static default for max cache age");
    assertEquals(staticDefault.isBackgroundGeneration(), builderDefault.isBackgroundGeneration(), 
        "Builder default should match static default for background generation");
  }
}