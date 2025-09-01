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

/**
 * Configuration for statistics collection.
 * HLL is disabled by default as it provides useless approximations for SQL queries.
 */
public class StatisticsConfig {

  private final boolean hllEnabled;
  private final int hllPrecision;
  private final long hllThreshold;
  private final long maxCacheAge;
  private final boolean backgroundGeneration;
  private final boolean autoGenerateStatistics;

  /**
   * Default configuration with HLL enabled for COUNT(DISTINCT) optimization.
   * HLL provides approximate counts with controllable precision.
   */
  public static final StatisticsConfig DEFAULT = new Builder().hllEnabled(true).build();

  /**
   * Configuration with HLL disabled (for testing or resource-constrained environments).
   */
  public static final StatisticsConfig NO_HLL = new Builder().hllEnabled(false).build();

  private StatisticsConfig(boolean hllEnabled, int hllPrecision, long hllThreshold,
      long maxCacheAge, boolean backgroundGeneration, boolean autoGenerateStatistics) {
    this.hllEnabled = hllEnabled;
    this.hllPrecision = hllPrecision;
    this.hllThreshold = hllThreshold;
    this.maxCacheAge = maxCacheAge;
    this.backgroundGeneration = backgroundGeneration;
    this.autoGenerateStatistics = autoGenerateStatistics;
  }

  /**
   * Whether HyperLogLog sketches should be generated.
   */
  public boolean isHllEnabled() {
    return hllEnabled;
  }

  /**
   * HLL precision (4-16, higher = more accurate but more memory).
   */
  public int getHllPrecision() {
    return hllPrecision;
  }

  /**
   * Minimum estimated distinct count to generate HLL.
   */
  public long getHllThreshold() {
    return hllThreshold;
  }

  /**
   * Maximum age of cached statistics in milliseconds.
   */
  public long getMaxCacheAge() {
    return maxCacheAge;
  }

  /**
   * Whether statistics generation should happen in background threads.
   */
  public boolean isBackgroundGeneration() {
    return backgroundGeneration;
  }

  /**
   * Whether to automatically generate statistics when tables are accessed.
   * When true, statistics (including HLL sketches) are generated on first access.
   * When false, statistics must be explicitly generated.
   * Default: true for better out-of-box experience.
   */
  public boolean isAutoGenerateStatistics() {
    return autoGenerateStatistics;
  }

  /**
   * Create configuration from system properties.
   * Allows runtime configuration without code changes.
   */
  public static StatisticsConfig fromSystemProperties() {
    Builder builder = new Builder();

    // Check system properties for configuration
    String hllEnabled = System.getProperty("calcite.file.statistics.hll.enabled", "false");
    builder.hllEnabled(Boolean.parseBoolean(hllEnabled));

    String hllPrecision = System.getProperty("calcite.file.statistics.hll.precision", "14");
    builder.hllPrecision(Integer.parseInt(hllPrecision));

    String hllThreshold = System.getProperty("calcite.file.statistics.hll.threshold", "1000");
    builder.hllThreshold(Long.parseLong(hllThreshold));

    String maxCacheAge =
        System.getProperty("calcite.file.statistics.cache.maxAge", "604800000"); // 7 days
    builder.maxCacheAge(Long.parseLong(maxCacheAge));

    String backgroundGeneration =
        System.getProperty("calcite.file.statistics.backgroundGeneration", "true");
    builder.backgroundGeneration(Boolean.parseBoolean(backgroundGeneration));

    String autoGenerate = System.getProperty("calcite.file.statistics.auto.generate", "true");
    builder.autoGenerateStatistics(Boolean.parseBoolean(autoGenerate));

    return builder.build();
  }

  /**
   * Create configuration from environment variables.
   */
  public static StatisticsConfig fromEnvironmentVariables() {
    Builder builder = new Builder();

    String hllEnabled = System.getenv("CALCITE_STATISTICS_HLL_ENABLED");
    if (hllEnabled != null) {
      builder.hllEnabled(Boolean.parseBoolean(hllEnabled));
    }

    String hllPrecision = System.getenv("CALCITE_STATISTICS_HLL_PRECISION");
    if (hllPrecision != null) {
      builder.hllPrecision(Integer.parseInt(hllPrecision));
    }

    String hllThreshold = System.getenv("CALCITE_STATISTICS_HLL_THRESHOLD");
    if (hllThreshold != null) {
      builder.hllThreshold(Long.parseLong(hllThreshold));
    }

    String maxCacheAge = System.getenv("CALCITE_STATISTICS_CACHE_MAX_AGE");
    if (maxCacheAge != null) {
      builder.maxCacheAge(Long.parseLong(maxCacheAge));
    }

    String backgroundGeneration = System.getenv("CALCITE_STATISTICS_BACKGROUND_GENERATION");
    if (backgroundGeneration != null) {
      builder.backgroundGeneration(Boolean.parseBoolean(backgroundGeneration));
    }

    String autoGenerate = System.getenv("CALCITE_STATISTICS_AUTO_GENERATE");
    if (autoGenerate != null) {
      builder.autoGenerateStatistics(Boolean.parseBoolean(autoGenerate));
    }

    return builder.build();
  }

  /**
   * Get the effective configuration, checking system properties first,
   * then environment variables, falling back to defaults.
   */
  public static StatisticsConfig getEffectiveConfig() {
    // Start with defaults
    Builder builder = new Builder();

    // Override with environment variables if present
    StatisticsConfig envConfig = fromEnvironmentVariables();
    if (System.getenv("CALCITE_STATISTICS_HLL_ENABLED") != null) {
      builder.hllEnabled(envConfig.isHllEnabled());
    }
    if (System.getenv("CALCITE_STATISTICS_HLL_PRECISION") != null) {
      builder.hllPrecision(envConfig.getHllPrecision());
    }
    if (System.getenv("CALCITE_STATISTICS_HLL_THRESHOLD") != null) {
      builder.hllThreshold(envConfig.getHllThreshold());
    }
    if (System.getenv("CALCITE_STATISTICS_CACHE_MAX_AGE") != null) {
      builder.maxCacheAge(envConfig.getMaxCacheAge());
    }
    if (System.getenv("CALCITE_STATISTICS_BACKGROUND_GENERATION") != null) {
      builder.backgroundGeneration(envConfig.isBackgroundGeneration());
    }
    if (System.getenv("CALCITE_STATISTICS_AUTO_GENERATE") != null) {
      builder.autoGenerateStatistics(envConfig.isAutoGenerateStatistics());
    }

    // System properties override environment variables
    StatisticsConfig sysConfig = fromSystemProperties();
    if (System.getProperty("calcite.file.statistics.hll.enabled") != null) {
      builder.hllEnabled(sysConfig.isHllEnabled());
    }
    if (System.getProperty("calcite.file.statistics.hll.precision") != null) {
      builder.hllPrecision(sysConfig.getHllPrecision());
    }
    if (System.getProperty("calcite.file.statistics.hll.threshold") != null) {
      builder.hllThreshold(sysConfig.getHllThreshold());
    }
    if (System.getProperty("calcite.file.statistics.cache.maxAge") != null) {
      builder.maxCacheAge(sysConfig.getMaxCacheAge());
    }
    if (System.getProperty("calcite.file.statistics.backgroundGeneration") != null) {
      builder.backgroundGeneration(sysConfig.isBackgroundGeneration());
    }
    if (System.getProperty("calcite.file.statistics.auto.generate") != null) {
      builder.autoGenerateStatistics(sysConfig.isAutoGenerateStatistics());
    }

    return builder.build();
  }

  /**
   * Builder for StatisticsConfig.
   */
  public static class Builder {
    private boolean hllEnabled = true; // HLL enabled by default for optimization
    private int hllPrecision = 14; // 14-bit precision: ~0.8% error vs 12-bit ~1.6% error
    private long hllThreshold = 1000; // Generate HLL for columns with >1K distinct values
    private long maxCacheAge = 7 * 24 * 60 * 60 * 1000L; // 7 days in milliseconds
    private boolean backgroundGeneration = true; // Generate statistics in background by default
    private boolean autoGenerateStatistics = true; // Auto-generate on first access

    public Builder hllEnabled(boolean hllEnabled) {
      this.hllEnabled = hllEnabled;
      return this;
    }

    public Builder hllPrecision(int hllPrecision) {
      if (hllPrecision < 4 || hllPrecision > 16) {
        throw new IllegalArgumentException("HLL precision must be between 4 and 16");
      }
      this.hllPrecision = hllPrecision;
      return this;
    }

    public Builder hllThreshold(long hllThreshold) {
      if (hllThreshold < 0) {
        throw new IllegalArgumentException("HLL threshold must be non-negative");
      }
      this.hllThreshold = hllThreshold;
      return this;
    }

    public Builder maxCacheAge(long maxCacheAge) {
      if (maxCacheAge < 0) {
        throw new IllegalArgumentException("Max cache age must be non-negative");
      }
      this.maxCacheAge = maxCacheAge;
      return this;
    }

    public Builder backgroundGeneration(boolean backgroundGeneration) {
      this.backgroundGeneration = backgroundGeneration;
      return this;
    }

    public Builder autoGenerateStatistics(boolean autoGenerateStatistics) {
      this.autoGenerateStatistics = autoGenerateStatistics;
      return this;
    }

    public StatisticsConfig build() {
      return new StatisticsConfig(hllEnabled, hllPrecision, hllThreshold, maxCacheAge,
                                   backgroundGeneration, autoGenerateStatistics);
    }
  }

  @Override public String toString() {
    return String.format("StatisticsConfig{hllEnabled=%s, hllPrecision=%d, hllThreshold=%d, "
        + "maxCacheAge=%d, backgroundGeneration=%s, autoGenerateStatistics=%s}",
        hllEnabled, hllPrecision, hllThreshold, maxCacheAge, backgroundGeneration,
        autoGenerateStatistics);
  }
}
