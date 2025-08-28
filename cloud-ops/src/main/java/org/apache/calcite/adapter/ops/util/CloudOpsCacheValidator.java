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
package org.apache.calcite.adapter.ops.util;

import org.apache.calcite.adapter.ops.CloudOpsConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility for validating cache configuration and providing debug information.
 */
public class CloudOpsCacheValidator {
  private static final Logger logger = LoggerFactory.getLogger(CloudOpsCacheValidator.class);

  // Configuration limits and recommendations
  private static final int MIN_TTL_MINUTES = 1;
  private static final int MAX_TTL_MINUTES = 1440; // 24 hours
  private static final int RECOMMENDED_MIN_TTL = 2;
  private static final int RECOMMENDED_MAX_TTL = 60; // 1 hour

  /**
   * Validate cache configuration and return validation results.
   */
  public static CacheValidationResult validateCacheConfig(CloudOpsConfig config) {
    List<String> errors = new ArrayList<>();
    List<String> warnings = new ArrayList<>();
    List<String> recommendations = new ArrayList<>();

    if (config == null) {
      errors.add("CloudOpsConfig is null");
      return new CacheValidationResult(false, errors, warnings, recommendations);
    }

    // Validate cache enabled setting
    if (config.cacheEnabled) {
      logger.debug("Cache is ENABLED for Cloud Ops adapter");

      // Validate TTL configuration
      if (config.cacheTtlMinutes < MIN_TTL_MINUTES) {
        errors.add(
            String.format("Cache TTL (%d minutes) is below minimum (%d minutes)",
                                config.cacheTtlMinutes, MIN_TTL_MINUTES));
      } else if (config.cacheTtlMinutes > MAX_TTL_MINUTES) {
        warnings.add(
            String.format("Cache TTL (%d minutes) is very high (max recommended: %d minutes)",
                                  config.cacheTtlMinutes, MAX_TTL_MINUTES));
      } else if (config.cacheTtlMinutes < RECOMMENDED_MIN_TTL) {
        warnings.add(
            String.format("Cache TTL (%d minutes) is very low (recommended min: %d minutes)",
                                  config.cacheTtlMinutes, RECOMMENDED_MIN_TTL));
      } else if (config.cacheTtlMinutes > RECOMMENDED_MAX_TTL) {
        recommendations.add(
            String.format("Cache TTL (%d minutes) is high - consider shorter TTL for fresher data",
                                        config.cacheTtlMinutes));
      } else {
        logger.debug("Cache TTL configuration is optimal: {} minutes", config.cacheTtlMinutes);
      }

      // Debug mode validation
      if (config.cacheDebugMode) {
        logger.info("Cache DEBUG MODE is ENABLED - this may impact performance in production");
        recommendations.add("Consider disabling cache debug mode in production for better performance");
      } else {
        logger.debug("Cache debug mode is disabled");
      }

      // Provider-specific recommendations
      validateProviderCacheConfiguration(config, recommendations);

    } else {
      logger.info("Cache is DISABLED for Cloud Ops adapter - API calls will not be cached");
      recommendations.add("Consider enabling caching for better performance with repeated queries");
    }

    boolean isValid = errors.isEmpty();

    if (logger.isInfoEnabled()) {
      logger.info("Cache configuration validation: Valid={}, Errors={}, Warnings={}, Recommendations={}",
                 isValid, errors.size(), warnings.size(), recommendations.size());
    }

    return new CacheValidationResult(isValid, errors, warnings, recommendations);
  }

  private static void validateProviderCacheConfiguration(CloudOpsConfig config, List<String> recommendations) {
    int enabledProviders = 0;

    if (config.providers.contains("azure")) {
      enabledProviders++;
      if (config.azure == null) {
        recommendations.add("Azure provider enabled but no Azure configuration found");
      }
    }

    if (config.providers.contains("aws")) {
      enabledProviders++;
      if (config.aws == null) {
        recommendations.add("AWS provider enabled but no AWS configuration found");
      }
    }

    if (config.providers.contains("gcp")) {
      enabledProviders++;
      if (config.gcp == null) {
        recommendations.add("GCP provider enabled but no GCP configuration found");
      }
    }

    if (enabledProviders > 1) {
      logger.debug("Multi-cloud setup detected ({} providers) - caching will be beneficial for cross-provider queries",
                  enabledProviders);

      if (config.cacheTtlMinutes < 5) {
        recommendations.add("Multi-cloud setup benefits from longer cache TTL (consider 5+ minutes)");
      }
    } else if (enabledProviders == 1) {
      logger.debug("Single-cloud setup detected - caching will help with repeated queries");
    } else {
      recommendations.add("No cloud providers configured - caching may not be necessary");
    }
  }

  /**
   * Create and validate cache manager with the given configuration.
   */
  public static CloudOpsCacheManager createValidatedCacheManager(CloudOpsConfig config) {
    CacheValidationResult validation = validateCacheConfig(config);

    if (!validation.isValid()) {
      logger.warn("Cache configuration has errors: {}", validation.getErrors());
      // Log errors but don't fail - create cache manager with safe defaults
    }

    if (!validation.getWarnings().isEmpty()) {
      logger.warn("Cache configuration warnings: {}", validation.getWarnings());
    }

    if (!validation.getRecommendations().isEmpty() && logger.isDebugEnabled()) {
      logger.debug("Cache configuration recommendations: {}", validation.getRecommendations());
    }

    if (!config.cacheEnabled) {
      logger.info("Creating cache manager with caching DISABLED");
      return new CloudOpsCacheManager(0, config.cacheDebugMode); // 0 TTL effectively disables caching
    }

    int ttl = Math.max(MIN_TTL_MINUTES, Math.min(MAX_TTL_MINUTES, config.cacheTtlMinutes));
    CloudOpsCacheManager cacheManager = new CloudOpsCacheManager(ttl, config.cacheDebugMode);

    if (logger.isInfoEnabled()) {
      logger.info("Created cache manager: {}", cacheManager.getConfigSummary());
    }

    return cacheManager;
  }

  /**
   * Log comprehensive cache configuration information.
   */
  public static void logCacheConfiguration(CloudOpsConfig config, CloudOpsCacheManager cacheManager) {
    if (!logger.isDebugEnabled()) return;

    logger.debug("=== Cloud Ops Cache Configuration ===");
    logger.debug("Cache Enabled: {}", config.cacheEnabled);
    logger.debug("Cache TTL: {} minutes", config.cacheTtlMinutes);
    logger.debug("Cache Debug Mode: {}", config.cacheDebugMode);
    logger.debug("Cache Manager Config: {}", cacheManager.getConfigSummary());

    // Log provider configuration
    logger.debug("Enabled Providers: {}", config.providers);

    if (config.providers.contains("azure") && config.azure != null) {
      logger.debug("Azure: {} subscriptions",
                  config.azure.subscriptionIds != null ? config.azure.subscriptionIds.size() : 0);
    }

    if (config.providers.contains("aws") && config.aws != null) {
      logger.debug("AWS: {} accounts, region: {}",
                  config.aws.accountIds != null ? config.aws.accountIds.size() : 0,
                  config.aws.region);
    }

    if (config.providers.contains("gcp") && config.gcp != null) {
      logger.debug("GCP: {} projects",
                  config.gcp.projectIds != null ? config.gcp.projectIds.size() : 0);
    }

    logger.debug("=======================================");
  }

  /**
   * Cache validation result containing validation status and feedback.
   */
  public static class CacheValidationResult {
    private final boolean valid;
    private final List<String> errors;
    private final List<String> warnings;
    private final List<String> recommendations;

    public CacheValidationResult(boolean valid, List<String> errors,
                               List<String> warnings, List<String> recommendations) {
      this.valid = valid;
      this.errors = new ArrayList<>(errors);
      this.warnings = new ArrayList<>(warnings);
      this.recommendations = new ArrayList<>(recommendations);
    }

    public boolean isValid() {
      return valid;
    }

    public List<String> getErrors() {
      return new ArrayList<>(errors);
    }

    public List<String> getWarnings() {
      return new ArrayList<>(warnings);
    }

    public List<String> getRecommendations() {
      return new ArrayList<>(recommendations);
    }

    public boolean hasIssues() {
      return !errors.isEmpty() || !warnings.isEmpty();
    }

    @Override public String toString() {
      return String.format("CacheValidationResult[valid=%s, errors=%d, warnings=%d, recommendations=%d]",
                          valid, errors.size(), warnings.size(), recommendations.size());
    }
  }
}
