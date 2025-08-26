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

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;

/**
 * Handles pagination pushdown for multi-cloud query optimization.
 * Converts LIMIT/OFFSET to provider-specific pagination parameters and manages result windowing.
 */
public class CloudOpsPaginationHandler {
  private static final Logger logger = LoggerFactory.getLogger(CloudOpsPaginationHandler.class);

  private final @Nullable RexNode offset;
  private final @Nullable RexNode fetch;
  private final boolean hasPagination;
  private final long offsetValue;
  private final long limitValue;

  // Default pagination limits for cloud APIs
  private static final int DEFAULT_MAX_RESULTS = 1000;
  private static final int AZURE_MAX_RESULTS = 1000;
  private static final int AWS_MAX_RESULTS = 100;
  private static final int GCP_MAX_RESULTS = 500;

  public CloudOpsPaginationHandler(@Nullable RexNode offset, @Nullable RexNode fetch) {
    this.offset = offset;
    this.fetch = fetch;

    // Extract numeric values from RexNode
    this.offsetValue = extractNumericValue(offset, 0L);
    this.limitValue = extractNumericValue(fetch, DEFAULT_MAX_RESULTS);
    this.hasPagination = (offset != null || fetch != null);

    if (logger.isDebugEnabled()) {
      if (hasPagination) {
        logger.debug("CloudOpsPaginationHandler: LIMIT {} OFFSET {} (pagination enabled)",
                    limitValue, offsetValue);
      } else {
        logger.debug("CloudOpsPaginationHandler: No pagination (unbounded query)");
      }
    }
  }

  /**
   * Check if pagination pushdown is applicable.
   */
  public boolean hasPagination() {
    return hasPagination;
  }

  /**
   * Get the offset value (OFFSET clause).
   */
  public long getOffset() {
    return offsetValue;
  }

  /**
   * Get the limit value (LIMIT/FETCH clause).
   */
  public long getLimit() {
    return limitValue;
  }

  /**
   * Get the calculated end position (offset + limit).
   */
  public long getEndPosition() {
    return offsetValue + limitValue;
  }

  /**
   * Build Azure KQL TOP/SKIP clause for pagination.
   * Azure Resource Graph supports full pagination via KQL.
   */
  public @Nullable String buildAzureKqlPaginationClause() {
    if (!hasPagination) {
      return null;
    }

    StringBuilder kqlPagination = new StringBuilder();

    // Add SKIP clause for offset
    if (offsetValue > 0) {
      kqlPagination.append("| skip ").append(offsetValue);
    }

    // Add TOP clause for limit
    if (limitValue < DEFAULT_MAX_RESULTS) {
      if (kqlPagination.length() > 0) {
        kqlPagination.append(" ");
      }
      kqlPagination.append("| top ").append(Math.min(limitValue, AZURE_MAX_RESULTS));
    }

    String result = kqlPagination.toString();

    if (logger.isDebugEnabled() && !result.isEmpty()) {
      double reductionPercent = ((double) limitValue / DEFAULT_MAX_RESULTS) * 100;
      logger.debug("Azure KQL pagination: {} -> {:.1f}% data transfer reduction",
                  result, 100.0 - reductionPercent);
    }

    return result.isEmpty() ? null : result;
  }

  /**
   * Get AWS MaxResults parameter for pagination.
   * AWS APIs use MaxResults for limit control.
   */
  public int getAWSMaxResults() {
    if (!hasPagination) {
      return AWS_MAX_RESULTS; // Default AWS page size
    }

    // AWS doesn't handle offset directly, so we need to fetch offset + limit
    long totalNeeded = offsetValue + limitValue;
    int maxResults = (int) Math.min(totalNeeded, AWS_MAX_RESULTS);

    if (logger.isDebugEnabled()) {
      logger.debug("AWS MaxResults parameter: {} (offset={}, limit={})",
                  maxResults, offsetValue, limitValue);
    }

    return maxResults;
  }

  /**
   * Check if AWS needs multiple pages to handle offset.
   * When offset > 0, we may need to fetch multiple pages and skip results.
   */
  public boolean needsAWSMultiPageFetch() {
    return hasPagination && offsetValue > 0;
  }

  /**
   * Get GCP pageSize parameter for pagination.
   * GCP APIs support pageSize for limit control.
   */
  public int getGCPPageSize() {
    if (!hasPagination) {
      return GCP_MAX_RESULTS; // Default GCP page size
    }

    // Similar to AWS, GCP doesn't handle offset directly
    long totalNeeded = offsetValue + limitValue;
    int pageSize = (int) Math.min(totalNeeded, GCP_MAX_RESULTS);

    if (logger.isDebugEnabled()) {
      logger.debug("GCP pageSize parameter: {} (offset={}, limit={})",
                  pageSize, offsetValue, limitValue);
    }

    return pageSize;
  }

  /**
   * Check if GCP needs multiple pages to handle offset.
   */
  public boolean needsGCPMultiPageFetch() {
    return hasPagination && offsetValue > 0;
  }

  /**
   * Apply client-side pagination to result list.
   * Used when server-side pagination is not sufficient (e.g., offset handling).
   */
  public <T> List<T> applyClientSidePagination(List<T> results) {
    if (!hasPagination || results.isEmpty()) {
      return results;
    }

    int startIndex = (int) Math.min(offsetValue, results.size());
    int endIndex = (int) Math.min(offsetValue + limitValue, results.size());

    if (startIndex >= results.size()) {
      return results.subList(0, 0); // Empty list
    }

    List<T> paginatedResults = results.subList(startIndex, endIndex);

    if (logger.isDebugEnabled()) {
      logger.debug("Client-side pagination: {} -> {} results (offset={}, limit={})",
                  results.size(), paginatedResults.size(), offsetValue, limitValue);
    }

    return paginatedResults;
  }

  /**
   * Calculate pagination optimization metrics.
   */
  public PaginationMetrics calculateMetrics(boolean serverSidePushdown, int totalResultsAvailable) {
    if (!hasPagination) {
      return new PaginationMetrics(totalResultsAvailable, totalResultsAvailable, 0.0,
                                   "No pagination", false);
    }

    long resultsFetched = serverSidePushdown ? limitValue : totalResultsAvailable;
    double reductionPercent = totalResultsAvailable > 0 ?
        (1.0 - (double) resultsFetched / totalResultsAvailable) * 100 : 0.0;

    String strategy = serverSidePushdown ?
        "Server-side pagination" : "Client-side pagination";

    return new PaginationMetrics(totalResultsAvailable, (int) resultsFetched,
                                reductionPercent, strategy, serverSidePushdown);
  }

  /**
   * Extract numeric value from RexNode (LIMIT/OFFSET values).
   */
  private long extractNumericValue(@Nullable RexNode node, long defaultValue) {
    if (node == null) {
      return defaultValue;
    }

    try {
      if (node instanceof RexLiteral) {
        RexLiteral literal = (RexLiteral) node;
        Object value = literal.getValue();

        if (value instanceof Number) {
          return ((Number) value).longValue();
        } else if (value instanceof BigDecimal) {
          return ((BigDecimal) value).longValue();
        }
      }
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Failed to extract numeric value from RexNode: {}", e.getMessage());
      }
    }

    return defaultValue;
  }

  /**
   * Pagination optimization strategy information.
   */
  public static class PaginationStrategy {
    public final boolean useServerSidePagination;
    public final boolean needsMultipleFetches;
    public final String providerStrategy;
    public final String optimizationNotes;

    public PaginationStrategy(boolean useServerSidePagination, boolean needsMultipleFetches,
                             String providerStrategy, String optimizationNotes) {
      this.useServerSidePagination = useServerSidePagination;
      this.needsMultipleFetches = needsMultipleFetches;
      this.providerStrategy = providerStrategy;
      this.optimizationNotes = optimizationNotes;
    }

    @Override public String toString() {
      return String.format("Strategy: %s (%s)%s",
                          providerStrategy,
                          useServerSidePagination ? "server-side" : "client-side",
                          needsMultipleFetches ? " + multi-fetch" : "");
    }
  }

  /**
   * Get pagination strategy for Azure.
   */
  public PaginationStrategy getAzureStrategy() {
    if (!hasPagination) {
      return new PaginationStrategy(false, false, "No pagination", "Unbounded query");
    }

    return new PaginationStrategy(true, false, "KQL TOP/SKIP",
                                 "Full server-side pagination support");
  }

  /**
   * Get pagination strategy for AWS.
   */
  public PaginationStrategy getAWSStrategy() {
    if (!hasPagination) {
      return new PaginationStrategy(false, false, "No pagination", "Default page size");
    }

    boolean needsMultiFetch = needsAWSMultiPageFetch();
    String notes = needsMultiFetch ?
        "Multiple API calls needed for offset handling" :
        "Single API call with MaxResults";

    return new PaginationStrategy(true, needsMultiFetch, "MaxResults + NextToken", notes);
  }

  /**
   * Get pagination strategy for GCP.
   */
  public PaginationStrategy getGCPStrategy() {
    if (!hasPagination) {
      return new PaginationStrategy(false, false, "No pagination", "Default page size");
    }

    boolean needsMultiFetch = needsGCPMultiPageFetch();
    String notes = needsMultiFetch ?
        "Multiple API calls needed for offset handling" :
        "Single API call with pageSize";

    return new PaginationStrategy(true, needsMultiFetch, "pageSize + pageToken", notes);
  }

  /**
   * Metrics about pagination optimization.
   */
  public static class PaginationMetrics {
    public final int totalResults;
    public final int resultsFetched;
    public final double reductionPercent;
    public final String strategy;
    public final boolean serverSidePushdown;

    public PaginationMetrics(int totalResults, int resultsFetched, double reductionPercent,
                           String strategy, boolean serverSidePushdown) {
      this.totalResults = totalResults;
      this.resultsFetched = resultsFetched;
      this.reductionPercent = reductionPercent;
      this.strategy = strategy;
      this.serverSidePushdown = serverSidePushdown;
    }

    @Override public String toString() {
      return String.format("Pagination: %d/%d results (%.1f%% reduction) via %s",
                          resultsFetched, totalResults, reductionPercent, strategy);
    }
  }
}
