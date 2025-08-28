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
package org.apache.calcite.adapter.ops;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.ops.util.CloudOpsFilterHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsSortHandler;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Base class for Cloud Ops tables.
 * Handles common functionality like provider filtering and parallel execution.
 * Supports query optimization through projection, filtering, sorting, and pagination pushdown.
 */
public abstract class AbstractCloudOpsTable extends AbstractTable implements ProjectableFilterableTable {
  private static final Logger logger = LoggerFactory.getLogger(AbstractCloudOpsTable.class);

  protected final CloudOpsConfig config;

  protected AbstractCloudOpsTable(CloudOpsConfig config) {
    this.config = config;
  }

  @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters,
      int @Nullable [] projects) {
    // Log the received optimization hints
    logOptimizationHints(filters, projects, null, null, null);

    // Call the enhanced scan method with null for sort/pagination
    return scan(root, filters, projects, null, null, null);
  }

  /**
   * Enhanced scan method that accepts sort and pagination information.
   * This method can be called by custom table scan implementations.
   */
  public Enumerable<Object[]> scan(DataContext root,
                                   List<RexNode> filters,
                                   int @Nullable [] projects,
                                   @Nullable RelCollation collation,
                                   @Nullable RexNode offset,
                                   @Nullable RexNode fetch) {
    // Log all received optimization hints
    logOptimizationHints(filters, projects, collation, offset, fetch);

    // Create projection handler for optimization
    CloudOpsProjectionHandler projectionHandler =
        new CloudOpsProjectionHandler(getRowType(root.getTypeFactory()), projects);

    // Create sort handler for optimization
    CloudOpsSortHandler sortHandler =
        new CloudOpsSortHandler(getRowType(root.getTypeFactory()), collation);

    // Create pagination handler for optimization
    CloudOpsPaginationHandler paginationHandler =
        new CloudOpsPaginationHandler(offset, fetch);

    // Create filter handler for optimization
    CloudOpsFilterHandler filterHandler =
        new CloudOpsFilterHandler(getRowType(root.getTypeFactory()), filters);

    // Extract filters
    Set<String> providers = extractProviders(filterHandler);
    List<String> accounts = extractAccounts(filterHandler);

    // If no providers specified, use all configured providers
    if (providers.isEmpty()) {
      providers = new HashSet<>(config.providers);
    }

    // Query providers in parallel with projection support
    List<CompletableFuture<List<Object[]>>> futures = new ArrayList<>();

    if (providers.contains("azure") && config.azure != null) {
      futures.add(
          CompletableFuture.supplyAsync(() -> {
            List<Object[]> results =
                queryAzure(accounts.isEmpty() ? config.azure.subscriptionIds : accounts, projectionHandler, sortHandler, paginationHandler, filterHandler);
            return projectionHandler.projectRows(results);
          }));
    }

    if (providers.contains("gcp") && config.gcp != null) {
      futures.add(
          CompletableFuture.supplyAsync(() -> {
            List<Object[]> results =
                queryGCP(accounts.isEmpty() ? config.gcp.projectIds : accounts, projectionHandler, sortHandler, paginationHandler, filterHandler);
            return projectionHandler.projectRows(results);
          }));
    }

    if (providers.contains("aws") && config.aws != null) {
      futures.add(
          CompletableFuture.supplyAsync(() -> {
            List<Object[]> results =
                queryAWS(accounts.isEmpty() ? config.aws.accountIds : accounts, projectionHandler, sortHandler, paginationHandler, filterHandler);
            return projectionHandler.projectRows(results);
          }));
    }

    // Combine results
    List<Object[]> allResults = futures.stream()
        .map(CompletableFuture::join)
        .flatMap(List::stream)
        .collect(Collectors.toList());

    if (logger.isDebugEnabled()) {
      if (projectionHandler != null && !projectionHandler.isSelectAll()) {
        CloudOpsProjectionHandler.ProjectionMetrics metrics = projectionHandler.calculateMetrics();
        logger.debug("Combined projection results: {} rows with {}",
                     allResults.size(), metrics);
      }

      if (paginationHandler != null && paginationHandler.hasPagination()) {
        CloudOpsPaginationHandler.PaginationMetrics metrics =
            paginationHandler.calculateMetrics(true, allResults.size());
        logger.debug("Combined pagination results: {}", metrics);
      }

      if (filterHandler != null && filterHandler.hasPushableFilters()) {
        CloudOpsFilterHandler.FilterMetrics metrics =
            filterHandler.calculateMetrics(true, allResults.size());
        logger.debug("Combined filter results: {}", metrics);
      }
    }

    // Apply cross-provider client-side pagination if needed
    if (paginationHandler != null) {
      allResults = paginationHandler.applyClientSidePagination(allResults);
    }

    // Apply remaining filters in memory
    return applyFilters(Linq4j.asEnumerable(allResults), filters);
  }

  /**
   * Extract cloud provider filter from predicates.
   */
  protected Set<String> extractProviders(CloudOpsFilterHandler filterHandler) {
    if (filterHandler == null || !filterHandler.hasFilters()) {
      return new HashSet<>(); // Query all providers
    }

    Set<String> providers = filterHandler.extractProviderConstraints();
    if (logger.isDebugEnabled() && !providers.isEmpty()) {
      logger.debug("Provider constraints extracted from filters: {}", providers);
    }

    return providers;
  }

  /**
   * Extract account/subscription/project IDs from predicates.
   */
  protected List<String> extractAccounts(CloudOpsFilterHandler filterHandler) {
    if (filterHandler == null || !filterHandler.hasFilters()) {
      return new ArrayList<>(); // Use configured defaults
    }

    List<String> accounts = filterHandler.extractAccountConstraints();
    if (logger.isDebugEnabled() && !accounts.isEmpty()) {
      logger.debug("Account constraints extracted from filters: {}", accounts);
    }

    return accounts;
  }

  /**
   * Apply remaining filters that weren't pushed down.
   */
  protected Enumerable<Object[]> applyFilters(Enumerable<Object[]> rows, List<RexNode> filters) {
    if (filters == null || filters.isEmpty()) {
      return rows;
    }

    // For client-side filtering, we would use Calcite's built-in filter evaluation
    // For now, just return all rows (server-side filtering is the main optimization)
    if (logger.isDebugEnabled()) {
      logger.debug("Client-side filter application: {} remaining filter(s) to evaluate", filters.size());
    }

    return rows;
  }

  /**
   * Log optimization hints received from query planner.
   */
  protected void logOptimizationHints(List<RexNode> filters,
                                     int @Nullable [] projects,
                                     @Nullable RelCollation collation,
                                     @Nullable RexNode offset,
                                     @Nullable RexNode fetch) {
    if (logger.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder("Query optimization hints received for ")
          .append(this.getClass().getSimpleName()).append(":\n");

      // Log filters
      sb.append("  Filters: ");
      if (filters != null && !filters.isEmpty()) {
        sb.append(filters.size()).append(" filter(s)\n");
        for (int i = 0; i < filters.size(); i++) {
          sb.append("    [").append(i).append("] ").append(filters.get(i)).append("\n");
        }
      } else {
        sb.append("none\n");
      }

      // Log projections
      sb.append("  Projections: ");
      if (projects != null) {
        sb.append("columns ").append(Arrays.toString(projects)).append("\n");
      } else {
        sb.append("all columns (no projection pushdown)\n");
      }

      // Log sort collation
      sb.append("  Sort: ");
      if (collation != null && !collation.getFieldCollations().isEmpty()) {
        sb.append(collation.getFieldCollations().size()).append(" sort field(s)\n");
        collation.getFieldCollations().forEach(fc ->
          sb.append("    Field ").append(fc.getFieldIndex())
            .append(" ").append(fc.getDirection())
            .append(" ").append(fc.nullDirection).append("\n"));
      } else {
        sb.append("none (no sort pushdown)\n");
      }

      // Log pagination
      sb.append("  Pagination:\n");
      sb.append("    Offset: ").append(offset != null ? offset.toString() : "none").append("\n");
      sb.append("    Fetch/Limit: ").append(fetch != null ? fetch.toString() : "none").append("\n");

      logger.debug(sb.toString());
    }
  }

  /**
   * Query Azure resources with projection, sort, pagination, and filter support.
   */
  protected abstract List<Object[]> queryAzure(List<String> subscriptionIds,
                                               CloudOpsProjectionHandler projectionHandler,
                                               CloudOpsSortHandler sortHandler,
                                               CloudOpsPaginationHandler paginationHandler,
                                               CloudOpsFilterHandler filterHandler);

  /**
   * Query GCP resources with projection, sort, pagination, and filter support.
   */
  protected abstract List<Object[]> queryGCP(List<String> projectIds,
                                             CloudOpsProjectionHandler projectionHandler,
                                             CloudOpsSortHandler sortHandler,
                                             CloudOpsPaginationHandler paginationHandler,
                                             CloudOpsFilterHandler filterHandler);

  /**
   * Query AWS resources with projection, sort, pagination, and filter support.
   */
  protected abstract List<Object[]> queryAWS(List<String> accountIds,
                                             CloudOpsProjectionHandler projectionHandler,
                                             CloudOpsSortHandler sortHandler,
                                             CloudOpsPaginationHandler paginationHandler,
                                             CloudOpsFilterHandler filterHandler);
}
