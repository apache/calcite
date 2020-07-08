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
package org.apache.calcite.rel.rules.materialize;

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.MaterializedViewFilterScanRule;

/**
 * Collection of rules pertaining to materialized views.
 *
 * <p>Also may contain utilities for {@link MaterializedViewRule}.
 */
public abstract class MaterializedViewRules {
  private MaterializedViewRules() {}

  /** Rule that matches {@link Project} on {@link Aggregate}. */
  public static final MaterializedViewProjectAggregateRule PROJECT_AGGREGATE =
      new MaterializedViewProjectAggregateRule(RelFactories.LOGICAL_BUILDER,
          true, null);

  /** Rule that matches {@link Aggregate}. */
  public static final MaterializedViewOnlyAggregateRule AGGREGATE =
      new MaterializedViewOnlyAggregateRule(RelFactories.LOGICAL_BUILDER,
          true, null);

  /** Rule that matches {@link Filter}. */
  public static final MaterializedViewOnlyFilterRule FILTER =
      new MaterializedViewOnlyFilterRule(RelFactories.LOGICAL_BUILDER,
          true, null, true);

  /** Rule that matches {@link Join}. */
  public static final MaterializedViewOnlyJoinRule JOIN =
      new MaterializedViewOnlyJoinRule(RelFactories.LOGICAL_BUILDER,
          true, null, true);

  /** Rule that matches {@link Project} on {@link Filter}. */
  public static final MaterializedViewProjectFilterRule PROJECT_FILTER =
      new MaterializedViewProjectFilterRule(RelFactories.LOGICAL_BUILDER,
          true, null, true);

  /** Rule that matches {@link Project} on {@link Join}. */
  public static final MaterializedViewProjectJoinRule PROJECT_JOIN =
      new MaterializedViewProjectJoinRule(RelFactories.LOGICAL_BUILDER,
          true, null, true);

  /** Rule that converts a {@link Filter} on a {@link TableScan}
   * to a {@link Filter} on a Materialized View. */
  public static final MaterializedViewFilterScanRule FILTER_SCAN =
      new MaterializedViewFilterScanRule(RelFactories.LOGICAL_BUILDER);
}
