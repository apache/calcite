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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.format.csv.CsvProjectTableScanRule;
import org.apache.calcite.adapter.file.rules.FileColumnPruningRule;
import org.apache.calcite.adapter.file.rules.FileFilterPushdownRule;
import org.apache.calcite.adapter.file.rules.FileJoinReorderRule;
import org.apache.calcite.adapter.file.rules.HLLCountDistinctRule;

/** Planner rules relating to the File adapter. */
public abstract class FileRules {
  private FileRules() {}

  /** Rule that matches a {@link org.apache.calcite.rel.core.Project} on
   * a {@link CsvTableScan} and pushes down projects if possible. */
  public static final CsvProjectTableScanRule PROJECT_SCAN =
      CsvProjectTableScanRule.Config.DEFAULT.toRule();

  /** Rule that replaces COUNT(DISTINCT) with HLL sketch lookups when available. */
  public static final HLLCountDistinctRule HLL_COUNT_DISTINCT =
      HLLCountDistinctRule.INSTANCE;

  /** Rule that pushes down filters using statistics for high selectivity conditions. */
  public static final FileFilterPushdownRule STATISTICS_FILTER_PUSHDOWN =
      FileFilterPushdownRule.INSTANCE;

  /** Rule that reorders joins based on table size statistics for optimal performance. */
  public static final FileJoinReorderRule STATISTICS_JOIN_REORDER =
      FileJoinReorderRule.INSTANCE;

  /** Rule that prunes unused columns using statistics to reduce I/O. */
  public static final FileColumnPruningRule STATISTICS_COLUMN_PRUNING =
      FileColumnPruningRule.INSTANCE;
}
