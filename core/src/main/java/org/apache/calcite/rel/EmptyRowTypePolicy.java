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
package org.apache.calcite.rel;

/**
 * Policy that specifies whether a {@link RelNode} is allowed to have an
 * empty row type, that is, a row type that contains zero fields.
 *
 * <p>For example, {@code SELECT * FROM t GROUP BY ()} with no aggregate
 * functions returns one row with zero columns, and can be represented as
 * a {@link org.apache.calcite.rel.core.Values} whose row type is empty.
 *
 * <p>The policy is configured via
 * {@link org.apache.calcite.tools.RelBuilder.Config#emptyRowTypePolicy()} and
 * via the planner's
 * {@link org.apache.calcite.plan.Context}; both default to
 * {@link #DISCOURAGED}. For a few releases, tests should be run in all three
 * modes, and all rules must run in all modes.
 *
 * @see <a href="https://issues.apache.org/jira/browse/CALCITE-4597">
 * CALCITE-4597: Allow RelNodes to have an empty row type (zero fields)</a>
 */
public enum EmptyRowTypePolicy {
  /**
   * Empty row types are forbidden: Calcite prevents creation of
   * {@link RelNode}s whose row type is empty. For example, the planner and
   * {@code RelBuilder} throw if they see one.
   *
   * <p>Rules must not produce empty row types, and can assume that they
   * will not encounter empty row types.
   */
  FORBIDDEN,

  /**
   * Empty row types are discouraged: the planner and {@code RelBuilder} will
   * not throw if they see a {@link RelNode} whose row type is empty, but
   * they try not to create one.
   *
   * <p>Rules must not fail if they encounter an empty row type, and should
   * not produce empty row types (with reasonable exceptions, such as if the
   * input has an empty row type).
   */
  DISCOURAGED,

  /**
   * Empty row types are allowed: it is acceptable for a {@link RelNode} to
   * have an empty row type.
   *
   * <p>All rules should handle {@link RelNode}s with empty row types, and
   * it's OK if they generate {@link RelNode}s with empty row types.
   */
  ALLOWED
}
