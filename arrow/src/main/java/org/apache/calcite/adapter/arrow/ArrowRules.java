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

package org.apache.calcite.adapter.arrow;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.List;

/** Planner rules relating to the Arrow adapter. */
public class ArrowRules {
  private ArrowRules() {}

  /** Rule that matches a {@link org.apache.calcite.rel.core.Project} on
   * a {@link ArrowTableScan} and pushes down projects if possible. */
  public static final ArrowProjectTableScanRule PROJECT_SCAN =
      ArrowProjectTableScanRule.Config.DEFAULT.toRule();
  public static final ArrowFilterTableScanRule FILTER_SCAN =
      ArrowFilterTableScanRule.Config.DEFAULT.toRule();

  public static final RelOptRule[] RULES = {
      FILTER_SCAN,
      PROJECT_SCAN
  };

  static List<String> arrowFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(rowType.getFieldNames(),
        SqlValidatorUtil.EXPR_SUGGESTER, true);
  }
}
