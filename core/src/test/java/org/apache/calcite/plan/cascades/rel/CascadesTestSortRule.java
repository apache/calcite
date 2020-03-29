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
package org.apache.calcite.plan.cascades.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.cascades.CascadesRuleCall;
import org.apache.calcite.plan.cascades.CascadesTestUtils;
import org.apache.calcite.plan.cascades.ImplementationRule;
import org.apache.calcite.plan.cascades.RelSubGroup;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalSort;

/**
 *
 */
public class CascadesTestSortRule extends ImplementationRule<LogicalSort> {
  public static final CascadesTestSortRule CASCADES_SORT_RULE =
      new CascadesTestSortRule();

  public CascadesTestSortRule() {
    super(LogicalSort.class,
        r -> true,
        Convention.NONE, CascadesTestUtils.CASCADES_TEST_CONVENTION, RelFactories.LOGICAL_BUILDER,
        "CascadesSortRule");
  }

  @Override public void implement(LogicalSort sort, RelTraitSet requestedTraits,
      CascadesRuleCall call) {
    // Sort preserves any other trait except sorting. We may request RelCollation.ANY.
    requestedTraits = requestedTraits.plus(RelCollationTraitDef.INSTANCE.getDefault());
    RelSubGroup input = (RelSubGroup) convert(sort.getInput(), requestedTraits);

    CascadesTestSort newSort = CascadesTestSort.create(
        input,
        sort.getCollation(),
        null,
        null);
    call.transformTo(newSort);
    call.transformTo(input);
  }
}
