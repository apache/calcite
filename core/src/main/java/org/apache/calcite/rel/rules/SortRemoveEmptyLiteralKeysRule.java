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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

/**
 * Planner rule that removes keys from a
 * a {@link org.apache.calcite.rel.core.Sort} if those keys are empty litepal keys,
 * or removes the entire Sort if all keys are empty litepal keys.
 *
 * <p>Requires {@link RelCollationTraitDef}.
 */
@Value.Enclosing
public class SortRemoveEmptyLiteralKeysRule
    extends RelRule<SortRemoveEmptyLiteralKeysRule.Config>
    implements SubstitutionRule {

  /** Creates a SortRemoveEmptyLiteralKeysRule. */
  protected SortRemoveEmptyLiteralKeysRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Project project1 = call.rel(0);
    Project project2 = call.rel(1);

    final RelNode input1 = project1.getInput();
    final RelNode input2 = project2.getInput();
    final int size = input1.getRowType().getFieldList().size();

    if (input1.getRowType().getFieldList().get(size - 1).getValue().isNullable()) {
      Project newProject1 =
          (Project) project1.copy(project1.getTraitSet(), ImmutableList.of(input2));
      call.transformTo(newProject1);
      return;
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSortRemoveEmptyLiteralKeysRule.Config.of()
        .withOperandSupplier(b -> b.operand(Project.class)
        .oneInput(b1 -> b1.operand(Project.class)
        .anyInputs()));

    @Override default SortRemoveEmptyLiteralKeysRule toRule() {
      return new SortRemoveEmptyLiteralKeysRule(this);
    }
  }
}
