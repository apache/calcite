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
package org.apache.calcite.rel.rules.dm;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.dm.DatabricksTableMergeModify;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexUtil;

import org.immutables.value.Value;

@Value.Enclosing
public class DatabricksFilterSubQueryMoveRule
    extends RelRule<DatabricksFilterSubQueryMoveRule.Config>
    implements TransformationRule {

  private RuleMatchExtension extension;

  /** Creates an DatabricksFilterSubQueryMoveRule. */
  protected DatabricksFilterSubQueryMoveRule(Config config) {
    super(config);
  }

  public void setExtension(RuleMatchExtension extension) {
    this.extension = extension;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    extension.execute(call);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableDatabricksFilterSubQueryMoveRule.Config.of()
            .withOperandSupplier(b0 ->
                b0.operand(DatabricksTableMergeModify.class)
                    .oneInput(b1 ->
                        b1.operand(LogicalProject.class)
                            .oneInput(b2 ->
                                b2.operand(LogicalFilter.class)
                                    .predicate(RexUtil.SubQueryFinder::containsSubQuery)
                                    .anyInputs())))
            .withDescription("DatabricksFilterSubQueryMoveRule:Filter");

    @Override default DatabricksFilterSubQueryMoveRule toRule() {
      return new DatabricksFilterSubQueryMoveRule(this);
    }
  }
}
