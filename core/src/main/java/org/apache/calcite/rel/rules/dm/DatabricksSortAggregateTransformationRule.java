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
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.TransformationRule;

import org.immutables.value.Value;

import java.util.List;

/**
 * Transforms RelNode from
 * "Aggregate -> Project" to "Project -> Aggregate -> Sort"
 * if RelCollation present in Aggregate relNode.
 */
@Value.Enclosing
public class DatabricksSortAggregateTransformationRule
    extends RelRule<DatabricksSortAggregateTransformationRule.Config>
    implements TransformationRule {

  private RuleMatchExtension extension;

  /** Creates an DatabricksSortAggregateTransformationRule. */
  protected DatabricksSortAggregateTransformationRule(
      DatabricksSortAggregateTransformationRule.Config config) {
    super(config);
  }

  public void setExtension(RuleMatchExtension extension) {
    this.extension = extension;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    extension.execute(call);
  }

  private static boolean hasRelCollation(Aggregate aggregate) {
    List<AggregateCall> aggregateCallList = aggregate.getAggCallList();
    return aggregateCallList.stream().anyMatch(aggregateCall -> aggregateCall.collation != null);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {

    DatabricksSortAggregateTransformationRule.Config DEFAULT =
        ImmutableDatabricksSortAggregateTransformationRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Aggregate.class)
                .predicate(DatabricksSortAggregateTransformationRule::hasRelCollation)
                .oneInput(b1 ->
                    b1.operand(Project.class).anyInputs()))
        .as(DatabricksSortAggregateTransformationRule.Config.class);

    @Override default DatabricksSortAggregateTransformationRule toRule() {
      return new DatabricksSortAggregateTransformationRule(this);
    }
  }
}
