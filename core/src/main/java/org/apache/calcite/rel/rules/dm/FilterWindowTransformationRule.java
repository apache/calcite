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
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.TransformationRule;

import org.immutables.value.Value;

@Value.Enclosing
public class FilterWindowTransformationRule
    extends RelRule<FilterWindowTransformationRule.Config>
    implements TransformationRule {

  private RuleMatchExtension extension;

  /** Creates an SerializeDistinctStructRule. */
  protected FilterWindowTransformationRule(Config config) {
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
    Config DEFAULT = ImmutableFilterWindowTransformationRule.Config.of()
            .withOperandSupplier(b -> b.operand(Filter.class)
                    .predicate(Filter::containsOver)
                    .oneInput(b1 -> b1.operand(AbstractRelNode.class)
                            .anyInputs()));
//
//    FilterWindowTransformationRule.Config DEFAULT = EMPTY
//            .withOperandSupplier(b -> b.operand(Filter.class)
//                    .predicate(Filter::containsOver)
//                    .oneInput(b1 -> b1.operand(AbstractRelNode.class)
//                            .anyInputs()))
//            .as(Config.class);

    @Override default FilterWindowTransformationRule toRule() {
      return new FilterWindowTransformationRule(this);
    }


    /** Defines an operand tree for the given classes. */
    default FilterWindowTransformationRule.Config withOperandFor(
            Class<? extends Aggregate> aggregateClass,
            Class<? extends Project> projectClass) {
      return withOperandSupplier(b0 ->
              b0.operand(aggregateClass).oneInput(b1 ->
                      b1.operand(projectClass).anyInputs()))
              .as(FilterWindowTransformationRule.Config.class);
    }
  }
}
