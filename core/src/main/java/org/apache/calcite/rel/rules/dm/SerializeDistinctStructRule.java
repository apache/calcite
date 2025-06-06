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

import org.apache.calcite.plan.DistinctTrait;
import org.apache.calcite.plan.DistinctTraitDef;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.SubQueryAliasTrait;
import org.apache.calcite.plan.SubQueryAliasTraitDef;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.rules.TransformationRule;

import org.immutables.value.Value;

/**
 * Rule to convert struct used in min/max function as deserialize(min(serialize(struct))).
 *
 *
 * <p>Here serialize method will change the struct into a comparable string
 * while deserialize method will covert this string back into Struct.
 */
@Value.Enclosing
public class SerializeDistinctStructRule
    extends RelRule<SerializeDistinctStructRule.Config>
    implements TransformationRule {

  private RuleMatchExtension extension;

  /**
   * Creates an SerializeDistinctStructRule.
   */
  protected SerializeDistinctStructRule(Config config) {
    super(config);
  }

  public void setExtension(RuleMatchExtension extension) {
    this.extension = extension;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    extension.execute(call);
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSerializeDistinctStructRule.Config.of()
        .withOperandFor(Aggregate.class);

    @Override default SerializeDistinctStructRule toRule() {
      return new SerializeDistinctStructRule(this);
    }


    /**
     * Defines an operand tree for the given classes.
     */
    default Config withOperandFor(Class<? extends Aggregate> distinctProject) {
      return withOperandSupplier(b0 ->
          b0.operand(distinctProject)
              .predicate(r -> r != null && isDistinctStructExists(r))
              .anyInputs())
          .as(Config.class);
    }

    static boolean isDistinctStructExists(Aggregate e) {
      DistinctTrait distinctTrait = e.getTraitSet().getTrait(DistinctTraitDef.instance);
      SubQueryAliasTrait subQueryAliasTrait =
          e.getTraitSet().getTrait(SubQueryAliasTraitDef.instance);
      return subQueryAliasTrait == null && distinctTrait != null
          && distinctTrait.isDistinct() && !distinctTrait.isEvaluated();
    }
  }
}
