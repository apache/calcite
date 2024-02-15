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
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.TransformationRule;

import org.immutables.value.Value;

@Value.Enclosing
public class DatabricksTableModifyTransformationRule
    extends RelRule<DatabricksTableModifyTransformationRule.Config>
    implements TransformationRule {

  private RuleMatchExtension extension;

  /** Creates an DatabricksTableModifyTransformationRule. */
  protected DatabricksTableModifyTransformationRule(Config config) {
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
    Config DEFAULT = ImmutableDatabricksTableModifyTransformationRule.Config.of()
            .withOperandSupplier(b0 ->
                    b0.operand(TableModify.class)
                            .predicate(TableModify::isUpdate)
                            .oneInput(b1 ->
                                    b1.operand(LogicalProject.class).anyInputs()));


    @Override default DatabricksTableModifyTransformationRule toRule() {
      return new DatabricksTableModifyTransformationRule(this);
    }

    /** Defines an operand tree for the given 2 classes. */
  }
}
