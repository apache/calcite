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
package org.apache.calcite.rel.rules.custom;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Nullify;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

import java.util.List;

/**
 * NullifyJoinReverseRule converts a nullified join (i.e., nullification operator
 * + outer cartesian product) backs to either a 1-sided outer join or inner join.
 */
public class NullifyJoinReverseRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the rule that reverses the nullification process. */
  public static final NullifyJoinReverseRule INSTANCE =
      new NullifyJoinReverseRule(operand(Nullify.class, operand(Join.class, any())), null);

  //~ Constructors -----------------------------------------------------------

  public NullifyJoinReverseRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public NullifyJoinReverseRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    // The nullification operator at the top.
    final Nullify nullify = call.rel(0);

    // The join operator at the bottom.
    final Join join = call.rel(1);
    if (join.getJoinType() != JoinRelType.OUTER_CARTESIAN) {
      LOGGER.debug("Nullification reverse should only be applied when the join is an outer cartesian product");
      return;
    }

    // Checks which join type should be converted back to.
    final List<RelDataTypeField> joinFieldList = join.getRowType().getFieldList();
  }
}

// End NullifyJoinReverseRule.java
