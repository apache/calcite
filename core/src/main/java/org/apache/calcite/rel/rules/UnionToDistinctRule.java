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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.sql.SqlKind;

/**
 * Planner rule that translates a distinct
 * {@link org.apache.calcite.rel.core.Union}
 * (<code>all</code> = <code>false</code>)
 * into an {@link org.apache.calcite.rel.core.Aggregate}
 * on top of a non-distinct {@link org.apache.calcite.rel.core.Union}
 * (<code>all</code> = <code>true</code>).
 */
public class UnionToDistinctRule extends RelOptRule {
  public static final UnionToDistinctRule INSTANCE =
      new UnionToDistinctRule(LogicalUnion.class,
          RelFactories.DEFAULT_SET_OP_FACTORY);

  private final RelFactories.SetOpFactory setOpFactory;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a UnionToDistinctRule.
   */
  public UnionToDistinctRule(Class<? extends Union> clazz,
      RelFactories.SetOpFactory setOpFactory) {
    super(operand(clazz, any()));
    this.setOpFactory = setOpFactory;
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    Union union = call.rel(0);
    if (union.all) {
      return; // nothing to do
    }
    RelNode unionAll = setOpFactory.createSetOp(SqlKind.UNION,
        union.getInputs(), true);
    call.transformTo(RelOptUtil.createDistinctRel(unionAll));
  }
}

// End UnionToDistinctRule.java
