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
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

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
      new UnionToDistinctRule(LogicalUnion.class, RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a UnionToDistinctRule.
   */
  public UnionToDistinctRule(Class<? extends Union> unionClazz,
      RelBuilderFactory relBuilderFactory) {
    super(operandJ(unionClazz, null, u -> !u.all, any()), relBuilderFactory, null);
  }

  @Deprecated // to be removed before 2.0
  public UnionToDistinctRule(Class<? extends Union> unionClazz,
      RelFactories.SetOpFactory setOpFactory) {
    this(unionClazz, RelBuilder.proto(setOpFactory));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Union union = call.rel(0);
    final RelBuilder relBuilder = call.builder();
    relBuilder.pushAll(union.getInputs());
    relBuilder.union(true, union.getInputs().size());
    relBuilder.distinct();
    call.transformTo(relBuilder.build());
  }
}

// End UnionToDistinctRule.java
