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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexProgram;

import com.google.common.collect.ImmutableList;

/** Variant of {@link org.apache.calcite.rel.rules.ProjectToCalcRule} for
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableProjectToCalcRule extends RelOptRule {
  EnumerableProjectToCalcRule() {
    super(operand(EnumerableProject.class, any()));
  }

  public void onMatch(RelOptRuleCall call) {
    final EnumerableProject project = call.rel(0);
    final RelNode child = project.getInput();
    final RelDataType rowType = project.getRowType();
    final RexProgram program =
        RexProgram.create(child.getRowType(),
            project.getProjects(),
            null,
            project.getRowType(),
            project.getCluster().getRexBuilder());
    final EnumerableCalc calc =
        new EnumerableCalc(
            project.getCluster(),
            project.getTraitSet(),
            child,
            rowType,
            program,
            ImmutableList.<RelCollation>of());
    call.transformTo(calc);
  }
}

// End EnumerableProjectToCalcRule.java
