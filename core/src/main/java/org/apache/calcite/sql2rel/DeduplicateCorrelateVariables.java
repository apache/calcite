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
package org.apache.calcite.sql2rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.List;
import java.util.Set;

/**
 * Rewrites relations to ensure the same correlation is referenced by the same
 * correlation variable.
 */
public class DeduplicateCorrelateVariables extends RelShuttleImpl {
  private final RexShuttle dedupRex;

  /**
   * Replaces alternative names of correlation variable to its canonical name.
   */
  private static class DeduplicateCorrelateVariablesShuttle extends RexShuttle {
    private final RexBuilder builder;
    private final String canonical;
    private final Set<String> altNames;

    public DeduplicateCorrelateVariablesShuttle(RexBuilder builder,
        String canonical, Set<String> altNames) {
      this.builder = builder;
      this.canonical = canonical;
      this.altNames = altNames;
    }

    @Override
    public RexNode visitCorrelVariable(RexCorrelVariable variable) {
      if (!altNames.contains(variable.getName())) {
        return variable;
      }

      return builder.makeCorrel(variable.getType(), canonical);
    }
  }

  public DeduplicateCorrelateVariables(RexBuilder builder,
      String canonical, Set<String> altNames) {
    dedupRex = new DeduplicateCorrelateVariablesShuttle(builder,
        canonical, altNames);
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    LogicalFilter newFilter = (LogicalFilter) super.visit(filter);
    RexNode condition = filter.getCondition();
    RexNode newCondition = condition.accept(dedupRex);
    if (condition != newCondition) {
      return newFilter.copy(newFilter.getTraitSet(), newFilter.getInput(),
          newCondition);
    }
    return newFilter;
  }

  @Override
  public RelNode visit(LogicalProject project) {
    LogicalProject project2 = (LogicalProject) super.visit(project);
    List<RexNode> childExps = project2.getChildExps();
    List<RexNode> newExps = dedupRex.apply(childExps);
    if (childExps != newExps) {
      return project2.copy(project2.getTraitSet(), project2.getInput(),
          newExps, project2.getRowType());
    }
    return project2;
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    LogicalJoin join2 = (LogicalJoin) super.visit(join);
    RexNode condition = join2.getCondition();
    RexNode newCondition = condition.accept(dedupRex);
    if (condition != newCondition) {
      return join2.copy(join2.getTraitSet(), newCondition, join2.getLeft(),
          join2.getRight(), join2.getJoinType(), join2.isSemiJoinDone());
    }
    return join2;
  }
}

// End DeduplicateCorrelateVariables.java
