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

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.initialization.qual.NotOnlyInitialized;
import org.checkerframework.checker.initialization.qual.UnderInitialization;

/**
 * Rewrites relations to ensure a correlation references the right row type after trimming.
 */
public class ChangeTypeOfCorrelateVariables extends RelHomogeneousShuttle {
  @NotOnlyInitialized
  private final RexShuttle dedupRex;

  /** Creates a ChangeTypeOfCorrelateVariables. */
  private ChangeTypeOfCorrelateVariables(RexBuilder builder,
      ImmutableSet<CorrelationId> corrIds, RelDataType expectedType) {
    dedupRex = new ChangeTypeOfCorrelateVariablesShuttle(builder,
        corrIds, expectedType, this);
  }

  /**
   * Rewrites a relational expression, replacing correlation variables
   * with a similar one but proper row type.
   */
  public static RelNode go(RexBuilder builder, Iterable<? extends CorrelationId> corrIds,
      RelDataType expectedType, RelNode r) {
    return r.accept(
        new ChangeTypeOfCorrelateVariables(builder, ImmutableSet.copyOf(corrIds), expectedType));
  }

  @Override public RelNode visit(RelNode other) {
    RelNode next = super.visit(other);
    return next.accept(dedupRex);
  }

  /**
   * Replaces row type of correlation variable to the expected one.
   */
  private static class ChangeTypeOfCorrelateVariablesShuttle extends RexShuttle {
    private final RexBuilder builder;
    private final ImmutableSet<CorrelationId> corrIds;
    private final RelDataType expectedType;
    @NotOnlyInitialized
    private final ChangeTypeOfCorrelateVariables shuttle;

    private ChangeTypeOfCorrelateVariablesShuttle(RexBuilder builder,
        ImmutableSet<CorrelationId> corrIds, RelDataType expectedType,
        @UnderInitialization ChangeTypeOfCorrelateVariables shuttle) {
      this.builder = builder;
      this.corrIds = corrIds;
      this.expectedType = expectedType;
      this.shuttle = shuttle;
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      RexNode before = fieldAccess.getReferenceExpr();
      RexNode after = before.accept(this);

      if (before == after) {
        return fieldAccess;
      } else {
        return builder.makeFieldAccess(after,
            fieldAccess.getField().getName(), true);
      }
    }

    @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
      if (!corrIds.contains(variable.id) || variable.getType().equals(expectedType)) {
        return variable;
      }

      return builder.makeCorrel(expectedType, variable.id);
    }

    @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
      if (shuttle != null) {
        RelNode r = subQuery.rel.accept(shuttle); // look inside sub-queries
        if (r != subQuery.rel) {
          subQuery = subQuery.clone(r);
        }
      }
      return super.visitSubQuery(subQuery);
    }
  }
}
