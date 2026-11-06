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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A visitor for relational expressions that extracts a
 * {@link org.apache.calcite.rel.core.Project}, with a "simple" computation over
 * the correlated variables, from the right side of a correlation
 * ({@link org.apache.calcite.rel.core.Correlate}) and places it on the left
 * side.
 *
 * <p>Plan before
 * <pre>
 * LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])
 *   LogicalTableScan(table=[[scott, EMP]])
 *   LogicalFilter(condition=[=($0, +(10, $cor0.DEPTNO))])
 *     LogicalTableScan(table=[[scott, DEPT]])
 * </pre>
 *
 * <p>Plan after
 * <pre>
 * LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],... DNAME=[$10], LOC=[$11])
 *   LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{8}])
 *     LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], ... COMM=[$6], DEPTNO=[$7], $f8=[+(10, $7)])
 *       LogicalTableScan(table=[[scott, EMP]])
 *     LogicalFilter(condition=[=($0, $cor0.$f8)])
 *       LogicalTableScan(table=[[scott, DEPT]])
 * </pre>
 *
 * <p>Essentially this transformation moves the computation over a correlated
 * expression from the inner loop to the outer loop. It materializes the
 * computation on the left side and flattens expressions on correlated variables
 * on the right side.
 */
@API(since = "1.27", status = API.Status.EXPERIMENTAL)
public final class CorrelateProjectExtractor extends RelHomogeneousShuttle {

  private final RelBuilderFactory builderFactory;

  public CorrelateProjectExtractor(RelBuilderFactory factory) {
    this.builderFactory = factory;
  }

  /** Returns whether {@code node} is a direct field access on the correlation
   * variable with the specified id, such as {@code $cor0.DEPTNO}. A nested
   * access such as {@code $cor0.REC.DEPTNO} is not direct. */
  private static boolean isDirectFieldAccess(RexNode node, CorrelationId id) {
    if (node instanceof RexFieldAccess) {
      RexFieldAccess access = (RexFieldAccess) node;
      if (access.getReferenceExpr() instanceof RexCorrelVariable) {
        RexCorrelVariable correlVar = (RexCorrelVariable) access.getReferenceExpr();
        return correlVar.id.equals(id);
      }
    }
    return false;
  }

  @Override public RelNode visit(LogicalCorrelate correlate) {
    RelNode left = correlate.getLeft().accept(this);
    RelNode right = correlate.getRight().accept(this);
    int oldLeft = left.getRowType().getFieldCount();
    // Find the correlated expressions from the right side that can be moved to the left
    Set<RexNode> expressionsToExtract =
        findExtractableExpressions(correlate.getCorrelationId(), right);
    if (expressionsToExtract.isEmpty()) {
      if (correlate.getLeft().equals(left) && correlate.getRight().equals(right)) {
        return correlate;
      } else {
        return correlate.copy(
            correlate.getTraitSet(),
            left,
            right,
            correlate.getCorrelationId(),
            correlate.getRequiredColumns(),
            correlate.getJoinType());
      }
    }

    RelBuilder builder = builderFactory.create(correlate.getCluster(), null);
    // Transform the correlated expression from the right side to an expression over the left side
    builder.push(left);

    List<RexNode> leftExpressions = new ArrayList<>();
    for (RexNode expression : expressionsToExtract) {
      leftExpressions.add(replaceCorrelationsWithInputRef(expression, builder));
    }
    builder.projectPlus(leftExpressions);

    // Construct the mapping to transform the expressions in the right side based on the new
    // projection in the left side.
    Map<RexNode, RexNode> transformMapping = new HashMap<>();
    int newFieldIndex = oldLeft;
    for (RexNode expression : expressionsToExtract) {
      RexBuilder xb = builder.getRexBuilder();
      RexNode v = xb.makeCorrel(builder.peek().getRowType(), correlate.getCorrelationId());
      RexNode flatCorrelationInRight = xb.makeFieldAccess(v, newFieldIndex);
      transformMapping.put(expression, flatCorrelationInRight);
      newFieldIndex++;
    }

    final int newLeft = builder.fields().size();
    // Transform the expressions in the right side using the mapping constructed earlier.
    right = replaceExpressionsUsingMap(right, correlate.getCorrelationId(), transformMapping);
    // Regions left in place, such as lambda bodies, may still read original fields.
    // Bound the remaining references by the old declaration and the added columns.
    final ImmutableBitSet requiredColumns =
        correlate.getRequiredColumns().union(ImmutableBitSet.range(oldLeft, newLeft))
            .intersect(RelOptUtil.correlationColumns(correlate.getCorrelationId(), right));
    List<RexNode> requiredFields = builder.fields(requiredColumns.asList());
    builder.push(right);

    builder.correlate(correlate.getJoinType(), correlate.getCorrelationId(), requiredFields);
    // Remove the additional fields that were added for the needs of the correlation to keep the old
    // and new plan equivalent.
    List<Integer> retainFields;
    switch (correlate.getJoinType()) {
    case SEMI:
    case ANTI:
      retainFields = ImmutableBitSet.range(0, oldLeft).asList();
      break;
    case LEFT:
    case INNER:
      retainFields =
          ImmutableBitSet.builder()
              .set(0, oldLeft)
              .set(newLeft, newLeft + right.getRowType().getFieldCount())
              .build()
              .asList();
      break;
    default:
      throw new AssertionError(correlate.getJoinType());
    }
    builder.project(builder.fields(retainFields));
    return builder.build();
  }

  /**
   * Finds calls and nested field accesses that can move to the left input,
   * within the scope of the specified correlation.
   */
  private static Set<RexNode> findExtractableExpressions(CorrelationId corrId, RelNode plan) {
    SimpleCorrelationCollector finder = new SimpleCorrelationCollector(corrId);
    visitCorrelationScope(plan, corrId, finder);
    return finder.expressions;
  }

  /** Replaces mapped expressions in the scope of {@code id}, using the same
   * traversal as {@link #findExtractableExpressions}. */
  private static RelNode replaceExpressionsUsingMap(RelNode plan, CorrelationId id,
      Map<RexNode, RexNode> mapping) {
    return visitCorrelationScope(plan, id, new ExpressionReplacer(mapping));
  }

  /** Visits Project and Filter expressions in the scope of {@code id}.
   *
   * <p>If a Project or Filter binds the id, skips its expressions but visits
   * its input. Other nodes that bind the id are left untouched.
   */
  private static RelNode visitCorrelationScope(RelNode plan, CorrelationId id,
      RexShuttle visitor) {
    return plan.accept(new RelHomogeneousShuttle() {
      @Override public RelNode visit(RelNode other) {
        final boolean expressionNode = other instanceof Project || other instanceof Filter;
        final boolean bindsId = other.getVariablesSet().contains(id);
        if (bindsId && !expressionNode) {
          // Conservatively leave other nested bindings untouched.
          return other;
        }
        final RelNode node = expressionNode && !bindsId ? other.accept(visitor) : other;
        // Project and Filter bindings apply to their expressions, not their inputs.
        return super.visit(node);
      }
    });
  }

  /**
   * Collects calls and nested field accesses that can move to the left input.
   *
   * <p>Selects the outermost eligible expression. Direct field accesses such as
   * {@code $cor0.A} stay in place; nested accesses such as {@code $cor0.R.A}
   * may be extracted.
   *
   * <p>Does not collect candidates from lambda bodies or sub-query plans.
   * An enclosing call may still be selected if {@link SimpleCorrelationDetector}
   * accepts it as a whole.
   */
  private static final class SimpleCorrelationCollector extends RexShuttle {
    private final CorrelationId correlationId;
    // Clients are iterating over the collection thus it is better to use LinkedHashSet to keep
    // plans stable among executions.
    private final Set<RexNode> expressions = new LinkedHashSet<>();

    SimpleCorrelationCollector(CorrelationId corrId) {
      this.correlationId = corrId;
    }

    @Override public RexNode visitCall(RexCall call) {
      if (isSimpleCorrelatedExpression(call, correlationId)) {
        expressions.add(call);
        return call;
      } else {
        return super.visitCall(call);
      }
    }

    @Override public RexNode visitLambda(RexLambda lambda) {
      // Expressions in the body may depend on the lambda's parameters.
      return lambda;
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      if (!isDirectFieldAccess(fieldAccess, correlationId)
          && isSimpleCorrelatedExpression(fieldAccess, correlationId)) {
        expressions.add(fieldAccess);
        return fieldAccess;
      } else {
        return super.visitFieldAccess(fieldAccess);
      }
    }
  }

  /**
   * Returns whether the specified node is a simply correlated expression.
   */
  private static boolean isSimpleCorrelatedExpression(RexNode node, CorrelationId id) {
    Boolean r = node.accept(new SimpleCorrelationDetector(id));
    return r == null ? Boolean.FALSE : r;
  }

  /**
   * Classifies expressions by their dependence on the specified correlation.
   *
   * <p>Returns true if an expression uses that correlation and has no disallowed
   * operands. Returns false for other correlation ids, input or local references,
   * window expressions, sub-queries, or non-deterministic calls. Otherwise returns
   * null, which is neutral when combining operands and does not qualify an
   * expression for extraction on its own.
   *
   * <p>Lambda bodies are checked as part of the enclosing expression; their
   * parameters are neutral. A call containing a lambda can qualify as a whole.
   *
   * <p>Examples:
   *
   * <pre>{@code
   * +(10, $cor0.DEPTNO) -> TRUE
   * /(100,+(10, $cor0.DEPTNO)) -> TRUE
   * CAST(+(10, $cor0.DEPTNO)):INTEGER NOT NULL -> TRUE
   * CASE(IS NOT NULL($cor0.PATH), $cor0.PATH, ARRAY(null:INTEGER)) -> TRUE
   * +($0, $cor0.DEPTNO) -> FALSE
   * }</pre>
   *
   * <p>A deterministic subexpression built only from literals and dynamic parameters, such
   * as {@code ARRAY(null:INTEGER)} above, is neutral: it neither qualifies nor
   * disqualifies the enclosing call.
   */
  private static class SimpleCorrelationDetector
      extends RexVisitorImpl<@Nullable Boolean> {
    private final CorrelationId corrId;

    private SimpleCorrelationDetector(CorrelationId corrId) {
      super(true);
      this.corrId = corrId;
    }

    @Override public Boolean visitOver(RexOver over) {
      return Boolean.FALSE;
    }

    @Override public Boolean visitSubQuery(RexSubQuery subQuery) {
      return Boolean.FALSE;
    }

    @Override public @Nullable Boolean visitCall(RexCall call) {
      if (!call.getOperator().isDeterministic()) {
        return Boolean.FALSE;
      }
      // Constant operands must not disqualify the call
      Boolean hasSimpleCorrelation = null;
      for (RexNode op : call.operands) {
        Boolean b = op.accept(this);
        if (b != null) {
          hasSimpleCorrelation = hasSimpleCorrelation == null ? b : hasSimpleCorrelation && b;
        }
      }
      return hasSimpleCorrelation;
    }

    @Override public @Nullable Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
      return fieldAccess.getReferenceExpr().accept(this);
    }

    @Override public Boolean visitInputRef(RexInputRef inputRef) {
      return Boolean.FALSE;
    }

    @Override public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
      return correlVariable.id.equals(corrId);
    }

    @Override public Boolean visitTableInputRef(RexTableInputRef ref) {
      return Boolean.FALSE;
    }

    @Override public Boolean visitLocalRef(RexLocalRef localRef) {
      return Boolean.FALSE;
    }

    @Override public Boolean visitPatternFieldRef(RexPatternFieldRef fieldRef) {
      return Boolean.FALSE;
    }
  }

  private static RexNode replaceCorrelationsWithInputRef(RexNode exp, RelBuilder b) {
    return exp.accept(new RexShuttle() {
      @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
        if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
          return b.field(fieldAccess.getField().getIndex());
        }
        return super.visitFieldAccess(fieldAccess);
      }
    });
  }

  /**
   * A visitor traversing row expressions and replacing calls and field
   * accesses with other expressions according to the specified mapping.
   * The mapping is consulted before recursing so that the outermost
   * matching expression wins. Does not descend into lambda bodies or sub-query
   * plans.
   */
  private static final class ExpressionReplacer extends RexShuttle {
    private final Map<RexNode, RexNode> mapping;

    ExpressionReplacer(Map<RexNode, RexNode> mapping) {
      this.mapping = mapping;
    }

    @Override public RexNode visitLambda(RexLambda lambda) {
      return lambda;
    }

    @Override public RexNode visitCall(RexCall oldCall) {
      RexNode newCall = mapping.get(oldCall);
      if (newCall != null) {
        return newCall;
      } else {
        return super.visitCall(oldCall);
      }
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      RexNode replacement = mapping.get(fieldAccess);
      if (replacement != null) {
        return replacement;
      } else {
        return super.visitFieldAccess(fieldAccess);
      }
    }
  }
}
