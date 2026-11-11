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
import org.apache.calcite.rel.core.Correlate;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

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
    final Map<RexNode, Integer> expressions = new LinkedHashMap<>();
    final CorrelatedExpressionSelector collector =
        new CorrelatedExpressionSelector(correlate.getCorrelationId()) {
          @Override RexNode selected(RexNode expression) {
            expressions.computeIfAbsent(expression, key -> expressions.size());
            return expression;
          }
        };
    visitCorrelationScope(right, correlate.getCorrelationId(), collector);
    // Early exit condition
    if (expressions.isEmpty()) {
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

    List<RexNode> callsWithCorrelationOverLeft = new ArrayList<>();
    for (RexNode callInRight : expressions.keySet()) {
      callsWithCorrelationOverLeft.add(
          replaceCorrelationsWithInputRef(callInRight, correlate.getCorrelationId(), builder));
    }
    builder.projectPlus(callsWithCorrelationOverLeft);

    final int newLeft = builder.fields().size();
    final RexBuilder rexBuilder = builder.getRexBuilder();
    final RexNode variable =
        rexBuilder.makeCorrel(builder.peek().getRowType(), correlate.getCorrelationId());
    final CorrelatedExpressionSelector replacements =
        new CorrelatedExpressionSelector(correlate.getCorrelationId()) {
          @Override RexNode selected(RexNode expression) {
            final int slot =
                requireNonNull(expressions.get(expression), "selected expression");
            return rexBuilder.makeFieldAccess(variable, oldLeft + slot);
          }
        };
    // Transform the expressions in the right side using the mapping constructed earlier.
    right = visitCorrelationScope(right, correlate.getCorrelationId(), replacements);
    // References in skipped expressions still need their original left fields.
    final ImmutableBitSet declared = correlate.getRequiredColumns()
        .union(ImmutableBitSet.range(oldLeft, newLeft));
    final ImmutableBitSet requiredColumns =
        CorrelationReferences.requiredColumns(right, correlate.getCorrelationId(), declared);
    final List<RexNode> requiredFields = builder.fields(requiredColumns.asList());
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

  /** Applies the visitor within the scope of an outer correlation binding.
   *
   * <p>The scope is the part of {@code root} where {@code id} refers to the
   * binding defined outside {@code root}. References to nested bindings
   * with the same id are excluded. */
  private static RelNode visitCorrelationScope(RelNode root, CorrelationId id,
      RexShuttle visitor) {
    return root.accept(new RelHomogeneousShuttle() {
      @Override public RelNode visit(RelNode rel) {
        if (rel.getVariablesSet().contains(id)) {
          if (rel instanceof Correlate) {
            return visitChild(rel, 0, rel.getInput(0));
          }
          if (rel instanceof Project || rel instanceof Filter) {
            return visitChildren(rel);
          }
          return rel;
        }
        final RelNode node = rel instanceof Project || rel instanceof Filter
            ? rel.accept(visitor) : rel;
        return visitChildren(node);
      }
    });
  }

  /** Selects deterministic expressions for evaluation on the outer input.
   *
   * <p>The collector stores each selected {@link RexNode} with its offset among
   * the fields added to the left input. A Project computes these fields.
   * The replacement visitor replaces their uses on the right with
   * {@link RexFieldAccess} references.
   *
   * <p>Selected expressions go to {@link #selected(RexNode)} without visiting
   * their operands. Other calls have their operands checked separately.
   * Lambda bodies and sub-query plans are skipped. A complete call containing
   * a deterministic lambda can still be selected.
   *
   * <p>Each occurrence is checked because equal operators may declare different
   * determinism. */
  private abstract static class CorrelatedExpressionSelector extends RexShuttle {
    private final CorrelationId id;
    private final SimpleCorrelationDetector detector;

    CorrelatedExpressionSelector(CorrelationId id) {
      this.id = id;
      this.detector = new SimpleCorrelationDetector(id);
    }

    /** Handles an expression that has passed the extraction checks.
     *
     * @return Expression to use at this occurrence
     */
    abstract RexNode selected(RexNode expression);

    @Override public RexNode visitCall(RexCall call) {
      return Boolean.TRUE.equals(call.accept(detector))
          ? selected(call) : super.visitCall(call);
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess access) {
      if (isDirectFieldAccess(access, id)) {
        return access;
      }
      return Boolean.TRUE.equals(access.accept(detector))
          ? selected(access) : super.visitFieldAccess(access);
    }

    @Override public RexNode visitLambda(RexLambda lambda) {
      return lambda;
    }
  }

  /**
   * A visitor classifying row expressions as simply correlated if they satisfy
   * the conditions below. Lambda parameters are neutral and must be bound
   * within the expression being analyzed.
   *
   * <ul>
   * <li>all correlation fields have the specified correlation id</li>
   * <li>leaves are correlation fields, dynamic parameters, literals, or
   * parameters bound by a contained lambda</li>
   * <li>intermediate nodes are deterministic calls, field accesses, or lambdas
   * whose bodies satisfy these conditions</li>
   * </ul>
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
        if (Boolean.FALSE.equals(b)) {
          return Boolean.FALSE;
        }
        if (Boolean.TRUE.equals(b)) {
          hasSimpleCorrelation = Boolean.TRUE;
        }
      }
      // Null means neutral: no operand contributes a correlation with corrId.
      return hasSimpleCorrelation;
    }

    @Override public @Nullable Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
      if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
        return ((RexCorrelVariable) fieldAccess.getReferenceExpr()).id.equals(corrId);
      }
      return fieldAccess.getReferenceExpr().accept(this);
    }

    @Override public Boolean visitInputRef(RexInputRef inputRef) {
      return Boolean.FALSE;
    }

    @Override public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
      // A whole-row value cannot become a single input reference on the left.
      return Boolean.FALSE;
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

  private static RexNode replaceCorrelationsWithInputRef(RexNode exp, CorrelationId id,
      RelBuilder b) {
    return exp.accept(new RexShuttle() {
      @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
        if (isDirectFieldAccess(fieldAccess, id)) {
          return b.field(fieldAccess.getField().getIndex());
        }
        return super.visitFieldAccess(fieldAccess);
      }
    });
  }

}
