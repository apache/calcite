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
package org.apache.calcite.plan;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Litmus;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Predicates that are known to hold in the output of a particular relational
 * expression.
 *
 * <p><b>Pulled up predicates</b> (field {@link #pulledUpPredicates} are
 * predicates that apply to every row output by the relational expression. They
 * are inferred from the input relational expression(s) and the relational
 * operator.
 *
 * <p>For example, if you apply {@code Filter(x > 1)} to a relational
 * expression that has a predicate {@code y < 10} then the pulled up predicates
 * for the Filter are {@code [y < 10, x > 1]}.
 *
 * <p><b>Inferred predicates</b> only apply to joins. If there is a
 * predicate on the left input to a join, and that predicate is over columns
 * used in the join condition, then a predicate can be inferred on the right
 * input to the join. (And vice versa.)
 *
 * <p>For example, in the query
 * <blockquote>SELECT *<br>
 * FROM emp<br>
 * JOIN dept ON emp.deptno = dept.deptno
 * WHERE emp.gender = 'F' AND emp.deptno &lt; 10</blockquote>
 * we have
 * <ul>
 *   <li>left: {@code Filter(Scan(EMP), deptno < 10},
 *       predicates: {@code [deptno < 10]}
 *   <li>right: {@code Scan(DEPT)}, predicates: {@code []}
 *   <li>join: {@code Join(left, right, emp.deptno = dept.deptno},
 *      leftInferredPredicates: [],
 *      rightInferredPredicates: [deptno &lt; 10],
 *      pulledUpPredicates: [emp.gender = 'F', emp.deptno &lt; 10,
 *      emp.deptno = dept.deptno, dept.deptno &lt; 10]
 * </ul>
 *
 * <p>Note that the predicate from the left input appears in
 * {@code rightInferredPredicates}. Predicates from several sources appear in
 * {@code pulledUpPredicates}.
 */
public class RelOptPredicateList {
  private static final ImmutableList<RexNode> EMPTY_LIST = ImmutableList.of();
  public static final RelOptPredicateList EMPTY =
      new RelOptPredicateList(EMPTY_LIST, EMPTY_LIST, EMPTY_LIST,
          ImmutableMap.of());

  /** Predicates that can be pulled up from the relational expression and its
   * inputs. */
  public final ImmutableList<RexNode> pulledUpPredicates;

  /** Predicates that were inferred from the right input.
   * Empty if the relational expression is not a join. */
  public final ImmutableList<RexNode> leftInferredPredicates;

  /** Predicates that were inferred from the left input.
   * Empty if the relational expression is not a join. */
  public final ImmutableList<RexNode> rightInferredPredicates;

  /** A map of each (e, constant) pair that occurs within
   * {@link #pulledUpPredicates}. */
  public final ImmutableMap<RexNode, RexNode> constantMap;

  // Please keep this constructor private; if you add additional constructors,
  // please check the invariants similar to this constructor.
  private RelOptPredicateList(ImmutableList<RexNode> pulledUpPredicates,
      ImmutableList<RexNode> leftInferredPredicates,
      ImmutableList<RexNode> rightInferredPredicates,
      ImmutableMap<RexNode, RexNode> constantMap) {
    this.pulledUpPredicates =
        requireNonNull(pulledUpPredicates, "pulledUpPredicates");
    this.leftInferredPredicates =
        requireNonNull(leftInferredPredicates, "leftInferredPredicates");
    this.rightInferredPredicates =
        requireNonNull(rightInferredPredicates, "rightInferredPredicates");
    this.constantMap = requireNonNull(constantMap, "constantMap");

    // Validate invariants required
    // (unfortunately the style rules don't allow us to move this to a separate function).
    // Do not allow comparisons with null literals in pulledUpPredicates
    for (RexNode predicate : this.pulledUpPredicates) {
      switch (predicate.getKind()) {
        // note that IS_DISTINCT_FROM is not in this list
      case EQUALS:
      case NOT_EQUALS:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        final RexCall call = (RexCall) predicate;
        final RexNode left = call.getOperands().get(0);
        if (left.getKind() == SqlKind.LITERAL) {
          RexLiteral literal = (RexLiteral) left;
          Litmus.THROW.check(literal.getValue() != null,
              "Comparison with NULL in pulledUpPredicates");
        }
        final RexNode right = call.getOperands().get(1);
        if (right.getKind() == SqlKind.LITERAL) {
          RexLiteral literal = (RexLiteral) right;
          Litmus.THROW.check(literal.getValue() != null,
              "Comparison with NULL in pulledUpPredicates");
        }
        break;
      default:
        break;
      }
    }
  }

  /** Creates a RelOptPredicateList with only pulled-up predicates, no inferred
   * predicates.
   *
   * <p>Use this for relational expressions other than joins.
   *
   * @param pulledUpPredicates Predicates that apply to the rows returned by the
   * relational expression
   */
  public static RelOptPredicateList of(RexBuilder rexBuilder,
      Iterable<RexNode> pulledUpPredicates) {
    ImmutableList<RexNode> pulledUpPredicatesList =
        ImmutableList.copyOf(pulledUpPredicates);
    if (pulledUpPredicatesList.isEmpty()) {
      return EMPTY;
    }
    return of(rexBuilder, pulledUpPredicatesList, EMPTY_LIST, EMPTY_LIST);
  }

  /**
   * Returns true if given predicate list is empty.
   *
   * @param value input predicate list
   * @return true if all the predicates are empty or if the argument is null
   */
  public static boolean isEmpty(@Nullable RelOptPredicateList value) {
    if (value == null || value == EMPTY) {
      return true;
    }
    return value.constantMap.isEmpty()
        && value.leftInferredPredicates.isEmpty()
        && value.rightInferredPredicates.isEmpty()
        && value.pulledUpPredicates.isEmpty();
  }

  /** Creates a RelOptPredicateList for a join.
   *
   * @param rexBuilder Rex builder
   * @param pulledUpPredicates Predicates that apply to the rows returned by the
   * relational expression
   * @param leftInferredPredicates Predicates that were inferred from the right
   *                               input
   * @param rightInferredPredicates Predicates that were inferred from the left
   *                                input
   */
  public static RelOptPredicateList of(RexBuilder rexBuilder,
      Iterable<RexNode> pulledUpPredicates,
      Iterable<RexNode> leftInferredPredicates,
      Iterable<RexNode> rightInferredPredicates) {
    final ImmutableList<RexNode> pulledUpPredicatesList =
        ImmutableList.copyOf(pulledUpPredicates);
    final ImmutableList<RexNode> leftInferredPredicateList =
        ImmutableList.copyOf(leftInferredPredicates);
    final ImmutableList<RexNode> rightInferredPredicatesList =
        ImmutableList.copyOf(rightInferredPredicates);
    if (pulledUpPredicatesList.isEmpty()
        && leftInferredPredicateList.isEmpty()
        && rightInferredPredicatesList.isEmpty()) {
      return EMPTY;
    }
    final ImmutableMap<RexNode, RexNode> constantMap =
        RexUtil.predicateConstants(RexNode.class, rexBuilder,
            pulledUpPredicatesList);
    return new RelOptPredicateList(pulledUpPredicatesList,
        leftInferredPredicateList, rightInferredPredicatesList, constantMap);
  }

  @Override public String toString() {
    final StringBuilder b = new StringBuilder("{");
    append(b, "pulled", pulledUpPredicates);
    append(b, "left", leftInferredPredicates);
    append(b, "right", rightInferredPredicates);
    append(b, "constants", constantMap.entrySet());
    return b.append("}").toString();
  }

  private static void append(StringBuilder b, String key, Collection<?> value) {
    if (!value.isEmpty()) {
      if (b.length() > 1) {
        b.append(", ");
      }
      b.append(key);
      b.append(value);
    }
  }

  public RelOptPredicateList union(RexBuilder rexBuilder,
      RelOptPredicateList list) {
    if (this == EMPTY) {
      return list;
    } else if (list == EMPTY) {
      return this;
    } else {
      return RelOptPredicateList.of(rexBuilder,
          concat(pulledUpPredicates, list.pulledUpPredicates),
          concat(leftInferredPredicates, list.leftInferredPredicates),
          concat(rightInferredPredicates, list.rightInferredPredicates));
    }
  }

  /** Concatenates two immutable lists, avoiding a copy it possible. */
  private static <E> ImmutableList<E> concat(ImmutableList<E> list1,
      ImmutableList<E> list2) {
    if (list1.isEmpty()) {
      return list2;
    } else if (list2.isEmpty()) {
      return list1;
    } else {
      return ImmutableList.<E>builder().addAll(list1).addAll(list2).build();
    }
  }

  public RelOptPredicateList shift(RexBuilder rexBuilder, int offset) {
    return RelOptPredicateList.of(rexBuilder,
        RexUtil.shift(pulledUpPredicates, offset),
        RexUtil.shift(leftInferredPredicates, offset),
        RexUtil.shift(rightInferredPredicates, offset));
  }

  /** Returns whether an expression is effectively NOT NULL due to an
   * {@code e IS NOT NULL} condition in this predicate list. */
  public boolean isEffectivelyNotNull(RexNode e) {
    if (!e.getType().isNullable()) {
      return true;
    }
    for (RexNode p : pulledUpPredicates) {
      if (p.getKind() == SqlKind.IS_NOT_NULL) {
        // if e IS NOT NULL and e is TINYINT then cast(e as INTEGER) IS NOT NULL
        if (RexUtil.isLosslessCast(e)) {
          if (isEffectivelyNotNull(((RexCall) e).getOperands().get(0))) {
            return true;
          }
        }
        if (((RexCall) p).getOperands().get(0).equals(e)) {
          return true;
        }
      }
    }
    if (SqlKind.COMPARISON.contains(e.getKind())) {
      List<RexNode> operands = ((RexCall) e).getOperands();
      for (RexNode operand : operands) {
        if (!isEffectivelyNotNull(operand)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}
