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
package org.apache.calcite.rex;

import org.apache.calcite.plan.RelOptUtil.Logic;

import com.google.common.collect.Iterables;

import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Visitor that, given the {@link Logic} in force at the root of an
 * expression, computes the Logic in force at every occurrence of a sought
 * sub-expression {@code seek}.  Results are collected in {@code logicCollection}.
 *
 * <p>This value is meaningful only for expressions that evaluate to Boolean values.
 */
public class LogicVisitor extends RexUnaryBiVisitor<@Nullable Logic> {
  private final RexNode seek;
  private final Collection<Logic> logicCollection;

  /** Creates a LogicVisitor.
   *
   * @param seek Expression whose occurrences to find
   * @param logicCollection Receives the Logic in force for each occurrence of {@code seek} */
  private LogicVisitor(RexNode seek, Collection<Logic> logicCollection) {
    super(true);
    this.seek = seek;
    this.logicCollection = logicCollection;
  }

  /** Finds a suitable logic for evaluating {@code seek} within a list of
   * expressions.
   *
   * <p>Chooses a logic that is safe (that is, gives the right
   * answer) with the fewest possibilities (that is, we prefer one that
   * returns [true as true, false as false, unknown as false] over one that
   * distinguishes false from unknown).
   *
   * <p>If {@code seek} occurs multiple times, the result is
   * a single Logic that is safe for every one of them. If the occurrences
   * are evaluated under different Logic values, the result is
   * {@link Logic#TRUE_FALSE_UNKNOWN}, which is safe for any occurrence.
   *
   * @throws IllegalArgumentException if {@code seek} does not occur in
   *   {@code nodes}
   */
  public static Logic find(Logic logic, List<RexNode> nodes,
      RexNode seek) {
    final Set<Logic> set = EnumSet.noneOf(Logic.class);
    final LogicVisitor visitor = new LogicVisitor(seek, set);
    for (RexNode node : nodes) {
      node.accept(visitor, logic);
    }
    // Convert FALSE (which can only exist within LogicVisitor) to
    // UNKNOWN_AS_TRUE.
    if (set.remove(Logic.FALSE)) {
      set.add(Logic.UNKNOWN_AS_TRUE);
    }
    switch (set.size()) {
    case 0:
      throw new IllegalArgumentException("not found: " + seek);
    case 1:
      return Iterables.getOnlyElement(set);
    default:
      return Logic.TRUE_FALSE_UNKNOWN;
    }
  }

  /** Appends to {@code logicList}, for each occurrence of {@code seek}
   * within {@code node} in depth-first order, the Logic in force at that
   * occurrence. */
  public static void collect(RexNode node, RexNode seek, Logic logic,
      List<Logic> logicList) {
    node.accept(new LogicVisitor(seek, logicList), logic);
    // Convert FALSE (which can only exist within LogicVisitor) to
    // UNKNOWN_AS_TRUE.
    Collections.replaceAll(logicList, Logic.FALSE, Logic.UNKNOWN_AS_TRUE);
  }

  @Override public @Nullable Logic visitCall(RexCall call, @Nullable Logic logic) {
    final Logic arg0 = logic;
    switch (call.getKind()) {
    case IS_NOT_NULL:
    case IS_NULL:
      logic = Logic.TRUE_FALSE_UNKNOWN;
      break;
    case IS_TRUE:
    case IS_NOT_TRUE:
      logic = Logic.UNKNOWN_AS_FALSE;
      break;
    case IS_FALSE:
    case IS_NOT_FALSE:
      logic = Logic.UNKNOWN_AS_TRUE;
      break;
    case NOT:
      logic = requireNonNull(logic, "logic").negate2();
      break;
    case CASE:
      logic = Logic.TRUE_FALSE_UNKNOWN;
      break;
    default:
      break;
    }
    switch (requireNonNull(logic, "logic")) {
    case TRUE:
      switch (call.getKind()) {
      case AND:
        break;
      default:
        logic = Logic.TRUE_FALSE_UNKNOWN;
      }
      break;
    default:
      break;
    }
    for (RexNode operand : call.operands) {
      operand.accept(this, logic);
    }
    return end(call, arg0);
  }

  @Override protected @Nullable Logic end(RexNode node, @Nullable Logic arg) {
    if (node.equals(seek)) {
      logicCollection.add(requireNonNull(arg, "arg"));
    }
    return arg;
  }

  @Override public @Nullable Logic visitOver(RexOver over, @Nullable Logic arg) {
    return end(over, arg);
  }

  @Override public @Nullable Logic visitFieldAccess(RexFieldAccess fieldAccess,
      @Nullable Logic arg) {
    // Not a Boolean value
    Logic logic = requireNonNull(arg, "arg");
    if (logic == Logic.TRUE) {
      logic = Logic.TRUE_FALSE_UNKNOWN;
    }
    super.visitFieldAccess(fieldAccess, logic);
    return end(fieldAccess, arg);
  }

  @Override public @Nullable Logic visitSubQuery(RexSubQuery subQuery, @Nullable Logic arg) {
    if (!subQuery.getType().isNullable()) {
      if (arg == Logic.TRUE_FALSE_UNKNOWN) {
        arg = Logic.TRUE_FALSE;
      }
    }
    return end(subQuery, arg);
  }
}
