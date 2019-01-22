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

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Visitor pattern for traversing a tree of {@link RexNode} objects.
 */
public class LogicVisitor implements RexBiVisitor<Logic, Logic> {
  private final RexNode seek;
  private final Collection<Logic> logicCollection;

  /** Creates a LogicVisitor. */
  private LogicVisitor(RexNode seek, Collection<Logic> logicCollection) {
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

  public static void collect(RexNode node, RexNode seek, Logic logic,
      List<Logic> logicList) {
    node.accept(new LogicVisitor(seek, logicList), logic);
    // Convert FALSE (which can only exist within LogicVisitor) to
    // UNKNOWN_AS_TRUE.
    Collections.replaceAll(logicList, Logic.FALSE, Logic.UNKNOWN_AS_TRUE);
  }

  public Logic visitCall(RexCall call, Logic logic) {
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
      logic = logic.negate2();
      break;
    case CASE:
      logic = Logic.TRUE_FALSE_UNKNOWN;
      break;
    }
    switch (logic) {
    case TRUE:
      switch (call.getKind()) {
      case AND:
        break;
      default:
        logic = Logic.TRUE_FALSE_UNKNOWN;
      }
    }
    for (RexNode operand : call.operands) {
      operand.accept(this, logic);
    }
    return end(call, arg0);
  }

  private Logic end(RexNode node, Logic arg) {
    if (node.equals(seek)) {
      logicCollection.add(arg);
    }
    return arg;
  }

  public Logic visitInputRef(RexInputRef inputRef, Logic arg) {
    return end(inputRef, arg);
  }

  public Logic visitLocalRef(RexLocalRef localRef, Logic arg) {
    return end(localRef, arg);
  }

  public Logic visitLiteral(RexLiteral literal, Logic arg) {
    return end(literal, arg);
  }

  public Logic visitOver(RexOver over, Logic arg) {
    return end(over, arg);
  }

  public Logic visitCorrelVariable(RexCorrelVariable correlVariable,
      Logic arg) {
    return end(correlVariable, arg);
  }

  public Logic visitDynamicParam(RexDynamicParam dynamicParam, Logic arg) {
    return end(dynamicParam, arg);
  }

  public Logic visitRangeRef(RexRangeRef rangeRef, Logic arg) {
    return end(rangeRef, arg);
  }

  public Logic visitFieldAccess(RexFieldAccess fieldAccess, Logic arg) {
    return end(fieldAccess, arg);
  }

  public Logic visitSubQuery(RexSubQuery subQuery, Logic arg) {
    if (!subQuery.getType().isNullable()) {
      if (arg == Logic.TRUE_FALSE_UNKNOWN) {
        arg = Logic.TRUE_FALSE;
      }
    }
    return end(subQuery, arg);
  }

  @Override public Logic visitTableInputRef(RexTableInputRef ref, Logic arg) {
    return end(ref, arg);
  }

  @Override public Logic visitPatternFieldRef(RexPatternFieldRef ref, Logic arg) {
    return end(ref, arg);
  }
}

// End LogicVisitor.java
