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

import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * Utility class for various methods related to multisets.
 */
public class RexMultisetUtil {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * A set defining all implementable multiset calls
   */
  private static final Set<SqlOperator> MULTISET_OPERATORS =
      ImmutableSet.of(
          SqlStdOperatorTable.CARDINALITY,
          SqlStdOperatorTable.CAST,
          SqlStdOperatorTable.ELEMENT,
          SqlStdOperatorTable.ELEMENT_SLICE,
          SqlStdOperatorTable.MULTISET_EXCEPT_DISTINCT,
          SqlStdOperatorTable.MULTISET_EXCEPT,
          SqlStdOperatorTable.MULTISET_INTERSECT_DISTINCT,
          SqlStdOperatorTable.MULTISET_INTERSECT,
          SqlStdOperatorTable.MULTISET_UNION_DISTINCT,
          SqlStdOperatorTable.MULTISET_UNION,
          SqlStdOperatorTable.IS_A_SET,
          SqlStdOperatorTable.IS_NOT_A_SET,
          SqlStdOperatorTable.MEMBER_OF,
          SqlStdOperatorTable.NOT_SUBMULTISET_OF,
          SqlStdOperatorTable.SUBMULTISET_OF);

  //~ Methods ----------------------------------------------------------------

  private RexMultisetUtil() {
  }

  /**
   * Returns true if any expression in a program contains a mixing between
   * multiset and non-multiset calls.
   */
  public static boolean containsMixing(RexProgram program) {
    RexCallMultisetOperatorCounter counter =
        new RexCallMultisetOperatorCounter();
    for (RexNode expr : program.getExprList()) {
      counter.reset();
      expr.accept(counter);

      if ((counter.totalCount != counter.multisetCount)
          && (0 != counter.multisetCount)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if a node contains a mixing between multiset and
   * non-multiset calls.
   */
  public static boolean containsMixing(RexNode node) {
    RexCallMultisetOperatorCounter counter =
        new RexCallMultisetOperatorCounter();
    node.accept(counter);
    return (counter.totalCount != counter.multisetCount)
        && (0 != counter.multisetCount);
  }

  /**
   * Returns true if node contains a multiset operator, otherwise false. Use
   * it with deep=false when checking if a RexCall is a multiset call.
   *
   * @param node Expression
   * @param deep If true, returns whether expression contains a multiset. If
   *             false, returns whether expression <em>is</em> a multiset.
   */
  public static boolean containsMultiset(final RexNode node, boolean deep) {
    return null != findFirstMultiset(node, deep);
  }

  /**
   * Returns whether a list of expressions contains a multiset.
   */
  public static boolean containsMultiset(List<RexNode> nodes, boolean deep) {
    for (RexNode node : nodes) {
      if (containsMultiset(node, deep)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns whether a program contains a multiset.
   */
  public static boolean containsMultiset(RexProgram program) {
    return containsMultiset(program.getExprList(), true);
  }

  /**
   * Returns true if {@code call} is a call to <code>CAST</code> and the to/from
   * cast types are of multiset types.
   */
  public static boolean isMultisetCast(RexCall call) {
    switch (call.getKind()) {
    case CAST:
      return call.getType().getSqlTypeName() == SqlTypeName.MULTISET;
    default:
      return false;
    }
  }

  /**
   * Returns a reference to the first found multiset call or null if none was
   * found
   */
  public static RexCall findFirstMultiset(final RexNode node, boolean deep) {
    if (node instanceof RexFieldAccess) {
      return findFirstMultiset(
          ((RexFieldAccess) node).getReferenceExpr(),
          deep);
    }

    if (!(node instanceof RexCall)) {
      return null;
    }
    final RexCall call = (RexCall) node;
    RexCall firstOne = null;
    for (SqlOperator op : MULTISET_OPERATORS) {
      firstOne = RexUtil.findOperatorCall(op, call);
      if (null != firstOne) {
        if (firstOne.getOperator().equals(SqlStdOperatorTable.CAST)
            && !isMultisetCast(firstOne)) {
          firstOne = null;
          continue;
        }
        break;
      }
    }

    if (!deep && (firstOne != call)) {
      return null;
    }
    return firstOne;
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * A RexShuttle that traverse all RexNode and counts total number of
   * RexCalls traversed and number of multiset calls traversed.
   *
   * <p>totalCount &ge; multisetCount always holds true.
   */
  private static class RexCallMultisetOperatorCounter
      extends RexVisitorImpl<Void> {
    int totalCount = 0;
    int multisetCount = 0;

    RexCallMultisetOperatorCounter() {
      super(true);
    }

    void reset() {
      totalCount = 0;
      multisetCount = 0;
    }

    public Void visitCall(RexCall call) {
      ++totalCount;
      if (MULTISET_OPERATORS.contains(call.getOperator())) {
        if (!call.getOperator().equals(SqlStdOperatorTable.CAST)
            || isMultisetCast(call)) {
          ++multisetCount;
        }
      }
      return super.visitCall(call);
    }
  }
}

// End RexMultisetUtil.java
