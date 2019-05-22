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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/** Utilities for strong predicates.
 *
 * <p>A predicate is strong (or null-rejecting) if it is UNKNOWN if any of its
 * inputs is UNKNOWN.</p>
 *
 * <p>By the way, UNKNOWN is just the boolean form of NULL.</p>
 *
 * <p>Examples:</p>
 * <ul>
 *   <li>{@code UNKNOWN} is strong in [] (definitely null)
 *   <li>{@code c = 1} is strong in [c] (definitely null if and only if c is
 *   null)
 *   <li>{@code c IS NULL} is not strong (always returns TRUE or FALSE, never
 *   null)
 *   <li>{@code p1 AND p2} is strong in [p1, p2] (definitely null if either p1
 *   is null or p2 is null)
 *   <li>{@code p1 OR p2} is strong if p1 and p2 are strong
 *   <li>{@code c1 = 1 OR c2 IS NULL} is strong in [c1] (definitely null if c1
 *   is null)
 * </ul>
 */
public class Strong {
  private static final Map<SqlKind, Policy> MAP = createPolicyMap();

  public Strong() {
    super();
  }

  /** Returns a checker that consults a bit set to find out whether particular
   * inputs may be null. */
  public static Strong of(final ImmutableBitSet nullColumns) {
    return new Strong() {
      @Override public boolean isNull(RexInputRef ref) {
        return nullColumns.get(ref.getIndex());
      }
    };
  }

  /** Returns whether the analyzed expression will definitely return null if
   * all of a given set of input columns are null. */
  public static boolean isNull(RexNode node, ImmutableBitSet nullColumns) {
    return of(nullColumns).isNull(node);
  }

  /** Returns whether the analyzed expression will definitely not return true
   * (equivalently, will definitely not return null or false) if
   * all of a given set of input columns are null. */
  public static boolean isNotTrue(RexNode node, ImmutableBitSet nullColumns) {
    return of(nullColumns).isNotTrue(node);
  }

  /** Returns how to deduce whether a particular kind of expression is null,
   * given whether its arguments are null. */
  public static Policy policy(SqlKind kind) {
    return MAP.getOrDefault(kind, Policy.AS_IS);
  }

  /**
   * Returns whether a given expression is strong.
   *
   * <p>Examples:</p>
   * <ul>
   *   <li>Returns true for {@code c = 1} since it returns null if and only if
   *   c is null
   *   <li>Returns false for {@code c IS NULL} since it always returns TRUE
   *   or FALSE
   *</ul>
   *
   * @param e Expression
   * @return true if the expression is strong, false otherwise
   */
  public static boolean isStrong(RexNode e) {
    final ImmutableBitSet.Builder nullColumns = ImmutableBitSet.builder();
    e.accept(
        new RexVisitorImpl<Void>(true) {
          public Void visitInputRef(RexInputRef inputRef) {
            nullColumns.set(inputRef.getIndex());
            return super.visitInputRef(inputRef);
          }
        });
    return isNull(e, nullColumns.build());
  }

  /** Returns whether all expressions in a list are strong. */
  public static boolean allStrong(List<RexNode> operands) {
    return operands.stream().allMatch(Strong::isStrong);
  }

  /** Returns whether an expression is definitely not true. */
  public boolean isNotTrue(RexNode node) {
    switch (node.getKind()) {
    case IS_NOT_NULL:
      return anyNull(((RexCall) node).getOperands());
    default:
      return isNull(node);
    }
  }

  /** Returns whether an expression is definitely null.
   *
   * <p>The answer is based on calls to {@link #isNull} for its constituent
   * expressions, and you may override methods to test hypotheses such as
   * "if {@code x} is null, is {@code x + y} null? */
  public boolean isNull(RexNode node) {
    final Policy policy = policy(node.getKind());
    switch (policy) {
    case NOT_NULL:
      return false;
    case ANY:
      return anyNull(((RexCall) node).getOperands());
    default:
      break;
    }

    switch (node.getKind()) {
    case LITERAL:
      return ((RexLiteral) node).isNull();
    // We can only guarantee AND to return NULL if both inputs are NULL  (similar for OR)
    // AND(NULL, FALSE) = FALSE
    case AND:
    case OR:
    case COALESCE:
      return allNull(((RexCall) node).getOperands());
    case NULLIF:
      // NULLIF(null, X) where X can be NULL, returns NULL
      // NULLIF(X, Y) where X is not NULL, then this may return NULL if X = Y, otherwise X.
      return allNull(ImmutableList.of(((RexCall) node).getOperands().get(0)));
    case INPUT_REF:
      return isNull((RexInputRef) node);
    case CASE:
      final RexCall caseCall = (RexCall) node;
      final List<RexNode> caseValues = new ArrayList<>();
      for (int i = 0; i < caseCall.getOperands().size(); i++) {
        if (!RexUtil.isCasePredicate(caseCall, i)) {
          caseValues.add(caseCall.getOperands().get(i));
        }
      }
      return allNull(caseValues);
    default:
      return false;
    }
  }

  /** Returns whether a given input is definitely null. */
  public boolean isNull(RexInputRef ref) {
    return false;
  }

  /** Returns whether all expressions in a list are definitely null. */
  private boolean allNull(List<RexNode> operands) {
    for (RexNode operand : operands) {
      if (!isNull(operand)) {
        return false;
      }
    }
    return true;
  }

  /** Returns whether any expressions in a list are definitely null. */
  private boolean anyNull(List<RexNode> operands) {
    for (RexNode operand : operands) {
      if (isNull(operand)) {
        return true;
      }
    }
    return false;
  }

  private static Map<SqlKind, Policy> createPolicyMap() {
    EnumMap<SqlKind, Policy> map = new EnumMap<>(SqlKind.class);

    map.put(SqlKind.INPUT_REF, Policy.AS_IS);
    map.put(SqlKind.LOCAL_REF, Policy.AS_IS);
    map.put(SqlKind.DYNAMIC_PARAM, Policy.AS_IS);
    map.put(SqlKind.OTHER_FUNCTION, Policy.AS_IS);

    // The following types of expressions could potentially be custom.
    map.put(SqlKind.CASE, Policy.AS_IS);
    map.put(SqlKind.DECODE, Policy.AS_IS);
    // NULLIF(1, NULL) yields 1, but NULLIF(1, 1) yields NULL
    map.put(SqlKind.NULLIF, Policy.AS_IS);
    // COALESCE(NULL, 2) yields 2
    map.put(SqlKind.COALESCE, Policy.AS_IS);
    map.put(SqlKind.NVL, Policy.AS_IS);
    // FALSE AND NULL yields FALSE
    // TRUE AND NULL yields NULL
    map.put(SqlKind.AND, Policy.AS_IS);
    // TRUE OR NULL yields TRUE
    // FALSE OR NULL yields NULL
    map.put(SqlKind.OR, Policy.AS_IS);

    // Expression types with custom handlers.
    map.put(SqlKind.LITERAL, Policy.CUSTOM);

    map.put(SqlKind.EXISTS, Policy.NOT_NULL);
    map.put(SqlKind.IS_DISTINCT_FROM, Policy.NOT_NULL);
    map.put(SqlKind.IS_NOT_DISTINCT_FROM, Policy.NOT_NULL);
    map.put(SqlKind.IS_NULL, Policy.NOT_NULL);
    map.put(SqlKind.IS_NOT_NULL, Policy.NOT_NULL);
    map.put(SqlKind.IS_TRUE, Policy.NOT_NULL);
    map.put(SqlKind.IS_NOT_TRUE, Policy.NOT_NULL);
    map.put(SqlKind.IS_FALSE, Policy.NOT_NULL);
    map.put(SqlKind.IS_NOT_FALSE, Policy.NOT_NULL);

    map.put(SqlKind.NOT, Policy.ANY);
    map.put(SqlKind.EQUALS, Policy.ANY);
    map.put(SqlKind.NOT_EQUALS, Policy.ANY);
    map.put(SqlKind.LESS_THAN, Policy.ANY);
    map.put(SqlKind.LESS_THAN_OR_EQUAL, Policy.ANY);
    map.put(SqlKind.GREATER_THAN, Policy.ANY);
    map.put(SqlKind.GREATER_THAN_OR_EQUAL, Policy.ANY);
    map.put(SqlKind.LIKE, Policy.ANY);
    map.put(SqlKind.SIMILAR, Policy.ANY);
    map.put(SqlKind.PLUS, Policy.ANY);
    map.put(SqlKind.PLUS_PREFIX, Policy.ANY);
    map.put(SqlKind.MINUS, Policy.ANY);
    map.put(SqlKind.MINUS_PREFIX, Policy.ANY);
    map.put(SqlKind.TIMES, Policy.ANY);
    map.put(SqlKind.DIVIDE, Policy.ANY);
    map.put(SqlKind.CAST, Policy.ANY);
    map.put(SqlKind.REINTERPRET, Policy.ANY);
    map.put(SqlKind.TRIM, Policy.ANY);
    map.put(SqlKind.LTRIM, Policy.ANY);
    map.put(SqlKind.RTRIM, Policy.ANY);
    map.put(SqlKind.CEIL, Policy.ANY);
    map.put(SqlKind.FLOOR, Policy.ANY);
    map.put(SqlKind.EXTRACT, Policy.ANY);
    map.put(SqlKind.GREATEST, Policy.ANY);
    map.put(SqlKind.LEAST, Policy.ANY);
    map.put(SqlKind.TIMESTAMP_ADD, Policy.ANY);
    map.put(SqlKind.TIMESTAMP_DIFF, Policy.ANY);

    // Assume that any other expressions cannot be simplified.
    for (SqlKind k
        : Iterables.concat(SqlKind.EXPRESSION, SqlKind.AGGREGATE)) {
      if (!map.containsKey(k)) {
        map.put(k, Policy.AS_IS);
      }
    }
    return map;
  }

  /** How whether an operator's operands are null affects whether a call to
   * that operator evaluates to null. */
  public enum Policy {
    /** This kind of expression is never null. No need to look at its arguments,
     * if it has any. */
    NOT_NULL,

    /** This kind of expression has its own particular rules about whether it
     * is null. */
    CUSTOM,

    /** This kind of expression is null if and only if at least one of its
     * arguments is null. */
    ANY,

    /** This kind of expression may be null. There is no way to rewrite. */
    AS_IS,
  }
}

// End Strong.java
