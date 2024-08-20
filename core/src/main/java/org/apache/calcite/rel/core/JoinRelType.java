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
package org.apache.calcite.rel.core;

import org.apiguardian.api.API;

import java.util.Locale;

/**
 * Enumeration of join types.
 */
public enum JoinRelType {
  /**
   * Inner join.
   */
  INNER,

  /**
   * Left-outer join.
   */
  LEFT,

  /**
   * Right-outer join.
   */
  RIGHT,

  /**
   * Full-outer join.
   */
  FULL,

  /**
   * Semi-join.
   *
   * <p>For example, {@code EMP semi-join DEPT} finds all {@code EMP} records
   * that have a corresponding {@code DEPT} record:
   *
   * <blockquote><pre>
   * SELECT * FROM EMP
   * WHERE EXISTS (SELECT 1 FROM DEPT
   *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
   * </blockquote>
   */
  SEMI,

  /**
   * Anti-join (also known as Anti-semi-join).
   *
   * <p>For example, {@code EMP anti-join DEPT} finds all {@code EMP} records
   * that do not have a corresponding {@code DEPT} record:
   *
   * <blockquote><pre>
   * SELECT * FROM EMP
   * WHERE NOT EXISTS (SELECT 1 FROM DEPT
   *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
   * </blockquote>
   */
  ANTI,

  /**
   * An ASOF JOIN operation combines rows from two tables based on comparable timestamp values.
   * For each row in the left table, the join finds at most one row in the right table that has the
   * "closest" timestamp value. The matched row on the right side is the closest match,
   * which could less than or equal or greater than or equal in the timestamp column,
   * as specified by the comparison operator.
   *
   * <p>Example:
   * <blockquote><pre>
   * FROM left_table ASOF JOIN right_table
   *   MATCH_CONDITION ( left_table.timecol &le; right_table.timecol )
   *   ON left_table.col = right_table.col</pre>
   * </blockquote>
   */
  ASOF,

  /**
   * The left version of an ASOF join, where each row from the left table is part of the output.
   */
  LEFT_ASOF;

  /** Lower-case name. */
  public final String lowerName = name().toLowerCase(Locale.ROOT);

  /**
   * Returns whether a join of this type may generate NULL values on the
   * right-hand side.
   */
  public boolean generatesNullsOnRight() {
    return (this == LEFT) || (this == FULL) || (this == LEFT_ASOF);
  }

  /**
   * Returns whether a join of this type may generate NULL values on the
   * left-hand side.
   */
  public boolean generatesNullsOnLeft() {
    return (this == RIGHT) || (this == FULL);
  }

  /**
   * Returns whether a join of this type is an outer join, returns true if the join type may
   * generate NULL values, either on the left-hand side or right-hand side.
   */
  public boolean isOuterJoin() {
    return (this == LEFT) || (this == RIGHT) || (this == FULL) || (this == LEFT_ASOF);
  }

  /**
   * Swaps left to right, and vice versa.
   */
  public JoinRelType swap() {
    switch (this) {
    case LEFT:
      return RIGHT;
    case RIGHT:
      return LEFT;
    default:
      return this;
    }
  }

  /** Returns whether this join type generates nulls on side #{@code i}. */
  public boolean generatesNullsOn(int i) {
    switch (i) {
    case 0:
      return generatesNullsOnLeft();
    case 1:
      return generatesNullsOnRight();
    default:
      throw new IllegalArgumentException("invalid: " + i);
    }
  }

  /** Returns a join type similar to this but that does not generate nulls on
   * the left. */
  public JoinRelType cancelNullsOnLeft() {
    switch (this) {
    case RIGHT:
      return INNER;
    case FULL:
      return LEFT;
    default:
      return this;
    }
  }

  /** Returns a join type similar to this but that does not generate nulls on
   * the right. */
  public JoinRelType cancelNullsOnRight() {
    switch (this) {
    case LEFT:
      return INNER;
    case FULL:
      return RIGHT;
    default:
      return this;
    }
  }

  public boolean projectsRight() {
    return this != SEMI && this != ANTI;
  }

  /** Returns whether this join type accepts pushing predicates from above into its predicate. */
  @API(since = "1.28", status = API.Status.EXPERIMENTAL)
  public boolean canPushIntoFromAbove() {
    return (this == INNER) || (this == SEMI);
  }

  /** Returns whether this join type accepts pushing predicates from above into its left input. */
  @API(since = "1.28", status = API.Status.EXPERIMENTAL)
  public boolean canPushLeftFromAbove() {
    return (this == INNER) || (this == LEFT) || (this == SEMI) || (this == ANTI);
  }

  /** Returns whether this join type accepts pushing predicates from above into its right input. */
  @API(since = "1.28", status = API.Status.EXPERIMENTAL)
  public boolean canPushRightFromAbove() {
    return (this == INNER) || (this == RIGHT);
  }

  /** Returns whether this join type accepts pushing predicates from within into its left input. */
  @API(since = "1.28", status = API.Status.EXPERIMENTAL)
  public boolean canPushLeftFromWithin() {
    return (this == INNER) || (this == RIGHT) || (this == SEMI);
  }

  /** Returns whether this join type accepts pushing predicates from within into its right input. */
  @API(since = "1.28", status = API.Status.EXPERIMENTAL)
  public boolean canPushRightFromWithin() {
    return (this == INNER) || (this == LEFT) || (this == SEMI);
  }
}
