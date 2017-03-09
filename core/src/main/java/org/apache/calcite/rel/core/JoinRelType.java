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

import java.util.Locale;

/**
 * Enumeration of join types.
 */
public enum JoinRelType {
  INNER, LEFT, RIGHT, FULL;

  /** Lower-case name. */
  public final String lowerName = name().toLowerCase(Locale.ROOT);

  /**
   * Returns whether a join of this type may generate NULL values on the
   * right-hand side.
   */
  public boolean generatesNullsOnRight() {
    return (this == LEFT) || (this == FULL);
  }

  /**
   * Returns whether a join of this type may generate NULL values on the
   * left-hand side.
   */
  public boolean generatesNullsOnLeft() {
    return (this == RIGHT) || (this == FULL);
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
}

// End JoinRelType.java
