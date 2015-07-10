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
package org.apache.calcite.rel;

import org.apache.calcite.sql.validate.SqlMonotonicity;

/**
 * Definition of the ordering of one field of a {@link RelNode} whose
 * output is to be sorted.
 *
 * @see RelCollation
 */
public class RelFieldCollation {
  /** Utility method that compares values taking into account null
   * direction. */
  public static int compare(Comparable c1, Comparable c2, int nullComparison) {
    if (c1 == c2) {
      return 0;
    } else if (c1 == null) {
      return nullComparison;
    } else if (c2 == null) {
      return -nullComparison;
    } else {
      //noinspection unchecked
      return c1.compareTo(c2);
    }
  }

  //~ Enums ------------------------------------------------------------------

  /**
   * Direction that a field is ordered in.
   */
  public enum Direction {
    /**
     * Ascending direction: A value is always followed by a greater or equal
     * value.
     */
    ASCENDING("ASC"),

    /**
     * Strictly ascending direction: A value is always followed by a greater
     * value.
     */
    STRICTLY_ASCENDING("SASC"),

    /**
     * Descending direction: A value is always followed by a lesser or equal
     * value.
     */
    DESCENDING("DESC"),

    /**
     * Strictly descending direction: A value is always followed by a lesser
     * value.
     */
    STRICTLY_DESCENDING("SDESC"),

    /**
     * Clustered direction: Values occur in no particular order, and the
     * same value may occur in contiguous groups, but never occurs after
     * that. This sort order tends to occur when values are ordered
     * according to a hash-key.
     */
    CLUSTERED("CLU");

    public final String shortString;

    Direction(String shortString) {
      this.shortString = shortString;
    }

    /** Converts thie direction to a
     * {@link org.apache.calcite.sql.validate.SqlMonotonicity}. */
    public SqlMonotonicity monotonicity() {
      switch (this) {
      case ASCENDING:
        return SqlMonotonicity.INCREASING;
      case STRICTLY_ASCENDING:
        return SqlMonotonicity.STRICTLY_INCREASING;
      case DESCENDING:
        return SqlMonotonicity.DECREASING;
      case STRICTLY_DESCENDING:
        return SqlMonotonicity.STRICTLY_DECREASING;
      case CLUSTERED:
        return SqlMonotonicity.MONOTONIC;
      default:
        throw new AssertionError("unknown: " + this);
      }
    }

    /** Converts a {@link SqlMonotonicity} to a direction. */
    public static Direction of(SqlMonotonicity monotonicity) {
      switch (monotonicity) {
      case INCREASING:
        return ASCENDING;
      case DECREASING:
        return DESCENDING;
      case STRICTLY_INCREASING:
        return STRICTLY_ASCENDING;
      case STRICTLY_DECREASING:
        return STRICTLY_DESCENDING;
      case MONOTONIC:
        return CLUSTERED;
      default:
        throw new AssertionError("unknown: " + monotonicity);
      }
    }
  }

  /**
   * Ordering of nulls.
   */
  public enum NullDirection {
    FIRST(-1),
    LAST(1),
    UNSPECIFIED(1);

    public final int nullComparison;

    NullDirection(int nullComparison) {
      this.nullComparison = nullComparison;
    }
  }

  //~ Instance fields --------------------------------------------------------

  /**
   * 0-based index of field being sorted.
   */
  private final int fieldIndex;

  /**
   * Direction of sorting.
   */
  public final Direction direction;

  /**
   * Direction of sorting of nulls.
   */
  public final NullDirection nullDirection;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an ascending field collation.
   */
  public RelFieldCollation(int fieldIndex) {
    this(fieldIndex, Direction.ASCENDING, NullDirection.UNSPECIFIED);
  }

  /**
   * Creates a field collation with unspecified null direction.
   */
  public RelFieldCollation(int fieldIndex, Direction direction) {
    this(fieldIndex, direction, NullDirection.UNSPECIFIED);
  }

  /**
   * Creates a field collation.
   */
  public RelFieldCollation(
      int fieldIndex,
      Direction direction,
      NullDirection nullDirection) {
    this.fieldIndex = fieldIndex;
    this.direction = direction;
    this.nullDirection = nullDirection;
    assert direction != null;
    assert nullDirection != null;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a copy of this RelFieldCollation against a different field.
   */
  public RelFieldCollation copy(int target) {
    if (target == fieldIndex) {
      return this;
    }
    return new RelFieldCollation(target, direction, nullDirection);
  }

  /**
   * Returns a copy of this RelFieldCollation with the field index shifted
   * {@code offset} to the right.
   */
  public RelFieldCollation shift(int offset) {
    return copy(fieldIndex + offset);
  }

  // implement Object
  public boolean equals(Object obj) {
    if (!(obj instanceof RelFieldCollation)) {
      return false;
    }
    RelFieldCollation other = (RelFieldCollation) obj;
    return (fieldIndex == other.fieldIndex)
        && (direction == other.direction)
        && (nullDirection == other.nullDirection);
  }

  // implement Object
  public int hashCode() {
    return this.fieldIndex
        | (this.direction.ordinal() << 4)
        | (this.nullDirection.ordinal() << 8);
  }

  public int getFieldIndex() {
    return fieldIndex;
  }

  public RelFieldCollation.Direction getDirection() {
    return direction;
  }

  public String toString() {
    return fieldIndex
        + " " + direction.shortString
        + (nullDirection == NullDirection.UNSPECIFIED
        ? ""
        : " " + nullDirection);
  }

  public String shortString() {
    switch (nullDirection) {
    case FIRST:
      return direction.shortString + "-nulls-first";
    case LAST:
      return direction.shortString + "-nulls-last";
    default:
      return direction.shortString;
    }
  }
}

// End RelFieldCollation.java
