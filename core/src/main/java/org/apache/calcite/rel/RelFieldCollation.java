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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * Definition of the ordering of one field of a {@link RelNode} whose
 * output is to be sorted.
 *
 * @see RelCollation
 */
public class RelFieldCollation {
  /** Utility method that compares values taking into account null
   * direction. */
  public static int compare(@Nullable Comparable c1, @Nullable Comparable c2, int nullComparison) {
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

    /** Converts the direction to a
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

    /** Returns the null direction if not specified. Consistent with Oracle,
     * NULLS are sorted as if they were positive infinity. */
    public NullDirection defaultNullDirection() {
      switch (this) {
      case ASCENDING:
      case STRICTLY_ASCENDING:
        return NullDirection.LAST;
      case DESCENDING:
      case STRICTLY_DESCENDING:
        return NullDirection.FIRST;
      default:
        return NullDirection.UNSPECIFIED;
      }
    }

    /** Returns whether this is {@link #DESCENDING} or
     * {@link #STRICTLY_DESCENDING}. */
    public boolean isDescending() {
      switch (this) {
      case DESCENDING:
      case STRICTLY_DESCENDING:
        return true;
      default:
        return false;
      }
    }

    /**
     * Returns the reverse of this direction.
     *
     * @return reverse of the input direction
     */
    public Direction reverse() {
      switch (this) {
      case ASCENDING:
        return DESCENDING;
      case STRICTLY_ASCENDING:
        return STRICTLY_DESCENDING;
      case DESCENDING:
        return ASCENDING;
      case STRICTLY_DESCENDING:
        return STRICTLY_ASCENDING;
      default:
        return this;
      }
    }

    /** Removes strictness. */
    public Direction lax() {
      switch (this) {
      case STRICTLY_ASCENDING:
        return ASCENDING;
      case STRICTLY_DESCENDING:
        return DESCENDING;
      default:
        return this;
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
    this(fieldIndex, Direction.ASCENDING);
  }

  /**
   * Creates a field collation with unspecified null direction.
   */
  public RelFieldCollation(int fieldIndex, Direction direction) {
    this(fieldIndex, direction, direction.defaultNullDirection());
  }

  /**
   * Creates a field collation.
   */
  public RelFieldCollation(
      int fieldIndex,
      Direction direction,
      NullDirection nullDirection) {
    this.fieldIndex = fieldIndex;
    this.direction = Objects.requireNonNull(direction, "direction");
    this.nullDirection = Objects.requireNonNull(nullDirection, "nullDirection");
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a copy of this RelFieldCollation against a different field.
   */
  public RelFieldCollation withFieldIndex(int fieldIndex) {
    return this.fieldIndex == fieldIndex ? this
        : new RelFieldCollation(fieldIndex, direction, nullDirection);
  }

  @Deprecated // to be removed before 2.0
  public RelFieldCollation copy(int target) {
    return withFieldIndex(target);
  }

  /** Creates a copy of this RelFieldCollation with a different direction. */
  public RelFieldCollation withDirection(Direction direction) {
    return this.direction == direction ? this
        : new RelFieldCollation(fieldIndex, direction, nullDirection);
  }

  /** Creates a copy of this RelFieldCollation with a different null
   * direction. */
  public RelFieldCollation withNullDirection(NullDirection nullDirection) {
    return this.nullDirection == nullDirection ? this
        : new RelFieldCollation(fieldIndex, direction, nullDirection);
  }

  /**
   * Returns a copy of this RelFieldCollation with the field index shifted
   * {@code offset} to the right.
   */
  public RelFieldCollation shift(int offset) {
    return withFieldIndex(fieldIndex + offset);
  }

  @Override public boolean equals(@Nullable Object o) {
    return this == o
        || o instanceof RelFieldCollation
        && fieldIndex == ((RelFieldCollation) o).fieldIndex
        && direction == ((RelFieldCollation) o).direction
        && nullDirection == ((RelFieldCollation) o).nullDirection;
  }

  @Override public int hashCode() {
    return Objects.hash(fieldIndex, direction, nullDirection);
  }

  public int getFieldIndex() {
    return fieldIndex;
  }

  public RelFieldCollation.Direction getDirection() {
    return direction;
  }

  @Override public String toString() {
    if (direction == Direction.ASCENDING
        && nullDirection == direction.defaultNullDirection()) {
      return String.valueOf(fieldIndex);
    }
    final StringBuilder sb = new StringBuilder();
    sb.append(fieldIndex).append(" ").append(direction.shortString);
    if (nullDirection != direction.defaultNullDirection()) {
      sb.append(" ").append(nullDirection);
    }
    return sb.toString();
  }

  public String shortString() {
    if (nullDirection == direction.defaultNullDirection()) {
      return direction.shortString;
    }
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
