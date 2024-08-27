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

import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * Specification of the window of rows over which a {@link RexOver} windowed
 * aggregate is evaluated.
 *
 * <p>Treat it as immutable!
 */
public class RexWindow {
  //~ Instance fields --------------------------------------------------------

  public final ImmutableList<RexNode> partitionKeys;
  public final ImmutableList<RexFieldCollation> orderKeys;
  private final RexWindowBound lowerBound;
  private final RexWindowBound upperBound;
  private final RexWindowExclusion exclude;
  private final boolean isRows;
  private final String digest;
  public final int nodeCount;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a window.
   *
   * <p>If you need to create a window from outside this package, use
   * {@link RexBuilder#makeOver}.
   *
   * <p>If {@code orderKeys} is empty the bracket will usually be
   * "BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING".
   *
   * <p>The digest assumes 'default' brackets, and does not print brackets or
   * bounds that are the default.
   *
   * <p>If {@code orderKeys} is empty, assumes the bracket is "RANGE BETWEEN
   * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" and does not print the
   * bracket.
   *
   *<li>If {@code orderKeys} is not empty, the default top is "CURRENT ROW".
   * The default bracket is "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
   * which will be printed as blank.
   * "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" is different, and is
   * printed as "ROWS UNBOUNDED PRECEDING".
   * "ROWS BETWEEN 5 PRECEDING AND CURRENT ROW" is printed as
   * "ROWS 5 PRECEDING".
   */
  @SuppressWarnings("method.invocation.invalid")
  RexWindow(
      List<RexNode> partitionKeys,
      List<RexFieldCollation> orderKeys,
      RexWindowBound lowerBound,
      RexWindowBound upperBound,
      boolean isRows,
      RexWindowExclusion exclude) {
    this.partitionKeys = ImmutableList.copyOf(partitionKeys);
    this.orderKeys = ImmutableList.copyOf(orderKeys);
    this.lowerBound = requireNonNull(lowerBound, "lowerBound");
    this.upperBound = requireNonNull(upperBound, "upperBound");
    this.exclude = exclude;
    this.isRows = isRows;
    this.nodeCount = computeCodeCount();
    this.digest = computeDigest();
    checkArgument(
        !(lowerBound.isUnboundedPreceding()
            && upperBound.isUnboundedFollowing()
            && isRows),
        "use RANGE for unbounded, not ROWS");
  }

  //~ Methods ----------------------------------------------------------------

  @Override public String toString() {
    return digest;
  }

  @Override public int hashCode() {
    return digest.hashCode();
  }

  @Override public boolean equals(@Nullable Object that) {
    if (that instanceof RexWindow) {
      RexWindow window = (RexWindow) that;
      return digest.equals(window.digest);
    }
    return false;
  }

  private String computeDigest() {
    return appendDigest_(new StringBuilder(), true).toString();
  }

  StringBuilder appendDigest(StringBuilder sb, boolean allowFraming) {
    if (allowFraming) {
      // digest was calculated with allowFraming=true; reuse it
      return sb.append(digest);
    } else {
      return appendDigest_(sb, allowFraming);
    }
  }

  private StringBuilder appendDigest_(StringBuilder sb, boolean allowFraming) {
    final int initialLength = sb.length();
    if (!partitionKeys.isEmpty()) {
      sb.append("PARTITION BY ");
      for (int i = 0; i < partitionKeys.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(partitionKeys.get(i));
      }
    }
    if (!orderKeys.isEmpty()) {
      sb.append(sb.length() > initialLength ? " ORDER BY " : "ORDER BY ");
      for (int i = 0; i < orderKeys.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(orderKeys.get(i));
      }
    }
    // There are 3 reasons to skip the ROWS/RANGE clause.
    // 1. If this window is being used with a RANK-style function that does not
    //    allow framing, or
    // 2. If it is RANGE without ORDER BY (in which case all frames yield all
    //    rows),
    // 3. If it is an unbounded range
    //    ("RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
    //    or "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING")
    //    with no ORDER BY.
    if (!allowFraming // 1
        || (!isRows && orderKeys.isEmpty()) // 2
        || (orderKeys.isEmpty()
            && lowerBound.isUnboundedPreceding() // 3
            && upperBound.isUnboundedFollowing())) {
      // Don't print a ROWS or RANGE clause
    } else if (upperBound.isCurrentRow()) {
      // Per MSSQL: If ROWS/RANGE is specified and <window frame preceding>
      // is used for <window frame extent> (short syntax) then this
      // specification is used for the window frame boundary starting point and
      // CURRENT ROW is used for the boundary ending point. For example
      // "ROWS 5 PRECEDING" is equal to "ROWS BETWEEN 5 PRECEDING AND CURRENT
      // ROW".
      //
      // We print the shorter option if it is
      // the default. If the RexWindow is, say, "ROWS BETWEEN 5 PRECEDING AND
      // CURRENT ROW", we output "ROWS 5 PRECEDING" because it is equivalent and
      // is shorter.
      if (!isRows && lowerBound.isUnboundedPreceding()) {
        // OVER (ORDER BY x)
        // OVER (ORDER BY x RANGE UNBOUNDED PRECEDING)
        // OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        // are equivalent, so print the first (i.e. nothing).
      } else {
        sb.append(sb.length() > initialLength ? " " : "")
            .append(isRows ? "ROWS" : "RANGE")
            .append(' ')
            .append(lowerBound);
      }
    } else {
      sb.append(sb.length() > initialLength ? " " : "")
          .append(isRows ? "ROWS" : "RANGE")
          .append(" BETWEEN ")
          .append(lowerBound)
          .append(" AND ")
          .append(upperBound);
    }
    if (exclude != RexWindowExclusion.EXCLUDE_NO_OTHER) {
      sb.append(" ").append(exclude).append(" ");
    }
    return sb;
  }

  public RexWindowBound getLowerBound() {
    return lowerBound;
  }

  public RexWindowBound getUpperBound() {
    return upperBound;
  }

  public RexWindowExclusion getExclude() {
    return exclude;
  }

  public boolean isRows() {
    return isRows;
  }

  private int computeCodeCount() {
    return RexUtil.nodeCount(partitionKeys)
        + RexUtil.nodeCount(Pair.left(orderKeys))
        + (lowerBound == null ? 0 : lowerBound.nodeCount())
        + (upperBound == null ? 0 : upperBound.nodeCount());
  }
}
