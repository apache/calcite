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
package org.apache.calcite.runtime;

import com.google.common.collect.Ordering;

import java.util.HashMap;
import java.util.List;

/**
 * Utilities for space-filling curves.
 *
 * <p>Includes code from
 * <a href="https://github.com/locationtech/sfcurve">LocationTech SFCurve</a>,
 * Copyright (c) 2015 Azavea.
 */
public interface SpaceFillingCurve2D {
  long toIndex(double x, double y);
  Point toPoint(long i);
  List<IndexRange> toRanges(double xMin, double yMin, double xMax,
      double yMax, RangeComputeHints hints);

  /** Hints for the {@link SpaceFillingCurve2D#toRanges} method. */
  class RangeComputeHints extends HashMap<String, Object> {
  }

  /** Range. */
  interface IndexRange {
    long lower();
    long upper();
    boolean contained();

    IndexRangeTuple tuple();
  }

  /** Data representing a range. */
  class IndexRangeTuple {
    final long lower;
    final long upper;
    final boolean contained;

    IndexRangeTuple(long lower, long upper, boolean contained) {
      this.lower = lower;
      this.upper = upper;
      this.contained = contained;
    }
  }

  /** Base class for Range implementations. */
  abstract class AbstractRange implements IndexRange {
    final long lower;
    final long upper;

    protected AbstractRange(long lower, long upper) {
      this.lower = lower;
      this.upper = upper;
    }

    public long lower() {
      return lower;
    }

    public long upper() {
      return upper;
    }

    public IndexRangeTuple tuple() {
      return new IndexRangeTuple(lower, upper, contained());
    }
  }

  /** Range that is covered. */
  class CoveredRange extends AbstractRange {
    CoveredRange(long lower, long upper) {
      super(lower, upper);
    }

    public boolean contained() {
      return true;
    }

    @Override public String toString() {
      return "covered(" + lower + ", " + upper + ")";
    }
  }

  /** Range that is not contained. */
  class OverlappingRange extends AbstractRange {
    OverlappingRange(long lower, long upper) {
      super(lower, upper);
    }

    public boolean contained() {
      return false;
    }

    @Override public String toString() {
      return "overlap(" + lower + ", " + upper + ")";
    }
  }

  /** Lexicographic ordering for {@link IndexRange}. */
  class IndexRangeOrdering extends Ordering<IndexRange> {
    public int compare(IndexRange x, IndexRange y) {
      final int c1 = Long.compare(x.lower(), y.lower());
      if (c1 != 0) {
        return c1;
      }
      return Long.compare(x.upper(), y.upper());
    }
  }

  /** Utilities for {@link IndexRange}. */
  class IndexRanges {
    private IndexRanges() {}

    static IndexRange create(long l, long u, boolean contained) {
      return contained ? new CoveredRange(l, u) : new OverlappingRange(l, u);
    }
  }

  /** A 2-dimensional point. */
  class Point {
    final double x;
    final double y;

    Point(double x, double y) {
      this.x = x;
      this.y = y;
    }
  }
}
