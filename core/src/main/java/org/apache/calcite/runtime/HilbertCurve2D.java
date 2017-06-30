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

import com.google.common.collect.ImmutableList;
import com.google.uzaygezen.core.BacktrackingQueryBuilder;
import com.google.uzaygezen.core.BitVector;
import com.google.uzaygezen.core.BitVectorFactories;
import com.google.uzaygezen.core.CompactHilbertCurve;
import com.google.uzaygezen.core.FilteredIndexRange;
import com.google.uzaygezen.core.LongContent;
import com.google.uzaygezen.core.PlainFilterCombiner;
import com.google.uzaygezen.core.Query;
import com.google.uzaygezen.core.SimpleRegionInspector;
import com.google.uzaygezen.core.ZoomingSpaceVisitorAdapter;
import com.google.uzaygezen.core.ranges.LongRange;
import com.google.uzaygezen.core.ranges.LongRangeHome;

import java.util.ArrayList;
import java.util.List;

/**
 * 2-dimensional Hilbert space-filling curve.
 *
 * <p>Includes code from
 * <a href="https://github.com/locationtech/sfcurve">LocationTech SFCurve</a>,
 * Copyright (c) 2015 Azavea.
 */
public class HilbertCurve2D implements SpaceFillingCurve2D {
  final long precision;
  final CompactHilbertCurve chc;
  private final int resolution;

  public HilbertCurve2D(int resolution) {
    this.resolution = resolution;
    precision = (long) Math.pow(2, resolution);
    chc = new CompactHilbertCurve(new int[] {resolution, resolution});
  }

  long getNormalizedLongitude(double x) {
    return (long) ((x + 180) * (precision - 1) / 360d);
  }

  long getNormalizedLatitude(double y) {
    return (long) ((y + 90) * (precision - 1) / 180d);
  }

  long setNormalizedLatitude(long latNormal) {
    if (!(latNormal >= 0 && latNormal <= precision)) {
      throw new NumberFormatException(
          "Normalized latitude must be greater than 0 and less than the maximum precision");
    }
    return (long) (latNormal * 180d / (precision - 1));
  }

  long setNormalizedLongitude(long lonNormal) {
    if (!(lonNormal >= 0 && lonNormal <= precision)) {
      throw new NumberFormatException(
          "Normalized longitude must be greater than 0 and less than the maximum precision");
    }
    return (long) (lonNormal * 360d / (precision - 1));
  }

  public long toIndex(double x, double y) {
    final long normX = getNormalizedLongitude(x);
    final long normY = getNormalizedLatitude(y);
    final BitVector[] p = {
        BitVectorFactories.OPTIMAL.apply(resolution),
        BitVectorFactories.OPTIMAL.apply(resolution)
    };

    p[0].copyFrom(normX);
    p[1].copyFrom(normY);

    final BitVector hilbert = BitVectorFactories.OPTIMAL.apply(resolution * 2);

    chc.index(p, 0, hilbert);
    return hilbert.toLong();
  }

  public Point toPoint(long i) {
    final BitVector h = BitVectorFactories.OPTIMAL.apply(resolution * 2);
    h.copyFrom(i);
    final BitVector[] p = {
        BitVectorFactories.OPTIMAL.apply(resolution),
        BitVectorFactories.OPTIMAL.apply(resolution)
    };

    chc.indexInverse(h, p);

    final long x = setNormalizedLongitude(p[0].toLong()) - 180;
    final long y = setNormalizedLatitude(p[1].toLong()) - 90;
    return new Point((double) x, (double) y);
  }

  public List<IndexRange> toRanges(double xMin, double yMin, double xMax,
      double yMax, RangeComputeHints hints) {
    final CompactHilbertCurve chc =
        new CompactHilbertCurve(new int[] {resolution, resolution});
    final List<LongRange> region = new ArrayList<>();

    final long minNormalizedLongitude = getNormalizedLongitude(xMin);
    final long minNormalizedLatitude  = getNormalizedLatitude(yMin);

    final long maxNormalizedLongitude = getNormalizedLongitude(xMax);
    final long maxNormalizedLatitude  = getNormalizedLatitude(yMax);

    region.add(LongRange.of(minNormalizedLongitude, maxNormalizedLongitude));
    region.add(LongRange.of(minNormalizedLatitude, maxNormalizedLatitude));

    final LongContent zero = new LongContent(0L);

    final SimpleRegionInspector<LongRange, Long, LongContent, LongRange> inspector =
        SimpleRegionInspector.create(ImmutableList.of(region),
            new LongContent(1L), range -> range, LongRangeHome.INSTANCE,
            zero);

    final PlainFilterCombiner<LongRange, Long, LongContent, LongRange> combiner =
        new PlainFilterCombiner<>(LongRange.of(0, 1));

    final BacktrackingQueryBuilder<LongRange, Long, LongContent, LongRange> queryBuilder =
        BacktrackingQueryBuilder.create(inspector, combiner, Integer.MAX_VALUE,
            true, LongRangeHome.INSTANCE, zero);

    chc.accept(new ZoomingSpaceVisitorAdapter(chc, queryBuilder));

    final Query<LongRange, LongRange> query = queryBuilder.get();

    final List<FilteredIndexRange<LongRange, LongRange>> ranges =
        query.getFilteredIndexRanges();

    // result
    final List<IndexRange> result = new ArrayList<>();

    for (FilteredIndexRange<LongRange, LongRange> l : ranges) {
      final LongRange range = l.getIndexRange();
      final Long start = range.getStart();
      final Long end = range.getEnd();
      final boolean contained = l.isPotentialOverSelectivity();
      result.add(0, IndexRanges.create(start, end, contained));
    }
    return result;
  }
}
