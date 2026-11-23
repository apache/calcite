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
package org.apache.calcite.rel.rules;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Bitmap tool for dphyp.
 */
public class LongBitmap {

  private LongBitmap() {}

  public static long newBitmapBetween(int startInclude, int endExclude) {
    long bitmap = 0;
    for (int i = startInclude; i < endExclude; i++) {
      bitmap |= 1L << i;
    }
    return bitmap;
  }

  public static long newBitmap(int value) {
    return 1L << value;
  }

  /**
   * Corresponding to Bv = {node|node ≺ csg} in "Dynamic programming strikes back".
   */
  public static long getBvBitmap(long csg) {
    return (csg & -csg) - 1;
  }

  public static boolean isSubSet(long maySub, long bigger) {
    return (bigger | maySub) == bigger;
  }

  public static boolean isOverlap(long bitmap1, long bitmap2) {
    return (bitmap1 & bitmap2) != 0;
  }

  public static long newBitmapFromList(List<Integer> values) {
    long bitmap = 0;
    for (int value : values) {
      bitmap |= 1L << value;
    }
    return bitmap;
  }

  public static String printBitmap(long bitmap) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    while (bitmap != 0) {
      sb.append(Long.numberOfTrailingZeros(bitmap)).append(", ");
      bitmap = bitmap & (bitmap - 1);
    }
    sb.delete(sb.length() - 2, sb.length());
    sb.append("}");
    return sb.toString();
  }

  /**
   * Traverse the bitmap in reverse order.
   */
  public static class ReverseIterator implements Iterable<Long> {

    private long bitmap;

    public ReverseIterator(long bitmap) {
      this.bitmap = bitmap;
    }

    @Override public Iterator<Long> iterator() {
      return new Iterator<Long>() {
        @Override public boolean hasNext() {
          return bitmap != 0;
        }

        @Override public Long next() {
          long res = Long.highestOneBit(bitmap);
          bitmap &= ~res;
          return res;
        }
      };
    }
  }

  /**
   * Enumerate all subsets of a bitmap from small to large.
   */
  public static class SubsetIterator implements Iterable<Long> {

    private final long bitmap;

    private long current;

    public SubsetIterator(long bitmap) {
      this.bitmap = bitmap;
    }

    @Override public Iterator<Long> iterator() {
      return new Iterator<Long>() {
        @Override public boolean hasNext() {
          return current != bitmap;
        }

        @Override public Long next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          // Subset enumeration of Vance and Maier ("Rapid Bushy Join-order
          // Optimization with Cartesian Products", SIGMOD 1996), which the
          // DPhyp paper uses ("Dynamic Programming Strikes Back", SIGMOD 2008).
          // The set bits of bitmap act as the digits of a binary counter, and
          // this adds one to it. Since current has no bit in common with
          // ~bitmap, current - bitmap equals (current | ~bitmap) + 1: the
          // positions outside bitmap are set to 1, so the carry moves past
          // them, and the mask clears them again. For example, with bitmap
          // 0b1101 and current 0b0101 (low 4 bits shown):
          //   current | ~bitmap   0111  (bit 1 is outside bitmap, set to 1)
          //   + 1                 1000  (the carry moves past bit 1)
          //   & bitmap            1000  (next subset)
          current = (current - bitmap) & bitmap;
          return current;
        }
      };
    }

    public void reset() {
      current = 0;
    }
  }

}
