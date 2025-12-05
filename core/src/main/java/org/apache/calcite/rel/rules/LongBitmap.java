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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

    private ArrayList<Long> subsetList;

    private int index;

    public SubsetIterator(long bitmap) {
      long curBiggestSubset = bitmap;
      this.subsetList = new ArrayList<>();

      while (curBiggestSubset != 0) {
        subsetList.add(curBiggestSubset);
        curBiggestSubset = (curBiggestSubset - 1) & bitmap;
      }

      this.index = subsetList.size() - 1;
    }

    @Override public Iterator<Long> iterator() {
      return new Iterator<Long>() {
        @Override public boolean hasNext() {
          return index >= 0;
        }

        @Override public Long next() {
          return subsetList.get(index--);
        }
      };
    }

    public void reset() {
      index = subsetList.size() - 1;
    }
  }

}
