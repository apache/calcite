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

import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Functions;

import java.util.Comparator;

/**
 * Binary search for the implementation of
 * RANGE BETWEEN XXX PRECEDING/FOLLOWING clause.
 *
 */
public class BinarySearch {
  // Even though this is a utility class (all methods are static), we cannot
  // make the constructor private. Because Janino doesn't do static import,
  // generated code is placed in sub-classes.
  protected BinarySearch() {
  }

  /**
   * Performs binary search of the lower bound in the given array.
   * It is assumed that the array is sorted.
   * The method is guaranteed to return the minimal index of the element that is
   * greater or equal to the given key.
   * @param a array that holds the values
   * @param key element to look for
   * @param comparator comparator that compares keys
   * @param <T> the type of elements in array
   * @return minimal index of the element that is
   *   greater or equal to the given key. Returns -1 when all elements exceed
   *   the given key or the array is empty. Returns {@code a.length} when all
   *   elements are less than the given key.
   */
  public static <T> int lowerBound(T[] a, T key, Comparator<T> comparator) {
    return lowerBound(a, key, 0, a.length - 1,
        Functions.identitySelector(), comparator);
  }

  /**
   * Performs binary search of the upper bound in the given array.
   * It is assumed that the array is sorted.
   * The method is guaranteed to return the maximal index of the element that is
   * less or equal to the given key.
   * @param a array that holds the values
   * @param key element to look for
   * @param comparator comparator that compares keys
   * @param <T> the type of elements in array
   * @return maximal index of the element that is
   *   less or equal to the given key. Returns -1 when all elements are less
   *   than the given key or the array is empty. Returns {@code a.length} when
   *   all elements exceed the given key.
   */
  public static <T> int upperBound(T[] a, T key, Comparator<T> comparator) {
    return upperBound(a, key, 0, a.length - 1,
        Functions.identitySelector(), comparator);
  }

  /**
   * Performs binary search of the lower bound in the given array.
   * The elements in the array are transformed before the comparison.
   * It is assumed that the array is sorted.
   * The method is guaranteed to return the minimal index of the element that is
   * greater or equal to the given key.
   * @param a array that holds the values
   * @param key element to look for
   * @param keySelector function that transforms array contents to the type
   *                    of the key
   * @param comparator comparator that compares keys
   * @param <T> the type of elements in array
   * @param <K> the type of lookup key
   * @return minimal index of the element that is
   *   greater or equal to the given key. Returns -1 when all elements exceed
   *   the given key or the array is empty. Returns {@code a.length} when all
   *   elements are less than the given key.
   */
  public static <T, K> int lowerBound(T[] a, K key, Function1<T, K> keySelector,
      Comparator<K> comparator) {
    return lowerBound(a, key, 0, a.length - 1, keySelector, comparator);
  }

  /**
   * Performs binary search of the upper bound in the given array.
   * The elements in the array are transformed before the comparison.
   * It is assumed that the array is sorted.
   * The method is guaranteed to return the maximal index of the element that is
   * less or equal to the given key.
   * @param a array that holds the values
   * @param key element to look for
   * @param keySelector function that transforms array contents to the type
   *                    of the key
   * @param comparator comparator that compares keys
   * @param <T> the type of elements in array
   * @param <K> the type of lookup key
   * @return maximal index of the element that is
   *   less or equal to the given key. Returns -1 when all elements are less
   *   than the given key or the array is empty. Returns {@code a.length} when
   *   all elements exceed the given key.
   */
  public static <T, K> int upperBound(T[] a, K key, Function1<T, K> keySelector,
      Comparator<K> comparator) {
    return upperBound(a, key, 0, a.length - 1, keySelector, comparator);
  }

  /**
   * Performs binary search of the lower bound in the given section of array.
   * It is assumed that the array is sorted.
   * The method is guaranteed to return the minimal index of the element that is
   * greater or equal to the given key.
   * @param a array that holds the values
   * @param key element to look for
   * @param imin the minimal index (inclusive) to look for
   * @param imax the maximum index (inclusive) to look for
   * @param comparator comparator that compares keys
   * @param <T> the type of elements in array
   * @return minimal index of the element that is
   *   greater or equal to the given key. Returns -1 when all elements exceed
   *   the given key or the array is empty. Returns {@code a.length} when all
   *   elements are less than the given key.
   */
  public static <T> int lowerBound(T[] a, T key, int imin, int imax,
      Comparator<T> comparator) {
    return lowerBound(a, key, imin, imax,
        Functions.identitySelector(), comparator);
  }

  /**
   * Performs binary search of the upper bound in the given array.
   * It is assumed that the array is sorted.
   * The method is guaranteed to return the maximal index of the element that is
   * less or equal to the given key.
   * @param a array that holds the values
   * @param key element to look for
   * @param imin the minimal index (inclusive) to look for
   * @param imax the maximum index (inclusive) to look for
   * @param comparator comparator that compares keys
   * @param <T> the type of elements in array
   * @return maximal index of the element that is
   *   less or equal to the given key. Returns -1 when all elements are less
   *   than the given key or the array is empty. Returns {@code a.length} when
   *   all elements exceed the given key.
   */
  public static <T> int upperBound(T[] a, T key, int imin, int imax,
      Comparator<T> comparator) {
    return upperBound(a, key, imin, imax,
        Functions.identitySelector(), comparator);
  }

  /**
   * Taken from http://en.wikipedia.org/wiki/Binary_search_algorithm
   * #Deferred_detection_of_equality
   */
  public static <T, K> int lowerBound(T[] a, K key, int imin, int imax,
      Function1<T, K> keySelector, Comparator<K> comparator) {
    // continually narrow search until just one element remains
    while (imin < imax) {
      // http://bugs.java.com/bugdatabase/view_bug.do?bug_id=5045582
      int imid = (imin + imax) >>> 1;

      // code must guarantee the interval is reduced at each iteration
      assert imid < imax
          : "search interval should be reduced min=" + imin
          + ", mid=" + imid + ", max=" + imax;
      // note: 0 <= imin < imax implies imid will always be less than imax

      // reduce the search
      if (comparator.compare(keySelector.apply(a[imid]), key) < 0) {
        // change min index to search upper subarray
        imin = imid + 1;
      } else {
        imax = imid;
      }
    }
    // At exit of while:
    //   if a[] is empty, then imax < imin
    //   otherwise imax == imin

    // deferred test for equality
    if (imax != imin) {
      return -1;
    }

    int cmp = comparator.compare(keySelector.apply(a[imin]), key);
    if (cmp == 0) {
      // Detected exact match, just return it
      return imin;
    }
    if (cmp < 0) {
      // We were asked the key that is greater that all the values in array
      return imin + 1;
    }
    // If imin != 0 we return imin since a[imin-1] < key < a[imin]
    // If imin == 0 we return -1 since the resulting window might be empty
    // For instance, range between 100 preceding and 99 preceding
    // Use if-else to ensure code coverage is reported for each return
    if (imin == 0) {
      return -1;
    } else {
      return imin;
    }
  }

  /**
   * Taken from http://en.wikipedia.org/wiki/Binary_search_algorithm
   * #Deferred_detection_of_equality
   * Adapted to find upper bound.
   */
  public static <T, K> int upperBound(T[] a, K key, int imin, int imax,
      Function1<T, K> keySelector, Comparator<K> comparator) {
    int initialMax = imax;
    // continually narrow search until just one element remains
    while (imin < imax) {
      int imid = (imin + imax + 1) >>> 1;

      // code must guarantee the interval is reduced at each iteration
      assert imid > imin
          : "search interval should be reduced min=" + imin
          + ", mid=" + imid + ", max=" + imax;
      // note: 0 <= imin < imax implies imid will always be less than imax

      // reduce the search
      if (comparator.compare(keySelector.apply(a[imid]), key) > 0) {
        // change max index to search lower subarray
        imax = imid - 1;
      } else {
        imin = imid;
      }
    }
    // At exit of while:
    //   if a[] is empty, then imax < imin
    //   otherwise imax == imin

    // deferred test for equality
    if (imax != imin) {
      return -1;
    }

    int cmp = comparator.compare(keySelector.apply(a[imin]), key);
    if (cmp == 0) {
      // Detected exact match, just return it
      return imin;
    }
    if (cmp > 0) {
      // We were asked the key that is less than all the values in array
      return imin - 1;
    }
    // If imin != initialMax we return imin since a[imin-1] < key < a[imin]
    // If imin == initialMax we return initialMax+11 since
    // the resulting window might be empty
    // For instance, range between 99 following and 100 following
    // Use if-else to ensure code coverage is reported for each return
    if (imin == initialMax) {
      return initialMax + 1;
    } else {
      return imin;
    }
  }
}

// End BinarySearch.java
