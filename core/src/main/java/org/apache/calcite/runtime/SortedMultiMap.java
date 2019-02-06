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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Map that allows you to partition values into lists according to a common
 * key, and then convert those lists into an iterator of sorted arrays.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class SortedMultiMap<K, V> extends HashMap<K, List<V>> {
  public void putMulti(K key, V value) {
    List<V> list = put(key, Collections.singletonList(value));
    if (list == null) {
      return;
    }
    if (list.size() == 1) {
      list = new ArrayList<>(list);
    }
    list.add(value);
    put(key, list);
  }

  public Iterator<V[]> arrays(final Comparator<V> comparator) {
    final Iterator<List<V>> iterator = values().iterator();
    return new Iterator<V[]>() {
      public boolean hasNext() {
        return iterator.hasNext();
      }

      public V[] next() {
        List<V> list = iterator.next();
        @SuppressWarnings("unchecked")
        final V[] vs = (V[]) list.toArray();
        Arrays.sort(vs, comparator);
        return vs;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /** Shortcut method if the partition key is empty. We know that we would end
   * up with a map with just one entry, so save ourselves the trouble of all
   * that hashing. */
  public static <V> Iterator<V[]> singletonArrayIterator(
      Comparator<V> comparator, List<V> list) {
    final SortedMultiMap<Object, V> multiMap =
        new SortedMultiMap<>();
    multiMap.put("x", list);
    return multiMap.arrays(comparator);
  }
}

// End SortedMultiMap.java
