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
package org.apache.calcite.util;

import com.google.common.collect.ImmutableList;

import java.util.AbstractList;
import java.util.List;

/**
 * Read-only list that is the concatenation of sub-lists.
 *
 * <p>The list is read-only; attempts to call methods such as
 * {@link #add(Object)} or {@link #set(int, Object)} will throw.
 *
 * <p>Changes to the backing lists, including changes in length, will be
 * reflected in this list.
 *
 * <p>This class is not thread-safe. Changes to backing lists will cause
 * unspecified behavior.
 *
 * @param <T> Element type
 */
public class CompositeList<T> extends AbstractList<T> {
  private final ImmutableList<List<T>> lists;

  /**
   * Creates a CompositeList.
   *
   * @param lists Constituent lists
   */
  private CompositeList(ImmutableList<List<T>> lists) {
    this.lists = lists;
  }

  /**
   * Creates a CompositeList.
   *
   * @param lists Constituent lists
   * @param <T>   Element type
   * @return List consisting of all lists
   */
  @SafeVarargs
  public static <T> CompositeList<T> of(List<? extends T>... lists) {
    //noinspection unchecked
    return new CompositeList<T>((ImmutableList) ImmutableList.copyOf(lists));
  }

  /**
   * Creates a CompositeList.
   *
   * @param lists Constituent lists
   * @param <T>   Element type
   * @return List consisting of all lists
   */
  public static <T> CompositeList<T> ofCopy(Iterable<List<T>> lists) {
    final ImmutableList<List<T>> list = ImmutableList.copyOf(lists);
    return new CompositeList<>(list);
  }

  /**
   * Creates a CompositeList of zero lists.
   *
   * @param <T>   Element type
   * @return List consisting of all lists
   */
  public static <T> List<T> of() {
    return ImmutableList.of();
  }

  /**
   * Creates a CompositeList of one list.
   *
   * @param list0 List
   * @param <T>   Element type
   * @return List consisting of all lists
   */
  public static <T> List<T> of(List<T> list0) {
    return list0;
  }

  /**
   * Creates a CompositeList of two lists.
   *
   * @param list0 First list
   * @param list1 Second list
   * @param <T>   Element type
   * @return List consisting of all lists
   */
  public static <T> CompositeList<T> of(List<? extends T> list0,
      List<? extends T> list1) {
    //noinspection unchecked
    return new CompositeList<T>((ImmutableList) ImmutableList.of(list0, list1));
  }

  /**
   * Creates a CompositeList of three lists.
   *
   * @param list0 First list
   * @param list1 Second list
   * @param list2 Third list
   * @param <T>   Element type
   * @return List consisting of all lists
   */
  public static <T> CompositeList<T> of(List<? extends T> list0,
      List<? extends T> list1,
      List<? extends T> list2) {
    //noinspection unchecked
    return new CompositeList<T>((ImmutableList) ImmutableList.of(list0, list1, list2));
  }

  public T get(int index) {
    for (List<? extends T> list : lists) {
      int nextIndex = index - list.size();
      if (nextIndex < 0) {
        return list.get(index);
      }
      index = nextIndex;
    }
    throw new IndexOutOfBoundsException();
  }

  public int size() {
    int n = 0;
    for (List<? extends T> list : lists) {
      n += list.size();
    }
    return n;
  }
}

// End CompositeList.java
