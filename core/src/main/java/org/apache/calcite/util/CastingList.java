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

import java.util.AbstractList;
import java.util.List;

/**
 * Converts a list whose members are automatically down-cast to a given type.
 *
 * <p>If a member of the backing list is not an instanceof <code>E</code>, the
 * accessing method (such as {@link List#get}) will throw a
 * {@link ClassCastException}.
 *
 * <p>All modifications are automatically written to the backing list. Not
 * synchronized.
 *
 * @param <E> Element type
 */
public class CastingList<E> extends AbstractList<E> implements List<E> {
  //~ Instance fields --------------------------------------------------------

  private final List<? super E> list;
  private final Class<E> clazz;

  //~ Constructors -----------------------------------------------------------

  protected CastingList(List<? super E> list, Class<E> clazz) {
    super();
    this.list = list;
    this.clazz = clazz;
  }

  //~ Methods ----------------------------------------------------------------

  public E get(int index) {
    return clazz.cast(list.get(index));
  }

  public int size() {
    return list.size();
  }

  public E set(int index, E element) {
    final Object o = list.set(index, element);
    return clazz.cast(o);
  }

  public E remove(int index) {
    return clazz.cast(list.remove(index));
  }

  public void add(int pos, E o) {
    list.add(pos, o);
  }
}

// End CastingList.java
