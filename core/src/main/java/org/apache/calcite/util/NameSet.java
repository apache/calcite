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

import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

/** Set of names that can be accessed with and without case sensitivity. */
public class NameSet {
  public static final Comparator<String> COMPARATOR = CaseInsensitiveComparator.COMPARATOR;

  private static final Object DUMMY = new Object();

  private final NameMap<Object> names;

  /** Creates a NameSet based on an existing set. */
  private NameSet(NameMap<Object> names) {
    this.names = names;
  }

  /** Creates a NameSet, initially empty. */
  public NameSet() {
    this(new NameMap<>());
  }

  /** Creates a NameSet that is an immutable copy of a given collection. */
  public static NameSet immutableCopyOf(Set<String> names) {
    return new NameSet(NameMap.immutableCopyOf(Maps.asMap(names, (k) -> DUMMY)));
  }

  @Override public String toString() {
    return names.map().keySet().toString();
  }

  @Override public int hashCode() {
    return names.hashCode();
  }

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof NameSet
        && names.equals(((NameSet) obj).names);
  }

  public void add(String name) {
    names.put(name, DUMMY);
  }

  /** Returns an iterable over all the entries in the set that match the given
   * name. If case-sensitive, that iterable will have 0 or 1 elements; if
   * case-insensitive, it may have 0 or more. */
  public Collection<String> range(String name, boolean caseSensitive) {
    return names.range(name, caseSensitive).keySet();
  }

  /** Returns whether this set contains the given name, with a given
   * case-sensitivity. */
  public boolean contains(String name, boolean caseSensitive) {
    return names.containsKey(name, caseSensitive);
  }

  /** Returns the contents as an iterable. */
  public Iterable<String> iterable() {
    return Collections.unmodifiableSet(names.map().keySet());
  }
}

// End NameSet.java
