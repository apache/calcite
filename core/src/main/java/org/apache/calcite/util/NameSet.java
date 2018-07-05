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
import com.google.common.collect.ImmutableSortedSet;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/** Set of names that can be accessed with and without case sensitivity. */
public class NameSet {
  /** Comparator that compares all strings differently, but if two strings are
   * equal in case-insensitive match they are right next to each other. In a
   * collection sorted on this comparator, we can find case-insensitive matches
   * for a given string using a range scan between the upper-case string and
   * the lower-case string. */
  public static final Comparator<String> COMPARATOR = (o1, o2) -> {
    int c = o1.compareToIgnoreCase(o2);
    if (c == 0) {
      c = o1.compareTo(o2);
    }
    return c;
  };

  private final NavigableSet<String> names;

  /** Creates a NameSet based on an existing set. */
  private NameSet(NavigableSet<String> names) {
    this.names = names;
    assert names.comparator() == COMPARATOR;
  }

  /** Creates a NameSet, initially empty. */
  public NameSet() {
    this(new TreeSet<>(COMPARATOR));
  }

  /** Creates a NameSet that is an immutable copy of a given collection. */
  public static NameSet immutableCopyOf(Set<String> names) {
    return new NameSet(ImmutableSortedSet.copyOf(NameSet.COMPARATOR, names));
  }

  public void add(String name) {
    names.add(name);
  }

  /** Returns an iterable over all the entries in the set that match the given
   * name. If case-sensitive, that iterable will have 0 or 1 elements; if
   * case-insensitive, it may have 0 or more. */
  public Collection<String> range(String name, boolean caseSensitive) {
    if (caseSensitive) {
      if (names.contains(name)) {
        return ImmutableList.of(name);
      } else {
        return ImmutableList.of();
      }
    } else {
      return names.subSet(name.toUpperCase(Locale.ROOT), true,
          name.toLowerCase(Locale.ROOT), true);
    }
  }

  /** Returns whether this set contains the given name, with a given
   * case-sensitivity. */
  public boolean contains(String name, boolean caseSensitive) {
    if (names.contains(name)) {
      return true;
    }
    if (!caseSensitive) {
      final String s = names.ceiling(name.toLowerCase(Locale.ROOT));
      return s != null
          && s.equalsIgnoreCase(name);
    }
    return false;
  }

  /** Returns the contents as an iterable. */
  public Iterable<String> iterable() {
    return Collections.unmodifiableSet(names);
  }
}

// End NameSet.java
