/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.runtime;

import java.util.Comparator;

/**
 * Comparator that uses the natural ordering of [@link Comparable} values.
 */
public final class ComparableComparator<T extends Comparable>
    implements Comparator<T>
{
  private static final ComparableComparator INSTANCE =
      new ComparableComparator();

  @SuppressWarnings("unchecked")
  public static <T extends Comparable> ComparableComparator<T> instance() {
    return INSTANCE;
  }

  @SuppressWarnings("unchecked")
  public int compare(T o1, T o2) {
    return o1.compareTo(o2);
  }
}

// End ComparableComparator.java
