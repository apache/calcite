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
package org.eigenbase.util;

import java.lang.reflect.Array;
import java.util.*;

import net.hydromatic.optiq.runtime.FlatLists;

/**
 * An immutable list of {@link Integer} values backed by an array of
 * {@code int}s.
 */
public class ImmutableIntList extends FlatLists.AbstractFlatList<Integer> {
    private final int[] ints;

    private static final Object[] EMPTY_ARRAY = new Object[0];

    private static final ImmutableIntList EMPTY = new EmptyImmutableIntList();

    // Does not copy array. Must remain private.
    private ImmutableIntList(int... ints) {
        this.ints = ints;
    }

    /** Returns an empty ImmutableIntList. */
    public static ImmutableIntList of() {
        return EMPTY;
    }

    /** Creates an ImmutableIntList from an array of {@code int}. */
    public static ImmutableIntList of(int... ints) {
        return new ImmutableIntList(ints.clone());
    }

    /** Creates an ImmutableIntList from an array of {@code Number}. */
    public static ImmutableIntList copyOf(Number... numbers) {
        final int[] ints = new int[numbers.length];
        for (int i = 0; i < ints.length; i++) {
          ints[i] = numbers[i].intValue();
        }
        return new ImmutableIntList(ints);
    }

    /** Creates an ImmutableIntList from a collection of {@link Number}. */
    public static ImmutableIntList copyOf(Collection<? extends Number> list) {
        if (list instanceof ImmutableIntList) {
            return (ImmutableIntList) list;
        }
        if (list.isEmpty()) {
            return EMPTY;
        }
        final int[] ints = new int[list.size()];
        int i = 0;
        for (Number number : list) {
            ints[i++] = number.intValue();
        }
        return new ImmutableIntList(ints);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(ints);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj
            || obj instanceof ImmutableIntList
            && Arrays.equals(ints, ((ImmutableIntList) obj).ints)
            || obj instanceof List
            && obj.equals(this);
    }

    @Override
    public String toString() {
        return Arrays.toString(ints);
    }

    @Override
    public boolean isEmpty() {
        return ints.length == 0;
    }

    public int size() {
        return ints.length;
    }

    public Object[] toArray() {
        final Object[] objects = new Object[ints.length];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = ints[i];
        }
        return objects;
    }

    public <T> T[] toArray(T[] a) {
        final int size = ints.length;
        if (a.length < size) {
            // Make a new array of a's runtime type, but my contents:
            a = a.getClass()  == Object[].class
                ? (T[]) new Object[size]
                : (T[]) Array.newInstance(
                    a.getClass().getComponentType(), size);
        }
        if (a.getClass() == Integer[].class) {
            final Integer[] integers = (Integer[]) a;
            for (int i = 0; i < integers.length; i++) {
                integers[i] = ints[i];
            }
        } else {
            System.arraycopy(toArray(), 0, a, 0, size);
        }
        if (a.length > size) {
            a[size] = null;
        }
        return a;
    }

    public Integer get(int index) {
        return ints[index];
    }

    public int indexOf(Object o) {
        if (o instanceof Integer) {
            final int seek = (Integer) o;
            for (int i = 0; i < ints.length; i++) {
                if (ints[i] == seek) {
                    return i;
                }
            }
        }
        return -1;
    }

    public int lastIndexOf(Object o) {
        if (o instanceof Integer) {
            final int seek = (Integer) o;
            for (int i = ints.length - 1; i >= 0; --i) {
                if (ints[i] == seek) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static class EmptyImmutableIntList extends ImmutableIntList {
        @Override public Object[] toArray() {
            return EMPTY_ARRAY;
        }

        @Override public <T> T[] toArray(T[] a) {
            if (a.length > 0) {
                a[0] = null;
            }
          return a;
        }
    }
}

// End ImmutableIntList.java
