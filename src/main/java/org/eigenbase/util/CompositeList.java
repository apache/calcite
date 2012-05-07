/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.util;

import java.util.*;

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
 *
 * @author jhyde
 * @version $Id$
 * @since 31 October, 2009
 */
public class CompositeList<T> extends AbstractList<T>
{
    private final List<T>[] lists;

    /**
     * Creates a CompoundList.
     *
     * @param lists Constituent lists
     */
    public CompositeList(List<T>... lists)
    {
        this.lists = lists;
    }

    /**
     * Creates a CompoundList.
     *
     * <p>More convenient than {@link #CompositeList(java.util.List[])},
     * because element type is inferred. Use this method as you would
     * {@link java.util.Arrays#asList(Object[])} or
     * {@link java.util.EnumSet#of(Enum, Enum[])}.
     *
     * @param lists Consistituent lists
     * @param <T> Element type
     * @return List consisting of all lists
     */
    public static <T> CompositeList<T> of(List<T>... lists)
    {
        return new CompositeList<T>(lists);
    }

    public T get(int index)
    {
        for (List<T> list : lists) {
            int nextIndex = index - list.size();
            if (nextIndex < 0) {
                return list.get(index);
            }
            index = nextIndex;
        }
        throw new IndexOutOfBoundsException();
    }

    public int size()
    {
        int n = 0;
        for (List<T> list : lists) {
            n += list.size();
        }
        return n;
    }
}

// End CompositeList.java
