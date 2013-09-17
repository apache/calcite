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

import java.util.*;

/**
 * Extension to {@link ArrayList} to help build an array of <code>int</code>
 * values.
 *
 * @author jhyde
 * @version $Id$
 */
public class IntList
    extends ArrayList<Integer>
{
    //~ Methods ----------------------------------------------------------------

    public int [] toIntArray()
    {
        return toArray(this);
    }

    /**
     * Converts a list of {@link Integer} objects to an array of primitive
     * <code>int</code>s.
     *
     * @param integers List of Integer objects
     *
     * @return Array of primitive <code>int</code>s
     */
    public static int [] toArray(List<Integer> integers)
    {
        final int [] ints = new int[integers.size()];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = integers.get(i);
        }
        return ints;
    }

    /**
     * Returns a list backed by an array of primitive <code>int</code> values.
     *
     * <p>The behavior is analogous to {@link Arrays#asList(Object[])}. Changes
     * to the list are reflected in the array. The list cannot be extended.
     *
     * @param args Array of primitive <code>int</code> values
     *
     * @return List backed by array
     */
    public static List<Integer> asList(final int [] args)
    {
        return new AbstractList<Integer>() {
            public Integer get(int index)
            {
                return args[index];
            }

            public int size()
            {
                return args.length;
            }

            public Integer set(int index, Integer element)
            {
                return args[index] = element;
            }
        };
    }

    public ImmutableIntList asImmutable() {
        return ImmutableIntList.copyOf(this);
    }
}

// End IntList.java
