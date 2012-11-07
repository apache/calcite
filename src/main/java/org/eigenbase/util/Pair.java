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
 * Pair of objects.
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 17, 2007
 */
public class Pair<T1, T2> implements Map.Entry<T1, T2>
{
    //~ Instance fields --------------------------------------------------------

    public final T1 left;
    public final T2 right;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a Pair.
     *
     * @param left left value
     * @param right right value
     */
    public Pair(T1 left, T2 right)
    {
        this.left = left;
        this.right = right;
    }

    /**
     * Creates a Pair of appropriate type.
     *
     * <p>This is a shorthand that allows you to omit implicit types. For
     * example, you can write:
     * <blockquote>return Pair.of(s, n);</blockquote>
     * instead of
     * <blockquote>return new Pair&lt;String, Integer&gt;(s, n);</blockquote>
     *
     * @param left left value
     * @param right right value
     * @return A Pair
     */
    public static <T1, T2> Pair<T1, T2> of(T1 left, T2 right)
    {
        return new Pair<T1, T2>(left, right);
    }

    //~ Methods ----------------------------------------------------------------

    public boolean equals(Object obj)
    {
        return (obj instanceof Pair)
            && Util.equal(this.left, ((Pair) obj).left)
            && Util.equal(this.right, ((Pair) obj).right);
    }

    public int hashCode()
    {
        int h1 = Util.hash(0, left);
        return Util.hash(h1, right);
    }

    public T1 getKey() {
        return left;
    }

    public T2 getValue() {
        return right;
    }

    public T2 setValue(T2 value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Converts a collection of Pairs into a Map.
     *
     * <p>This is an obvious thing to do because Pair is similar in structure to
     * {@link java.util.Map.Entry}.
     *
     * <p>The map contains a copy of the collection of Pairs; if you change the
     * collection, the map does not change.
     *
     * @param pairs Collection of Pair objects
     *
     * @return map with the same contents as the collection
     */
    public static <K, V> Map<K, V> toMap(Collection<Pair<K, V>> pairs)
    {
        HashMap<K, V> map = new HashMap<K, V>();
        for (Pair<K, V> pair : pairs) {
            map.put(pair.left, pair.right);
        }
        return map;
    }

    /**
     * Converts two lists into a list of {@link Pair}s.
     *
     * <p>The length of the combined list is the lesser of the lengths of the
     * source lists. But typically the source lists will be the same length.</p>
     *
     * @param ks Left list
     * @param vs Right list
     * @return List of pairs
     */
    public static <K, V> List<Pair<K, V>> zip(
        final List<K> ks,
        final List<V> vs)
    {
        return new AbstractList<Pair<K, V>>() {
            public Pair<K, V> get(int index) {
                return Pair.of(ks.get(index), vs.get(index));
            }

            public int size() {
                return Math.min(ks.size(), vs.size());
            }
        };
    }
}

// End Pair.java
