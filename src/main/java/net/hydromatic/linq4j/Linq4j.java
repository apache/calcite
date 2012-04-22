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
package net.hydromatic.linq4j;

import java.util.*;

/**
 * Utility and factory methods for Linq4j.
 */
public class Linq4j {
    private static final Object DUMMY = new Object();

    /**
     * Adapter that converts an enumerator into an iterator.
     *
     * @param enumerator Enumerator
     * @param <T> Element type
     * @return Iterator
     */
    public static <T> Iterator<T> enumeratorIterator(
        final Enumerator<T> enumerator)
    {
        return new Iterator<T>() {
            boolean hasNext = enumerator.moveNext();

            public boolean hasNext() {
                return hasNext;
            }

            public T next() {
                T t = enumerator.current();
                hasNext = enumerator.moveNext();
                return t;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Adapter that converts an iterable into an enumerator.
     *
     * @param iterable Iterable
     * @param <T> Element type
     * @return enumerator
     */
    public static <T> Enumerator<T> iterableEnumerator(
        final Iterable<T> iterable)
    {
        return new IterableEnumerator<T>(iterable);
    }

    /**
     * Adapter that converts an iterable into an enumerable.
     *
     * @param iterable Iterable
     * @param <T> Element type
     * @return enumerable
     */
    public static <T> Enumerable<T> asEnumerable(final Iterable<T> iterable) {
        return new AbstractEnumerable<T>() {
            public Enumerator<T> enumerator() {
                return iterableEnumerator(iterable);
            }
        };
    }

    /**
     * Adapter that converts an array into an enumerable.
     *
     * @param ts Array
     * @param <T> Element type
     * @return enumerable
     */
    public static <T> Enumerable<T> asEnumerable(final T[] ts) {
        return asEnumerable(Arrays.asList(ts));
    }

    /**
     * Adapter that converts a collection into an enumerator.
     *
     * @param values Collection
     * @param <V> Element type
     * @return Enumerator over the collection
     */
    public static <V> Enumerator<V> enumerator(Collection<V> values) {
        return iterableEnumerator(values);
    }

    /**
     * Converts the elements of a given Iterable to the specified type.
     *
     * <p>This method is implemented by using deferred execution. The immediate
     * return value is an object that stores all the information that is
     * required to perform the action. The query represented by this method is
     * not executed until the object is enumerated either by calling its
     * {@link Enumerable#enumerator} method directly or by using
     * {@code for (... in ...)}.
     *
     * <p>Since standard Java {@link Collection} objects implement the
     * {@link Iterable} interface, the {@code cast} method enables the standard
     * query operators to be invoked on collections
     * (including {@link java.util.List} and {@link java.util.Set}) by supplying
     * the necessary type information. For example, {@link ArrayList} does not
     * implement {@link Enumerable}&lt;T&gt;, but you can invoke
     *
     * <blockquote><code>Linq4j.cast(list, Integer.class)</code></blockquote>
     *
     * to convert the list of an enumerable that can be queried using the
     * standard query operators.
     *
     * <p>If an element cannot be cast to type &lt;TResult&gt;, this method will
     * throw a {@link ClassCastException}. To obtain only those elements that
     * can be cast to type TResult, use the {@link #ofType} method instead.
     *
     * @see Enumerable#cast(Class)
     * @see #ofType
     * @see #asEnumerable(Iterable)
     */
    public static <TSource, TResult> Enumerable<TResult> cast(
        Iterable<TSource> source,
        Class<TResult> clazz)
    {
        return asEnumerable(source).cast(clazz);
    }

    /**
     * Returns elements of a given {@link Iterable} that are of the specified
     * type.
     *
     * <p>This method is implemented by using deferred execution. The immediate
     * return value is an object that stores all the information that is
     * required to perform the action. The query represented by this method is
     * not executed until the object is enumerated either by calling its
     * {@link Enumerable#enumerator} method directly or by using
     * {@code for (... in ...)}.
     *
     * <p>The {@code ofType} method returns only those elements in source that
     * can be cast to type TResult. To instead receive an exception if an
     * element cannot be cast to type TResult, use
     * {@link #cast(Iterable, Class)}.</p>
     *
     * <p>Since standard Java {@link Collection} objects implement the
     * {@link Iterable} interface, the {@code cast} method enables the standard
     * query operators to be invoked on collections
     * (including {@link java.util.List} and {@link java.util.Set}) by supplying
     * the necessary type information. For example, {@link ArrayList} does not
     * implement {@link Enumerable}&lt;T&gt;, but you can invoke
     *
     * <blockquote><code>Linq4j.ofType(list, Integer.class)</code></blockquote>
     *
     * to convert the list of an enumerable that can be queried using the
     * standard query operators.
     *
     * @see Enumerable#cast(Class)
     * @see #cast
     */
    public static <TSource, TResult> Enumerable<TResult> ofType(
        Iterable<TSource> source,
        Class<TResult> clazz)
    {
        return asEnumerable(source).ofType(clazz);
    }

    @SuppressWarnings("unchecked")
    private static class IterableEnumerator<T> implements Enumerator<T> {
        private final Iterable<T> iterable;
        Iterator<T> iterator;
        T current;

        public IterableEnumerator(Iterable<T> iterable) {
            this.iterable = iterable;
            iterator = iterable.iterator();
            current = (T) DUMMY;
        }

        public T current() {
            if (current == DUMMY) {
                throw new NoSuchElementException();
            }
            return current;
        }

        public boolean moveNext() {
            if (iterator.hasNext()) {
                current = iterator.next();
                return true;
            }
            current = (T) DUMMY;
            return false;
        }

        public void reset() {
            iterator = iterable.iterator();
            current = (T) DUMMY;
        }
    }
}

// End Linq4j.java
