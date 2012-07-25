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

    /** Query provider that simply executes a {@link Queryable} by calling its
     * enumerator method; does not attempt optimization. */
    public static final QueryProvider DEFAULT_PROVIDER =
        new QueryProviderImpl() {
            public <T> Enumerator<T> executeQuery(Queryable<T> queryable)
            {
                return queryable.enumerator();
            }
        };

    private static final Enumerator<Object> EMPTY_ENUMERATOR =
        new Enumerator<Object>() {
            public Object current() {
                throw new NoSuchElementException();
            }

            public boolean moveNext() {
                return false;
            }

            public void reset() {
            }
        };

    public static final Enumerable<?> EMPTY_ENUMERABLE =
        new AbstractEnumerable<Object>() {
            public Enumerator<Object> enumerator() {
                return EMPTY_ENUMERATOR;
            }
        };

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
     * Adapter that converts an {@link List} into an {@link Enumerable}.
     *
     * @param list List
     * @param <T> Element type
     * @return enumerable
     */
    public static <T> Enumerable<T> asEnumerable(final List<T> list) {
        return new ListEnumerable<T>(list);
    }

    /**
     * Adapter that converts an {@link Collection} into an {@link Enumerable}.
     *
     * <p>It uses more efficient implementations if the iterable happens to
     * be a {@link List}.</p>
     *
     * @param collection Collection
     * @param <T> Element type
     * @return enumerable
     */
    public static <T> Enumerable<T> asEnumerable(final Collection<T> collection)
    {
        if (collection instanceof List) {
            //noinspection unchecked
            return asEnumerable((List) collection);
        }
        return new CollectionEnumerable<T>(collection);
    }

    /**
     * Adapter that converts an {@link Iterable} into an {@link Enumerable}.
     *
     * <p>It uses more efficient implementations if the iterable happens to
     * be a {@link Collection} or a {@link List}.</p>
     *
     * @param iterable Iterable
     * @param <T> Element type
     * @return enumerable
     */
    public static <T> Enumerable<T> asEnumerable(final Iterable<T> iterable) {
        if (iterable instanceof Collection) {
            //noinspection unchecked
            return asEnumerable((Collection) iterable);
        }
        return new IterableEnumerable<T>(iterable);
    }

    /**
     * Adapter that converts an array into an enumerable.
     *
     * @param ts Array
     * @param <T> Element type
     * @return enumerable
     */
    public static <T> Enumerable<T> asEnumerable(final T[] ts) {
        return new ListEnumerable<T>(Arrays.asList(ts));
    }

    public static String asEnumerable2(final Object[] ts) {
        return asEnumerable(ts).toString();
    }

    public static Enumerable asEnumerable3(final Object[] ts) {
        return asEnumerable(ts);
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

    /**
     * Returns an {@link Enumerable} that has one element.
     *
     * @param <T> Element type
     * @return Singleton enumerable
     */
    public static <T> Enumerable<T> singletonEnumerable(T element) {
        return asEnumerable(Collections.singletonList(element));
    }

    /**
     * Returns an {@link Enumerable} that has no elements.
     *
     * @param <T> Element type
     * @return Empty enumerable
     */
    public static <T> Enumerable<T> emptyEnumerable() {
        //noinspection unchecked
        return (Enumerable<T>) EMPTY_ENUMERABLE;
    }

    /**
     * Returns an {@link Enumerator} that has no elements.
     *
     * @param <T> Element type
     * @return Empty enumerator
     */
    public static <T> Enumerator<T> emptyEnumerator() {
        //noinspection unchecked
        return (Enumerator<T>) EMPTY_ENUMERATOR;
    }

    /**
     * Concatenates two or more {@link Enumerable}s to form a composite
     * enumerable that contains the union of their elements.
     *
     * @param enumerableList List of enumerable objects
     * @param <E> Element type
     * @return Composite enumerator
     */
    public static <E> Enumerable<E> concat(
        final List<Enumerable<E>> enumerableList)
    {
        return new CompositeEnumerable<E>(enumerableList);
    }

    /**
     * Returns an enumerator that is the cartesian product of the given
     * enumerators.
     *
     * <p>For example, given enumerator A that returns {"a", "b", "c"} and
     * enumerator B that returns {"x", "y"}, product(List(A, B)) will return
     * {List("a", "x"), List("a", "y"),
     * List("b", "x"), List("b", "y"),
     * List("c", "x"), List("c", "y")}.</p>
     *
     * <p>Notice that the cardinality of the result is the product of the
     * cardinality of the inputs. The enumerators A and B have 3 and 2
     * elements respectively, and the result has 3 * 2 = 6 elements.
     * This is always the case. In
     * particular, if any of the enumerators is empty, the result is empty.</p>
     *
     * @param enumerators List of enumerators
     * @param <T> Element type
     * @return Enumerator over the cartesian product
     */
    public static <T> Enumerator<List<T>> product(
        List<Enumerator<T>> enumerators)
    {
        return new CartesianProductEnumerator<T>(enumerators);
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

    static class CompositeEnumerable<E> extends AbstractEnumerable<E> {
        private final Enumerator<Enumerable<E>> enumerableEnumerator;

        CompositeEnumerable(List<Enumerable<E>> enumerableList) {
            enumerableEnumerator = iterableEnumerator(enumerableList);
        }

        public Enumerator<E> enumerator() {
            return new Enumerator<E>() {
                Enumerator<E> current = emptyEnumerator();

                public E current() {
                    return current.current();
                }

                public boolean moveNext() {
                    for (;;) {
                        if (current.moveNext()) {
                            return true;
                        }
                        if (!enumerableEnumerator.moveNext()) {
                            return false;
                        }
                        current = enumerableEnumerator.current().enumerator();
                    }
                }

                public void reset() {
                    enumerableEnumerator.reset();
                    current = emptyEnumerator();
                }
            };
        }
    }

    static class IterableEnumerable<T> extends AbstractEnumerable2<T> {
        protected final Iterable<T> iterable;

        IterableEnumerable(Iterable<T> iterable) {
            this.iterable = iterable;
        }

        public Iterator<T> iterator() {
            return iterable.iterator();
        }

        @Override
        public boolean any() {
            return iterable.iterator().hasNext();
        }
    }

    static class CollectionEnumerable<T> extends IterableEnumerable<T> {
        CollectionEnumerable(Collection<T> iterable) {
            super(iterable);
        }

        protected Collection<T> getCollection() {
            return (Collection<T>) iterable;
        }

        @Override
        public int count() {
            return getCollection().size();
        }

        @Override
        public long longCount() {
            return getCollection().size();
        }

        @Override
        public boolean contains(T element) {
            return getCollection().contains(element);
        }

        @Override
        public boolean any() {
            return !getCollection().isEmpty();
        }
    }

    static class ListEnumerable<T> extends CollectionEnumerable<T> {
        ListEnumerable(Collection<T> iterable) {
            super(iterable);
        }

        @Override
        public List<T> toList() {
            return (List<T>) iterable;
        }

        @Override
        public Enumerable<T> skip(int count) {
            final List<T> list = toList();
            return new ListEnumerable<T>(list.subList(count, list.size()));
        }

        @Override
        public Enumerable<T> take(int count) {
            final List<T> list = toList();
            return new ListEnumerable<T>(list.subList(0, count));
        }

        @Override
        public T elementAt(int index) {
            return toList().get(index);
        }
    }
}

// End Linq4j.java
