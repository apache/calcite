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
package org.apache.calcite.linq4j;

import org.apache.calcite.linq4j.function.Function1;

import com.google.common.collect.Lists;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.RandomAccess;

/**
 * Utility and factory methods for Linq4j.
 */
public abstract class Linq4j {
  private Linq4j() {}

  private static final Object DUMMY = new Object();

  public static Method getMethod(String className, String methodName,
      Class... parameterTypes) {
    try {
      return Class.forName(className).getMethod(methodName, parameterTypes);
    } catch (NoSuchMethodException e) {
      return null;
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  /**
   * Query provider that simply executes a {@link Queryable} by calling its
   * enumerator method; does not attempt optimization.
   */
  public static final QueryProvider DEFAULT_PROVIDER = new QueryProviderImpl() {
    public <T> Enumerator<T> executeQuery(Queryable<T> queryable) {
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

        public void close() {
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
   * <p><b>WARNING</b>: The iterator returned by this method does not call
   * {@link org.apache.calcite.linq4j.Enumerator#close()}, so it is not safe to
   * use with an enumerator that allocates resources.</p>
   *
   * @param enumerator Enumerator
   * @param <T> Element type
   *
   * @return Iterator
   */
  public static <T> Iterator<T> enumeratorIterator(Enumerator<T> enumerator) {
    return new EnumeratorIterator<>(enumerator);
  }

  /**
   * Adapter that converts an iterable into an enumerator.
   *
   * @param iterable Iterable
   * @param <T> Element type
   *
   * @return enumerator
   */
  public static <T> Enumerator<T> iterableEnumerator(
      final Iterable<? extends T> iterable) {
    if (iterable instanceof Enumerable) {
      @SuppressWarnings("unchecked") final Enumerable<T> enumerable =
          (Enumerable) iterable;
      return enumerable.enumerator();
    }
    return new IterableEnumerator<>(iterable);
  }

  /**
   * Adapter that converts an {@link List} into an {@link Enumerable}.
   *
   * @param list List
   * @param <T> Element type
   *
   * @return enumerable
   */
  public static <T> Enumerable<T> asEnumerable(final List<T> list) {
    return new ListEnumerable<>(list);
  }

  /**
   * Adapter that converts an {@link Collection} into an {@link Enumerable}.
   *
   * <p>It uses more efficient implementations if the iterable happens to
   * be a {@link List}.</p>
   *
   * @param collection Collection
   * @param <T> Element type
   *
   * @return enumerable
   */
  public static <T> Enumerable<T> asEnumerable(final Collection<T> collection) {
    if (collection instanceof List) {
      //noinspection unchecked
      return asEnumerable((List) collection);
    }
    return new CollectionEnumerable<>(collection);
  }

  /**
   * Adapter that converts an {@link Iterable} into an {@link Enumerable}.
   *
   * <p>It uses more efficient implementations if the iterable happens to
   * be a {@link Collection} or a {@link List}.</p>
   *
   * @param iterable Iterable
   * @param <T> Element type
   *
   * @return enumerable
   */
  public static <T> Enumerable<T> asEnumerable(final Iterable<T> iterable) {
    if (iterable instanceof Collection) {
      //noinspection unchecked
      return asEnumerable((Collection) iterable);
    }
    return new IterableEnumerable<>(iterable);
  }

  /**
   * Adapter that converts an array into an enumerable.
   *
   * @param ts Array
   * @param <T> Element type
   *
   * @return enumerable
   */
  public static <T> Enumerable<T> asEnumerable(final T[] ts) {
    return new ListEnumerable<>(Arrays.asList(ts));
  }

  /**
   * Adapter that converts a collection into an enumerator.
   *
   * @param values Collection
   * @param <V> Element type
   *
   * @return Enumerator over the collection
   */
  public static <V> Enumerator<V> enumerator(Collection<? extends V> values) {
    if (values instanceof List && values instanceof RandomAccess) {
      //noinspection unchecked
      return listEnumerator((List) values);
    }
    return iterableEnumerator(values);
  }

  private static <V> Enumerator<V> listEnumerator(List<? extends V> list) {
    return new ListEnumerator<>(list);
  }

  /** Applies a function to each element of an Enumerator.
   *
   * @param enumerator Backing enumerator
   * @param func Transform function
   * @param <F> Backing element type
   * @param <E> Element type
   * @return Enumerator
   */
  public static <F, E> Enumerator<E> transform(Enumerator<F> enumerator,
      final Function1<F, E> func) {
    return new TransformedEnumerator<F, E>(enumerator) {
      protected E transform(F from) {
        return func.apply(from);
      }
    };
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
   * implement {@link Enumerable}&lt;F&gt;, but you can invoke
   *
   * <blockquote><code>Linq4j.cast(list, Integer.class)</code></blockquote>
   *
   * <p>to convert the list of an enumerable that can be queried using the
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
      Iterable<TSource> source, Class<TResult> clazz) {
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
   * implement {@link Enumerable}&lt;F&gt;, but you can invoke
   *
   * <blockquote><code>Linq4j.ofType(list, Integer.class)</code></blockquote>
   *
   * <p>to convert the list of an enumerable that can be queried using the
   * standard query operators.
   *
   * @see Enumerable#cast(Class)
   * @see #cast
   */
  public static <TSource, TResult> Enumerable<TResult> ofType(
      Iterable<TSource> source, Class<TResult> clazz) {
    return asEnumerable(source).ofType(clazz);
  }

  /**
   * Returns an {@link Enumerable} that has one element.
   *
   * @param <T> Element type
   *
   * @return Singleton enumerable
   */
  public static <T> Enumerable<T> singletonEnumerable(final T element) {
    return new AbstractEnumerable<T>() {
      public Enumerator<T> enumerator() {
        return singletonEnumerator(element);
      }
    };
  }

  /**
   * Returns an {@link Enumerator} that has one element.
   *
   * @param <T> Element type
   *
   * @return Singleton enumerator
   */
  public static <T> Enumerator<T> singletonEnumerator(T element) {
    return new SingletonEnumerator<>(element);
  }

  /**
   * Returns an {@link Enumerator} that has one null element.
   *
   * @param <T> Element type
   *
   * @return Singleton enumerator
   */
  public static <T> Enumerator<T> singletonNullEnumerator() {
    return new SingletonNullEnumerator<>();
  }

  /**
   * Returns an {@link Enumerable} that has no elements.
   *
   * @param <T> Element type
   *
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
   *
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
   *
   * @return Composite enumerator
   */
  public static <E> Enumerable<E> concat(
      final List<Enumerable<E>> enumerableList) {
    return new CompositeEnumerable<>(enumerableList);
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
   *
   * @return Enumerator over the cartesian product
   */
  public static <T> Enumerator<List<T>> product(
      List<Enumerator<T>> enumerators) {
    return new CartesianProductListEnumerator<>(enumerators);
  }

  /** Returns the cartesian product of an iterable of iterables. */
  public static <T> Iterable<List<T>> product(
      final Iterable<? extends Iterable<T>> iterables) {
    return new Iterable<List<T>>() {
      public Iterator<List<T>> iterator() {
        final List<Enumerator<T>> enumerators = Lists.newArrayList();
        for (Iterable<T> iterable : iterables) {
          enumerators.add(iterableEnumerator(iterable));
        }
        return enumeratorIterator(
            new CartesianProductListEnumerator<>(enumerators));
      }
    };
  }

  /**
   * Returns whether the arguments are equal to each other.
   *
   * <p>Equivalent to {@link java.util.Objects#equals} in JDK 1.7 and above.
   */
  @Deprecated // to be removed before 2.0
  public static <T> boolean equals(T t0, T t1) {
    return t0 == t1 || t0 != null && t0.equals(t1);
  }

  /**
   * Throws {@link NullPointerException} if argument is null, otherwise
   * returns argument.
   *
   * <p>Equivalent to {@link java.util.Objects#requireNonNull} in JDK 1.7 and
   * above.
   */
  @Deprecated // to be removed before 2.0
  public static <T> T requireNonNull(T o) {
    if (o == null) {
      throw new NullPointerException();
    }
    return o;
  }

  /** Closes an iterator, if it can be closed. */
  private static <T> void closeIterator(Iterator<T> iterator) {
    if (iterator instanceof AutoCloseable) {
      try {
        ((AutoCloseable) iterator).close();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Iterable enumerator.
   *
   * @param <T> element type */
  @SuppressWarnings("unchecked")
  static class IterableEnumerator<T> implements Enumerator<T> {
    private final Iterable<? extends T> iterable;
    Iterator<? extends T> iterator;
    T current;

    IterableEnumerator(Iterable<? extends T> iterable) {
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

    public void close() {
      final Iterator<? extends T> iterator1 = this.iterator;
      this.iterator = null;
      closeIterator(iterator1);
    }
  }

  /** Composite enumerable.
   *
   * @param <E> element type */
  static class CompositeEnumerable<E> extends AbstractEnumerable<E> {
    private final Enumerator<Enumerable<E>> enumerableEnumerator;

    CompositeEnumerable(List<Enumerable<E>> enumerableList) {
      enumerableEnumerator = iterableEnumerator(enumerableList);
    }

    public Enumerator<E> enumerator() {
      return new Enumerator<E>() {
        // Never null.
        Enumerator<E> current = emptyEnumerator();

        public E current() {
          return current.current();
        }

        public boolean moveNext() {
          for (;;) {
            if (current.moveNext()) {
              return true;
            }
            current.close();
            if (!enumerableEnumerator.moveNext()) {
              current = emptyEnumerator();
              return false;
            }
            current = enumerableEnumerator.current().enumerator();
          }
        }

        public void reset() {
          enumerableEnumerator.reset();
          current = emptyEnumerator();
        }

        public void close() {
          current.close();
          current = emptyEnumerator();
        }
      };
    }
  }

  /** Iterable enumerable.
   *
   * @param <T> element type */
  static class IterableEnumerable<T> extends AbstractEnumerable2<T> {
    protected final Iterable<T> iterable;

    IterableEnumerable(Iterable<T> iterable) {
      this.iterable = iterable;
    }

    public Iterator<T> iterator() {
      return iterable.iterator();
    }

    @Override public boolean any() {
      return iterable.iterator().hasNext();
    }
  }

  /** Collection enumerable.
   *
   * @param <T> element type */
  static class CollectionEnumerable<T> extends IterableEnumerable<T> {
    CollectionEnumerable(Collection<T> iterable) {
      super(iterable);
    }

    protected Collection<T> getCollection() {
      return (Collection<T>) iterable;
    }

    @Override public int count() {
      return getCollection().size();
    }

    @Override public long longCount() {
      return getCollection().size();
    }

    @Override public boolean contains(T element) {
      return getCollection().contains(element);
    }

    @Override public boolean any() {
      return !getCollection().isEmpty();
    }
  }

  /** List enumerable.
   *
   * @param <T> element type */
  static class ListEnumerable<T> extends CollectionEnumerable<T> {
    ListEnumerable(List<T> list) {
      super(list);
    }

    @Override public Enumerator<T> enumerator() {
      if (iterable instanceof RandomAccess) {
        //noinspection unchecked
        return new ListEnumerator<>((List) iterable);
      }
      return super.enumerator();
    }

    @Override public List<T> toList() {
      return (List<T>) iterable;
    }

    @Override public Enumerable<T> skip(int count) {
      final List<T> list = toList();
      if (count >= list.size()) {
        return Linq4j.emptyEnumerable();
      }
      return new ListEnumerable<>(list.subList(count, list.size()));
    }

    @Override public Enumerable<T> take(int count) {
      final List<T> list = toList();
      if (count >= list.size()) {
        return this;
      }
      return new ListEnumerable<>(list.subList(0, count));
    }

    @Override public T elementAt(int index) {
      return toList().get(index);
    }
  }

  /** Enumerator that returns one element.
   *
   * @param <E> element type */
  private static class SingletonEnumerator<E> implements Enumerator<E> {
    final E e;
    int i = 0;

    SingletonEnumerator(E e) {
      this.e = e;
    }

    public E current() {
      return e;
    }

    public boolean moveNext() {
      return i++ == 0;
    }

    public void reset() {
      i = 0;
    }

    public void close() {
    }
  }

  /** Enumerator that returns one null element.
   *
   * @param <E> element type */
  private static class SingletonNullEnumerator<E> implements Enumerator<E> {
    int i = 0;

    public E current() {
      return null;
    }

    public boolean moveNext() {
      return i++ == 0;
    }

    public void reset() {
      i = 0;
    }

    public void close() {
    }
  }

  /** Iterator that reads from an underlying {@link Enumerator}.
   *
   * @param <T> element type */
  private static class EnumeratorIterator<T>
      implements Iterator<T>, AutoCloseable {
    private final Enumerator<T> enumerator;
    boolean hasNext;

    EnumeratorIterator(Enumerator<T> enumerator) {
      this.enumerator = enumerator;
      hasNext = enumerator.moveNext();
    }

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

    public void close() {
      enumerator.close();
    }
  }

  /** Enumerator optimized for random-access list.
   *
   * @param <V> element type */
  private static class ListEnumerator<V> implements Enumerator<V> {
    private final List<? extends V> list;
    int i = -1;

    ListEnumerator(List<? extends V> list) {
      this.list = list;
    }

    public V current() {
      return list.get(i);
    }

    public boolean moveNext() {
      return ++i < list.size();
    }

    public void reset() {
      i = -1;
    }

    public void close() {
    }
  }

  /** Enumerates over the cartesian product of the given lists, returning
   * a list for each row.
   *
   * @param <E> element type */
  private static class CartesianProductListEnumerator<E>
      extends CartesianProductEnumerator<E, List<E>> {
    CartesianProductListEnumerator(List<Enumerator<E>> enumerators) {
      super(enumerators);
    }

    public List<E> current() {
      return Arrays.asList(elements.clone());
    }
  }
}

// End Linq4j.java
