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
package net.hydromatic.linq4j.function;

import net.hydromatic.linq4j.Linq4j;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;

/**
 * Utilities relating to functions.
 */
public abstract class Functions {
  private Functions() {}

  public static final Map<Class<? extends Function>, Class>
  FUNCTION_RESULT_TYPES =
      Collections.<Class<? extends Function>, Class>unmodifiableMap(map(
          Function0.class, Object.class,
          Function1.class, Object.class,
          Function2.class, Object.class,
          BigDecimalFunction1.class, BigDecimal.class,
          DoubleFunction1.class, Double.TYPE,
          FloatFunction1.class, Float.TYPE,
          IntegerFunction1.class, Integer.TYPE,
          LongFunction1.class, Long.TYPE,
          NullableBigDecimalFunction1.class, BigDecimal.class,
          NullableDoubleFunction1.class, Double.class,
          NullableFloatFunction1.class, Float.class,
          NullableIntegerFunction1.class, Integer.class,
          NullableLongFunction1.class, Long.class));

  private static final Map<Class, Class<? extends Function>> FUNCTION1_CLASSES =
      Collections.unmodifiableMap(
          new HashMap<Class, Class<? extends Function>>(
              inverse(FUNCTION_RESULT_TYPES)));

  private static final Comparator NULLS_FIRST_COMPARATOR =
      new NullsFirstComparator();

  private static final Comparator NULLS_LAST_COMPARATOR =
      new NullsLastComparator();

  private static final Comparator NULLS_LAST_REVERSE_COMPARATOR =
      new NullsLastReverseComparator();

  private static final Comparator NULLS_FIRST_REVERSE_COMPARATOR =
      new NullsFirstReverseComparator();

  private static final EqualityComparer<Object> IDENTITY_COMPARER =
      new IdentityEqualityComparer();

  private static final EqualityComparer<Object[]> ARRAY_COMPARER =
      new ArrayEqualityComparer();

  private static final Function1 CONSTANT_NULL_FUNCTION1 =
      new Function1() {
        public Object apply(Object s) {
          return null;
        }
      };

  @SuppressWarnings("unchecked")
  private static <K, V> Map<K, V> map(K k, V v, Object... rest) {
    final Map<K, V> map = new HashMap<K, V>();
    map.put(k, v);
    for (int i = 0; i < rest.length; i++) {
      map.put((K) rest[i++], (V) rest[i++]);
    }
    return map;
  }

  private static <K, V> Map<V, K> inverse(Map<K, V> map) {
    HashMap<V, K> inverseMap = new HashMap<V, K>();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      inverseMap.put(entry.getValue(), entry.getKey());
    }
    return inverseMap;
  }

  /** Returns a 1-parameter function that always returns the same value. */
  public static <T, R> Function1<T, R> constant(final R r) {
    return new Function1<T, R>() {
      public R apply(T s) {
        return r;
      }
    };
  }

  /** Returns a 1-parameter function that always returns null. */
  @SuppressWarnings("unchecked")
  public static <T, R> Function1<T, R> constantNull() {
    return CONSTANT_NULL_FUNCTION1;
  }

  /**
   * A predicate with one parameter that always returns {@code true}.
   *
   * @param <T> First parameter type
   *
   * @return Predicate that always returns true
   */
  public static <T> Predicate1<T> truePredicate1() {
    //noinspection unchecked
    return (Predicate1<T>) Predicate1.TRUE;
  }

  /**
   * A predicate with one parameter that always returns {@code true}.
   *
   * @param <T> First parameter type
   *
   * @return Predicate that always returns true
   */
  public static <T> Predicate1<T> falsePredicate1() {
    //noinspection unchecked
    return (Predicate1<T>) Predicate1.FALSE;
  }

  /**
   * A predicate with two parameters that always returns {@code true}.
   *
   * @param <T1> First parameter type
   * @param <T2> Second parameter type
   *
   * @return Predicate that always returns true
   */
  public static <T1, T2> Predicate2<T1, T2> truePredicate2() {
    //noinspection unchecked
    return (Predicate2<T1, T2>) Predicate2.TRUE;
  }

  /**
   * A predicate with two parameters that always returns {@code false}.
   *
   * @param <T1> First parameter type
   * @param <T2> Second parameter type
   *
   * @return Predicate that always returns false
   */
  public static <T1, T2> Predicate2<T1, T2> falsePredicate2() {
    //noinspection unchecked
    return (Predicate2<T1, T2>) Predicate2.FALSE;
  }

  public static <TSource> Function1<TSource, TSource> identitySelector() {
    //noinspection unchecked
    return (Function1) Function1.IDENTITY;
  }

  /**
   * Creates a predicate that returns whether an object is an instance of a
   * particular type or is null.
   *
   * @param clazz Desired type
   * @param <T> Type of objects to test
   * @param <T2> Desired type
   *
   * @return Predicate that tests for desired type
   */
  public static <T, T2> Predicate1<T> ofTypePredicate(final Class<T2> clazz) {
    return new Predicate1<T>() {
      public boolean apply(T v1) {
        return v1 == null || clazz.isInstance(v1);
      }
    };
  }

  public static <T1, T2> Predicate2<T1, T2> toPredicate2(
      final Predicate1<T1> p1) {
    return new Predicate2<T1, T2>() {
      public boolean apply(T1 v1, T2 v2) {
        return p1.apply(v1);
      }
    };
  }

  /**
   * Converts a 2-parameter function to a predicate.
   */
  public static <T1, T2> Predicate2<T1, T2> toPredicate(
      final Function2<T1, T2, Boolean> function) {
    return new Predicate2<T1, T2>() {
      public boolean apply(T1 v1, T2 v2) {
        return function.apply(v1, v2);
      }
    };
  }

  /**
   * Converts a 1-parameter function to a predicate.
   */
  private static <T> Predicate1<T> toPredicate(
      final Function1<T, Boolean> function) {
    return new Predicate1<T>() {
      public boolean apply(T v1) {
        return function.apply(v1);
      }
    };
  }

  /**
   * Returns the appropriate interface for a lambda function with
   * 1 argument and the given return type.
   *
   * <p>For example:</p>
   * functionClass(Integer.TYPE) returns IntegerFunction1.class
   * functionClass(String.class) returns Function1.class
   *
   * @param aClass Return type
   *
   * @return Function class
   */
  public static Class<? extends Function> functionClass(Type aClass) {
    Class<? extends Function> c = FUNCTION1_CLASSES.get(aClass);
    if (c != null) {
      return c;
    }
    return Function1.class;
  }

  /**
   * Adapts an {@link IntegerFunction1} (that returns an {@code int}) to
   * an {@link Function1} returning an {@link Integer}.
   */
  public static <T1> Function1<T1, Integer> adapt(
      final IntegerFunction1<T1> f) {
    return new Function1<T1, Integer>() {
      public Integer apply(T1 a0) {
        return f.apply(a0);
      }
    };
  }

  /**
   * Adapts a {@link DoubleFunction1} (that returns a {@code double}) to
   * an {@link Function1} returning a {@link Double}.
   */
  public static <T1> Function1<T1, Double> adapt(final DoubleFunction1<T1> f) {
    return new Function1<T1, Double>() {
      public Double apply(T1 a0) {
        return f.apply(a0);
      }
    };
  }

  /**
   * Adapts a {@link LongFunction1} (that returns a {@code long}) to
   * an {@link Function1} returning a {@link Long}.
   */
  public static <T1> Function1<T1, Long> adapt(final LongFunction1<T1> f) {
    return new Function1<T1, Long>() {
      public Long apply(T1 a0) {
        return f.apply(a0);
      }
    };
  }

  /**
   * Adapts a {@link FloatFunction1} (that returns a {@code float}) to
   * an {@link Function1} returning a {@link Float}.
   */
  public static <T1> Function1<T1, Float> adapt(final FloatFunction1<T1> f) {
    return new Function1<T1, Float>() {
      public Float apply(T1 a0) {
        return f.apply(a0);
      }
    };
  }

  /**
   * Creates a view of a list that applies a function to each element.
   */
  public static <T1, R> List<R> adapt(final List<T1> list,
      final Function1<T1, R> f) {
    return new AbstractList<R>() {
      public R get(int index) {
        return f.apply(list.get(index));
      }

      public int size() {
        return list.size();
      }
    };
  }

  /**
   * Creates a view of an array that applies a function to each element.
   */
  public static <T, R> List<R> adapt(final T[] ts,
      final Function1<T, R> f) {
    return new AbstractList<R>() {
      public R get(int index) {
        return f.apply(ts[index]);
      }

      public int size() {
        return ts.length;
      }
    };
  }

  /**
   * Creates a copy of a list, applying a function to each element.
   */
  public static <T1, R> List<R> apply(final List<T1> list,
      final Function1<T1, R> f) {
    final ArrayList<R> list2 = new ArrayList<R>(list.size());
    for (T1 t : list) {
      list2.add(f.apply(t));
    }
    return list2;
  }

  /** Returns a list that contains only elements of {@code list} that match
   * {@code predicate}. Avoids allocating a list if all elements match or no
   * elements match. */
  public static <E> List<E> filter(List<E> list, Predicate1<E> predicate) {
  sniff:
    {
      int hitCount = 0;
      int missCount = 0;
      for (E e : list) {
        if (predicate.apply(e)) {
          if (missCount > 0) {
            break sniff;
          }
          ++hitCount;
        } else {
          if (hitCount > 0) {
            break sniff;
          }
          ++missCount;
        }
      }
      if (hitCount == 0) {
        return Collections.emptyList();
      }
      if (missCount == 0) {
        return list;
      }
    }
    final List<E> list2 = new ArrayList<E>(list.size());
    for (E e : list) {
      if (predicate.apply(e)) {
        list2.add(e);
      }
    }
    return list2;
  }

  /** Returns whether there is an element in {@code list} for which
   * {@code predicate} is true. */
  public static <E> boolean exists(List<? extends E> list,
      Predicate1<E> predicate) {
    for (E e : list) {
      if (predicate.apply(e)) {
        return true;
      }
    }
    return false;
  }

  /** Returns whether {@code predicate} is true for all elements of
   * {@code list}. */
  public static <E> boolean all(List<? extends E> list,
      Predicate1<E> predicate) {
    for (E e : list) {
      if (!predicate.apply(e)) {
        return false;
      }
    }
    return true;
  }

  /** Returns a list generated by applying a function to each index between
   * 0 and {@code size} - 1. */
  public static <E> List<E> generate(final int size,
      final Function1<Integer, E> fn) {
    if (size < 0) {
      throw new IllegalArgumentException();
    }
    return new AbstractList<E>() {
      public int size() {
        return size;
      }
      public E get(int index) {
        return fn.apply(index);
      }
    };
  }

  /**
   * Returns a function of arity 0 that does nothing.
   *
   * @param <R> Return type
   * @return Function that does nothing.
   */
  public static <R> Function0<R> ignore0() {
    //noinspection unchecked
    return Ignore.INSTANCE;
  }

  /**
   * Returns a function of arity 1 that does nothing.
   *
   * @param <R> Return type
   * @param <T0> Type of parameter 0
   * @return Function that does nothing.
   */
  public static <R, T0> Function1<R, T0> ignore1() {
    //noinspection unchecked
    return Ignore.INSTANCE;
  }

  /**
   * Returns a function of arity 2 that does nothing.
   *
   * @param <R> Return type
   * @param <T0> Type of parameter 0
   * @param <T1> Type of parameter 1
   * @return Function that does nothing.
   */
  public static <R, T0, T1> Function2<R, T0, T1> ignore2() {
    //noinspection unchecked
    return Ignore.INSTANCE;
  }

  /**
   * Returns a {@link Comparator} that handles null values.
   *
   * @param nullsFirst Whether nulls come before all other values
   * @param reverse Whether to reverse the usual order of {@link Comparable}s
   */
  @SuppressWarnings("unchecked")
  public static <T extends Comparable<T>> Comparator<T> nullsComparator(
      boolean nullsFirst,
      boolean reverse) {
    return (Comparator<T>)
        (reverse
        ? (nullsFirst
          ? NULLS_FIRST_REVERSE_COMPARATOR
          : NULLS_LAST_REVERSE_COMPARATOR)
        : (nullsFirst
          ? NULLS_FIRST_COMPARATOR
          : NULLS_LAST_COMPARATOR));
  }

  /**
   * Returns an {@link EqualityComparer} that uses object identity and hash
   * code.
   */
  @SuppressWarnings("unchecked")
  public static <T> EqualityComparer<T> identityComparer() {
    return (EqualityComparer) IDENTITY_COMPARER;
  }

  /**
   * Returns an {@link EqualityComparer} that works on arrays of objects.
   */
  @SuppressWarnings("unchecked")
  public static <T> EqualityComparer<T[]> arrayComparer() {
    return (EqualityComparer) ARRAY_COMPARER;
  }

  /**
   * Returns an {@link EqualityComparer} that uses a selector function.
   */
  public static <T, T2> EqualityComparer<T> selectorComparer(
      Function1<T, T2> selector) {
    return new SelectorEqualityComparer<T, T2>(selector);
  }

  private static class ArrayEqualityComparer
      implements EqualityComparer<Object[]> {
    public boolean equal(Object[] v1, Object[] v2) {
      return Arrays.equals(v1, v2);
    }

    public int hashCode(Object[] t) {
      return Arrays.hashCode(t);
    }
  }

  private static class IdentityEqualityComparer
      implements EqualityComparer<Object> {
    public boolean equal(Object v1, Object v2) {
      return Linq4j.equals(v1, v2);
    }

    public int hashCode(Object t) {
      return t == null ? 0x789d : t.hashCode();
    }
  }

  private static final class SelectorEqualityComparer<T, T2>
      implements EqualityComparer<T> {
    private final Function1<T, T2> selector;

    SelectorEqualityComparer(Function1<T, T2> selector) {
      this.selector = selector;
    }

    public boolean equal(T v1, T v2) {
      return v1 == v2
          || v1 != null
          && v2 != null
          && Linq4j.equals(selector.apply(v1), selector.apply(v2));
    }

    public int hashCode(T t) {
      return t == null ? 0x789d : selector.apply(t).hashCode();
    }
  }

  private static class NullsFirstComparator
      implements Comparator<Comparable>, Serializable {
    public int compare(Comparable o1, Comparable o2) {
      if (o1 == o2) {
        return 0;
      }
      if (o1 == null) {
        return -1;
      }
      if (o2 == null) {
        return 1;
      }
      //noinspection unchecked
      return o1.compareTo(o2);
    }
  }

  private static class NullsLastComparator
      implements Comparator<Comparable>, Serializable {
    public int compare(Comparable o1, Comparable o2) {
      if (o1 == o2) {
        return 0;
      }
      if (o1 == null) {
        return 1;
      }
      if (o2 == null) {
        return -1;
      }
      //noinspection unchecked
      return o1.compareTo(o2);
    }
  }

  private static class NullsFirstReverseComparator
      implements Comparator<Comparable>, Serializable  {
    public int compare(Comparable o1, Comparable o2) {
      if (o1 == o2) {
        return 0;
      }
      if (o1 == null) {
        return -1;
      }
      if (o2 == null) {
        return 1;
      }
      //noinspection unchecked
      return -o1.compareTo(o2);
    }
  }

  private static class NullsLastReverseComparator
      implements Comparator<Comparable>, Serializable  {
    public int compare(Comparable o1, Comparable o2) {
      if (o1 == o2) {
        return 0;
      }
      if (o1 == null) {
        return 1;
      }
      if (o2 == null) {
        return -1;
      }
      //noinspection unchecked
      return -o1.compareTo(o2);
    }
  }

  private static final class Ignore<R, T0, T1>
      implements Function0<R>, Function1<T0, R>, Function2<T0, T1, R> {
    public R apply() {
      return null;
    }

    public R apply(T0 p0) {
      return null;
    }

    public R apply(T0 p0, T1 p1) {
      return null;
    }

    static final Ignore INSTANCE = new Ignore();
  }
}

// End Functions.java
