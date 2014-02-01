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
package net.hydromatic.lambda.functions;

import net.hydromatic.linq4j.Linq4j;

import java.util.Map;

/**
 * Utilities for {@link Mapper}.
 */
public class Mappers {
  private Mappers() {
    throw new AssertionError();
  }

  private static final Mapper<Object, Object> IDENTITY =
      new Mapper<Object, Object>() {
        public Object map(Object o) {
          return o;
        }

        public <V> Mapper<Object, V> compose(
            Mapper<? super Object, ? extends V> after) {
          return chain(this, after);
        }
      };

  private static final Mapper<Object, String> STRING =
      new Mapper<Object, String>() {
        public String map(Object o) {
          return String.valueOf(o);
        }

        public <V> Mapper<Object, V> compose(
            Mapper<? super String, ? extends V> after) {
          return chain(this, after);
        }
      };

  @SuppressWarnings("unchecked")
  public static <T> Mapper<T, T> identity() {
    return (Mapper<T, T>) IDENTITY;
  }

  @SuppressWarnings("unchecked")
  public static <T> Mapper<T, String> string() {
    return (Mapper<T, String>) STRING;
  }

  public static <T, U, V> Mapper<T, V> chain(
      final Mapper<? super T, ? extends U> first,
      final Mapper<? super U, ? extends V> second) {
    return new Mapper<T, V>() {
      public V map(T t) {
        return second.map(first.map(t));
      }

      public <W> Mapper<T, W> compose(Mapper<? super V, ? extends W> after) {
        return chain(this, after);
      }
    };
  }

  public static <T, U> Mapper<T, U> constant(final U constant) {
    return new Mapper<T, U>() {
      public U map(T t) {
        return constant;
      }

      public <V> Mapper<T, V> compose(Mapper<? super U, ? extends V> after) {
        return chain(this, after);
      }
    };
  }

  public static <T> Mapper<T, T> substitute(final T subOut, final T subIn) {
    return new Mapper<T, T>() {
      public T map(T t) {
        return Linq4j.equals(subOut, t) ? subIn : t;
      }

      public <V> Mapper<T, V> compose(Mapper<? super T, ? extends V> after) {
        return chain(this, after);
      }
    };
  }

  public static <T, U> Mapper<T, U> instantiate(Class<? extends T> clazzT,
      Class<? extends U> clazzU) {
    throw new UnsupportedOperationException(); // tODO:
  }

  public static <T, U> Mapper<T, U> forMap(
      final Map<? super T, ? extends U> map) {
    return new Mapper<T, U>() {
      public U map(T t) {
        if (map.containsKey(t)) {
          return map.get(t);
        }
        throw new IllegalArgumentException("unmappable <T> : " + t);
      }

      public <V> Mapper<T, V> compose(Mapper<? super U, ? extends V> after) {
        return chain(this, after);
      }
    };
  }

  public static <T, U> Mapper<T, U> forMap(
      final Map<? super T, ? extends U> map, final U defaultValue) {
    return new Mapper<T, U>() {
      public U map(T t) {
        return map.containsKey(t) ? map.get(t) : defaultValue;
      }

      public <V> Mapper<T, V> compose(Mapper<? super U, ? extends V> after) {
        return chain(this, after);
      }
    };
  }

  public static <T, U> Mapper<T, U> forPredicate(
      final Predicate<? super T> predicate, final U forTrue, final U forFalse) {
    return new Mapper<T, U>() {
      public U map(T t) {
        return predicate.test(t) ? forTrue : forFalse;
      }

      public <V> Mapper<T, V> compose(Mapper<? super U, ? extends V> after) {
        return chain(this, after);
      }
    };
  }

  public abstract static class AbstractMapper<T, R> implements Mapper<T, R> {
    public <V> Mapper<T, V> compose(Mapper<? super R, ? extends V> after) {
      return chain(this, after);
    }
  }
}

// End Mappers.java
