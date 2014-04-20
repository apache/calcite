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

import java.util.*;

/**
 * Static methods for dealing with {@link Predicate} objects.
 */
public class Predicates {

  private static final Predicate<Object> IS_NULL =
      new AbstractPredicate<Object>() {
        public boolean test(Object o) {
          return o == null;
        }
      };

  private static final Predicate<Object> NON_NULL =
      new AbstractPredicate<Object>() {
        public boolean test(Object o) {
          return o != null;
        }
      };

  private static final Predicate<Object> FALSE =
      new AbstractPredicate<Object>() {
        public boolean test(Object o) {
          return false;
        }
      };

  private static final Predicate<Object> TRUE =
      new AbstractPredicate<Object>() {
        public boolean test(Object o) {
          return true;
        }
      };

  private Predicates() {
    throw new AssertionError("No instances!");
  }

  @SuppressWarnings("unchecked")
  public static <T> Predicate<T> isNull() {
    return (Predicate<T>) IS_NULL;
  }

  @SuppressWarnings("unchecked")
  public static <T> Predicate<T> nonNull() {
    return (Predicate<T>) NON_NULL;
  }

  @SuppressWarnings("unchecked")
  public static <T> Predicate<T> alwaysFalse() {
    return (Predicate<T>) FALSE;
  }

  @SuppressWarnings("unchecked")
  public static <T> Predicate<T> alwaysTrue() {
    return (Predicate<T>) TRUE;
  }

  public static <T> Predicate<T> instanceOf(final Class<?> clazz) {
    return new AbstractPredicate<T>() {
      public boolean test(T t) {
        return clazz.isInstance(t);
      }
    };
  }

  public static <T> Predicate<T> isSame(final Object target) {
    return new AbstractPredicate<T>() {
      public boolean test(T t) {
        return t == target;
      }
    };
  }

  public static <T> Predicate<T> isEqual(final Object target) {
    if (null == target) {
      return Predicates.isNull();
    } else {
      return new AbstractPredicate<T>() {
        public boolean test(T t) {
          return target.equals(t);
        }
      };
    }
  }

  public static <T> Predicate<T> contains(final Collection<?> target) {
    return new AbstractPredicate<T>() {
      public boolean test(T t) {
        return target.contains(t);
      }
    };
  }

  public static <T, V> Predicate<T> compose(
      final Predicate<? super V> predicate,
      final Mapper<? super T, ? extends V> mapper) {
    return new AbstractPredicate<T>() {
      public boolean test(T t) {
        return predicate.test(mapper.map(t));
      }
    };
  }

  public static <T> Predicate<T> negate(final Predicate<? super T> predicate) {
    return new AbstractPredicate<T>() {
      public boolean test(T t) {
        return !predicate.test(t);
      }
    };
  }

  public static <T> Predicate<T> and(final Predicate<? super T> first,
      final Predicate<? super T> second) {
    return new AbstractPredicate<T>() {
      public boolean test(T t) {
        return first.test(t) && second.test(t);
      }
    };
  }

  public static <T> Predicate<T> and(
      final Iterable<? extends Predicate<? super T>> predicates) {
    return new AbstractPredicate<T>() {
      public boolean test(T t) {
        for (Predicate<? super T> predicate : predicates) {
          if (predicate.test(t)) {
            return false;
          }
        }
        return true;
      }
    };
  }

  public static <T> Predicate<T> and(final Predicate<? super T> first,
      final Iterable<? extends Predicate<? super T>> predicates) {
    return new AbstractPredicate<T>() {
      public boolean test(T t) {
        if (!first.test(t)) {
          return false;
        }
        for (Predicate<? super T> predicate : predicates) {
          if (predicate.test(t)) {
            return false;
          }
        }
        return true;
      }
    };
  }

  public static <T> Predicate<T> or(final Predicate<? super T> first,
      final Predicate<? super T> second) {
    return new AbstractPredicate<T>() {
      public boolean test(T t) {
        return first.test(t) || second.test(t);
      }
    };
  }


  public static <T> Predicate<T> or(
      final Iterable<? extends Predicate<? super T>> predicates) {
    return new AbstractPredicate<T>() {
      public boolean test(T t) {
        for (Predicate<? super T> predicate : predicates) {
          if (predicate.test(t)) {
            return true;
          }
        }
        return false;
      }
    };
  }

  //@SafeVarargs
  public static <T> Predicate<T> or(Predicate<? super T>... predicates) {
    return or(Arrays.asList(predicates));
  }

  public static <T> Predicate<T> xor(final Predicate<? super T> first,
      final Predicate<? super T> second) {
    return new AbstractPredicate<T>() {
      public boolean test(T t) {
        return first.test(t) ^ second.test(t);
      }
    };
  }

  public static <T> Predicate<T> xor(
      Iterable<? extends Predicate<? super T>> predicates) {
    throw new UnsupportedOperationException(); // TODO:
  }

  //@SafeVarargs
  public static <T> Predicate<T> xor(Predicate<? super T>... predicates) {
    return xor(Arrays.asList(predicates));
  }

  public abstract static class AbstractPredicate<T> implements Predicate<T> {
    public Predicate<T> and(Predicate<? super T> p) {
      //noinspection unchecked
      return Predicates.and((Predicate) this, p);
    }

    public Predicate<T> or(Predicate<? super T> p) {
      //noinspection unchecked
      return Predicates.or((Predicate) this, p);
    }

    public Predicate<T> xor(Predicate<? super T> p) {
      //noinspection unchecked
      return Predicates.xor((Predicate) this, p);
    }

    public Predicate<T> negate() {
      return Predicates.negate(this);
    }
  }
}

// End Predicates.java
