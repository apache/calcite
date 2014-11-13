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

import java.util.Arrays;
import java.util.Iterator;

/**
 * Static methods for dealing with {@link BiPredicate} objects.
 */
public class BiPredicates {

  private static final BiPredicate<Object, Object> IS_NULL =
      new AbstractBiPredicate<Object, Object>() {
        public boolean eval(Object o, Object o2) {
          return o == null;
        }
      };

  private static final BiPredicate<Object, Object> NON_NULL =
      new AbstractBiPredicate<Object, Object>() {
        public boolean eval(Object o, Object o2) {
          return o != null;
        }
      };

  private static final BiPredicate<Object, Object> FALSE =
      new AbstractBiPredicate<Object, Object>() {
        public boolean eval(Object o, Object o2) {
          return false;
        }
      };

  private static final BiPredicate<Object, Object> TRUE =
      new AbstractBiPredicate<Object, Object>() {
        public boolean eval(Object o, Object o2) {
          return false;
        }
      };

  private BiPredicates() {
    throw new AssertionError("No instances!");
  }

  @SuppressWarnings("unchecked")
  public static <K, V> BiPredicate<K, V> isNull() {
    return (BiPredicate<K, V>) IS_NULL;
  }

  @SuppressWarnings("unchecked")
  public static <K, V> BiPredicate<K, V> nonNull() {
    return (BiPredicate<K, V>) NON_NULL;
  }

  @SuppressWarnings("unchecked")
  public static <K, V> BiPredicate<K, V> alwaysFalse() {
    return (BiPredicate<K, V>) FALSE;
  }

  @SuppressWarnings("unchecked")
  public static <K, V> BiPredicate<K, V> alwaysTrue() {
    return (BiPredicate<K, V>) TRUE;
  }

  public static <K, V> BiPredicate<K, V> negate(
      final BiPredicate<? super K, ? super V> biPredicate) {
    return new AbstractBiPredicate<K, V>() {
      public boolean eval(K k, V v) {
        return !biPredicate.eval(k, v);
      }
    };
  }

  public static <K, V> BiPredicate<K, V> and(
      final BiPredicate<? super K, ? super V> first,
      final BiPredicate<? super K, ? super V> second) {
    return new AbstractBiPredicate<K, V>() {
      public boolean eval(K k, V v) {
        return first.eval(k, v) && second.eval(k, v);
      }
    };
  }

  public static <K, V> BiPredicate<K, V> and(
      final Iterable<? extends BiPredicate<? super K, ? super V>> predicates) {
    return new AbstractBiPredicate<K, V>() {
      public boolean eval(K k, V v) {
        for (BiPredicate<? super K, ? super V> predicate : predicates) {
          if (!predicate.eval(k, v)) {
            return false;
          }
        }
        return true;
      }
    };
  }

  public static <K, V> BiPredicate<K, V> and(
      final BiPredicate<? super K, ? super V> first,
      final Iterable<? extends BiPredicate<? super K, ? super V>> predicates) {
    return new AbstractBiPredicate<K, V>() {
      public boolean eval(K k, V v) {
        if (!first.eval(k, v)) {
          return false;
        }
        for (BiPredicate<? super K, ? super V> predicate : predicates) {
          if (!predicate.eval(k, v)) {
            return false;
          }
        }
        return true;
      }
    };
  }

  public static <K, V> BiPredicate<K, V> or(
      final BiPredicate<? super K, ? super V> first,
      final BiPredicate<? super K, ? super V> second) {
    return new AbstractBiPredicate<K, V>() {
      public boolean eval(K k, V v) {
        return first.eval(k, v) || second.eval(k, v);
      }
    };
  }

  public static <K, V> BiPredicate<K, V> or(
      final Iterable<? extends BiPredicate<? super K, ? super V>> predicates) {
    return new AbstractBiPredicate<K, V>() {
      public boolean eval(K k, V v) {
        for (BiPredicate<? super K, ? super V> predicate : predicates) {
          if (predicate.eval(k, v)) {
            return true;
          }
        }
        return false;
      }
    };
  }

  public static <K, V> BiPredicate<K, V> or(
      final BiPredicate<? super K, ? super V> first,
      final Iterable<? extends BiPredicate<? super K, ? super V>> predicates) {
    return new AbstractBiPredicate<K, V>() {
      public boolean eval(K k, V v) {
        if (first.eval(k, v)) {
          return true;
        }
        for (BiPredicate<? super K, ? super V> predicate : predicates) {
          if (predicate.eval(k, v)) {
            return true;
          }
        }
        return false;
      }
    };
  }

  public static <K, V> BiPredicate<K, V> or(
      BiPredicate<? super K, ? super V>... predicates) {
    return or(Arrays.asList(predicates));
  }

  public static <K, V> BiPredicate<K, V> xor(
      final BiPredicate<? super K, ? super V> first,
      final BiPredicate<? super K, ? super V> second) {
    return new AbstractBiPredicate<K, V>() {
      public boolean eval(K k, V v) {
        return first.eval(k, v) ^ second.eval(k, v);
      }
    };
  }

  public static <K, V> BiPredicate<K, V> xor(
      final Iterable<? extends BiPredicate<? super K, ? super V>> predicates) {
    return new AbstractBiPredicate<K, V>() {
      public boolean eval(K k, V v) {
        Iterator<? extends BiPredicate<? super K, ? super V>>
            iterator =
            predicates.iterator();
        if (!iterator.hasNext()) {
          return false;
        } else {
          final boolean b = iterator.next().eval(k, v);
          while (iterator.hasNext()) {
            if (iterator.next().eval(k, v) != b) {
              return true;
            }
          }
          return false;
        }
      }
    };
  }

  public static <K, V> BiPredicate<K, V> xor(
      final BiPredicate<? super K, ? super V> first,
      final Iterable<? extends BiPredicate<? super K, ? super V>> predicates) {
    return new AbstractBiPredicate<K, V>() {
      public boolean eval(K k, V v) {
        boolean b = first.eval(k, v);
        for (BiPredicate<? super K, ? super V> predicate : predicates) {
          if (predicate.eval(k, v) != b) {
            return true;
          }
        }
        return false;
      }
    };
  }

  //@SafeVarargs
  public static <K, V> BiPredicate<K, V> xor(
      BiPredicate<? super K, ? super V>... predicates) {
    return xor(Arrays.asList(predicates));
  }

  public abstract static class AbstractBiPredicate<K, V>
      implements BiPredicate<K, V> {
    public BiPredicate<K, V> and(BiPredicate<? super K, ? super V> p) {
      //noinspection unchecked
      return BiPredicates.and((BiPredicate) this, (BiPredicate) p);
    }

    public BiPredicate<K, V> or(BiPredicate<? super K, ? super V> p) {
      //noinspection unchecked
      return BiPredicates.or((BiPredicate) this, p);
    }

    public BiPredicate<K, V> xor(BiPredicate<? super K, ? super V> p) {
      //noinspection unchecked
      return BiPredicates.xor((BiPredicate) this, p);
    }

    public BiPredicate<K, V> negate() {
      return BiPredicates.negate(this);
    }
  }
}

// End BiPredicates.java
