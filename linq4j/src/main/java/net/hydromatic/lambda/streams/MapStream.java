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
package net.hydromatic.lambda.streams;

import net.hydromatic.lambda.functions.*;

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;

import java.util.*;

/**
 * A stream of two element tuples. Depending on the source either of the tuple
 * elements may be unique.
 *
 * <p>(Based upon java.lang.MapStream coming in JDK 8.)</p>
 *
 * <p>JDK 1.7 does not have the mechanism for adding default implementations
 * of interface methods. Therefore each class that implements this interface
 * has to implement each method. To see the default implementation of each
 * method, see the implementation in {@link AbstractMapStream}. It is not
 * required that an implementing class extend {@code AbstractMapStream}.
 * The methods in {@code AbstractMapStream} call static methods in {@link Impl},
 * so it should be straightforward to reuse implementations.</p>
 */
public interface MapStream<K, V> extends Iterable<BiValue<K, V>> {

  boolean isEmpty();

  // name is temporary. "keys" is unavailable.
  Iterable<K> inputs();

  Iterable<V> values();

  Iterable<BiValue<K, V>> asIterable();

  BiValue<K, V> getFirst();

  BiValue<K, V> getOnly();

  BiValue<K, V> getAny();

  MapStream<K, Iterable<V>> asMultiStream();

  void forEach(BiBlock<? super K, ? super V> block);

  MapStream<K, V> filter(final BiPredicate<? super K, ? super V> predicate);

  MapStream<K, V> filterKeys(final Predicate<K> filter);

  MapStream<K, V> filterValues(final Predicate<V> filter);

  <W> MapStream<K, W> map(final BiMapper<K, V, W> mapper);

  <W> MapStream<K, W> mapValues(final Mapper<V, W> mapper);

  <W> MapStream<K, Iterable<W>> mapValuesMulti(
      final BiMapper<? super K, ? super V, Iterable<W>> mapper);

  <A extends Map<? super K, ? super V>> A into(A destination);

  <A extends Map<? super K, C>, C extends Collection<? super V>> A intoMulti(
      A destination, Factory<C> factory);

  boolean anyMatch(BiPredicate<? super K, ? super V> predicate);

  boolean allMatch(BiPredicate<? super K, ? super V> predicate);

  boolean noneMatch(BiPredicate<? super K, ? super V> predicate);

  MapStream<K, V> sorted(Comparator<? super K> comparator);

  MapStream<V, K> swap();

  MapStream<K, V> merge(MapStream<K, V> other);

  abstract class Impl {
    private Impl() {}
    public static <K, V> BiValue<K, V> getFirst(MapStream<K, V> s) {
      Iterator<? extends BiValue<K, V>> iterator = s.asIterable().iterator();
      return iterator.hasNext() ? iterator.next() : null;
    }

    public static <K, V> BiValue<K, V> getOnly(MapStream<K, V> s) {
      return Iterables.only(s.asIterable());
    }

    public static <K, V> MapStream<K, Iterable<V>> asMultiStream(
        MapStream<K, V> s) {
      return s.mapValues(new Mapper<V, Iterable<V>>() {
        public Iterable<V> map(V v) {
          return Collections.singletonList(v);
        }

        public <W> Mapper<V, W> compose(
            Mapper<? super Iterable<V>, ? extends W> after) {
          return Mappers.chain(this, after);
        }
      });

      // v - > (Iterable<V>) Collections.singletonList(v));
    }

    public static <K, V> void forEach(MapStream<K, V> s,
        BiBlock<? super K, ? super V> block) {
      for (BiValue<K, V> each : s.asIterable()) {
        block.apply(each.getKey(), each.getValue());
      }
    }

    public static <K, V> MapStream<K, V> filter(final MapStream<K, V> s,
        final BiPredicate<? super K, ? super V> predicate) {
      return new AbstractMapStream<K, V>() {
        public Iterable<BiValue<K, V>> asIterable() {
          final Enumerator<BiValue<K, V>> enumerator =
              Linq4j.iterableEnumerator(s);
          final Enumerator<BiValue<K, V>> enumerator2 =
              new Enumerator<BiValue<K, V>>() {
                public BiValue<K, V> current() {
                  return enumerator.current();
                }

                public boolean moveNext() {
                  while (enumerator.moveNext()) {
                    BiValue<K, V> o = enumerator.current();
                    if (predicate.eval(o.getKey(), o.getValue())) {
                      return true;
                    }
                  }
                  return false;
                }

                public void reset() {
                  enumerator.reset();
                }

                public void close() {
                  enumerator.close();
                }
              };
          return new Iterable<BiValue<K, V>>() {
            public Iterator<BiValue<K, V>> iterator() {
              return Linq4j.enumeratorIterator(enumerator2);
            }
          };
        }
      };
    }

    public static <K, V> MapStream<K, V> sorted(MapStream<K, V> s,
        Comparator<? super K> comparator) {
      TreeMap<K, V> result = new TreeMap<K, V>(comparator);
      for (BiValue<K, V> kv : s) {
        result.put(kv.getKey(), kv.getValue());
      }
      return of(result.entrySet());
    }

    public static <K, V> MapStream<V, K> swap(final MapStream<K, V> s) {
      return new AbstractMapStream<V, K>() {
        public Iterable<BiValue<V, K>> asIterable() {
          return new Iterable<BiValue<V, K>>() {
            final Iterator<BiValue<K, V>> x = s.asIterable().iterator();

            public Iterator<BiValue<V, K>> iterator() {
              return new Iterator<BiValue<V, K>>() {
                public boolean hasNext() {
                  return x.hasNext();
                }

                public BiValue<V, K> next() {
                  BiValue<K, V> o = x.next();
                  return BiVal.of(o.getValue(), o.getKey());
                }

                public void remove() {
                  x.remove();
                }
              };
            }
          };
        }
      };
    }

    public static <K, V> boolean anyMatch(MapStream<K, V> s,
        BiPredicate<? super K, ? super V> biPredicate) {
      for (BiValue<K, V> each : s.asIterable()) {
        if (biPredicate.eval(each.getKey(), each.getValue())) {
          return true;
        }
      }
      return false;
    }

    public static <K, V> boolean isEmpty(MapStream<K, V> s) {
      Iterator<BiValue<K, V>> iterator = s.iterator();
      return !iterator.hasNext();
    }

    public static <K, V> boolean allMatch(MapStream<K, V> s,
        BiPredicate<? super K, ? super V> biPredicate) {
      for (BiValue<K, V> each : s.asIterable()) {
        if (biPredicate.eval(each.getKey(), each.getValue())) {
          return false;
        }
      }
      return true;
    }

    public static <K, V> BiValue<K, V> getAny(MapStream<K, V> s) {
      // NOTE: the implementation can return another element if getFirst()
      //   is expensive
      return s.getFirst();
    }

    public static <K, V> MapStream<K, V> filterKeys(MapStream<K, V> s,
        final Predicate<K> filter) {
      return filter(s, new BiPredicates.AbstractBiPredicate<K, V>() {
        public boolean eval(K k, V v) {
          return filter.test(k);
        }
      });
    }

    public static <K, V> MapStream<K, V> filterValues(MapStream<K, V> s,
        final Predicate<V> filter) {
      return filter(s, new BiPredicates.AbstractBiPredicate<K, V>() {
        public boolean eval(K k, V v) {
          return filter.test(v);
        }
      });
    }

    public static <K, V, W> MapStream<K, W> mapValues(final MapStream<K, V> s,
        final Mapper<V, W> mapper) {
      return new AbstractMapStream<K, W>() {
        public Iterable<BiValue<K, W>> asIterable() {
          return new Iterable<BiValue<K, W>>() {
            public Iterator<BiValue<K, W>> iterator() {
              final Iterator<BiValue<K, V>> x = s.iterator();
              return new Iterator<BiValue<K, W>>() {
                public boolean hasNext() {
                  return x.hasNext();
                }

                public BiValue<K, W> next() {
                  BiValue<K, V> next = x.next();
                  K key = next.getKey();
                  V value = next.getValue();
                  return BiVal.of(key, mapper.map(value));
                }

                public void remove() {
                  x.remove();
                }
              };
            }
          };
        }
      };
    }

    public static <K, V, W> MapStream<K, Iterable<W>> mapValuesMulti(
        final MapStream<K, V> s,
        final BiMapper<? super K, ? super V, Iterable<W>> iterableBiMapper) {
      return new AbstractMapStream<K, Iterable<W>>() {
        public Iterable<BiValue<K, Iterable<W>>> asIterable() {
          return new Iterable<BiValue<K, Iterable<W>>>() {
            public Iterator<BiValue<K, Iterable<W>>> iterator() {
              final Iterator<BiValue<K, V>> x = s.iterator();
              return new Iterator<BiValue<K, Iterable<W>>>() {
                public boolean hasNext() {
                  return x.hasNext();
                }

                public BiValue<K, Iterable<W>> next() {
                  BiValue<K, V> next = x.next();
                  K key = next.getKey();
                  return BiVal.of(key, iterableBiMapper.map(key,
                      next.getValue()));
                }

                public void remove() {
                  x.remove();
                }
              };
            }
          };
        }
      };
    }

    public static <K, V, W> MapStream<K, W> map(final MapStream<K, V> s,
        final BiMapper<K, V, W> mapper) {
      return new AbstractMapStream<K, W>() {
        public Iterable<BiValue<K, W>> asIterable() {
          return new Iterable<BiValue<K, W>>() {
            public Iterator<BiValue<K, W>> iterator() {
              final Iterator<BiValue<K, V>> x = s.iterator();
              return new Iterator<BiValue<K, W>>() {
                public boolean hasNext() {
                  return x.hasNext();
                }

                public BiValue<K, W> next() {
                  BiValue<K, V> next = x.next();
                  K k = next.getKey();
                  V v = next.getValue();
                  return BiVal.of(k, mapper.map(k, v));
                }

                public void remove() {
                  x.remove();
                }
              };
            }
          };
        }
      };
    }

    public static <K, V> Iterable<K> inputs(MapStream<K, V> s) {
      final Iterable<BiValue<K, V>> filteredElements = s.asIterable();
      return new Iterable<K>() {
        public Iterator<K> iterator() {
          return new Iterator<K>() {
            final Iterator<? extends BiValue<K, V>> source =
                filteredElements.iterator();

            public boolean hasNext() {
              return source.hasNext();
            }

            public K next() {
              return source.next().getKey();
            }

            public void remove() {
              source.remove();
            }
          };
        }
      };
    }

    public static <K, V> Iterable<V> values(MapStream<K, V> s) {
      final Iterable<BiValue<K, V>> filteredElements = s.asIterable();
      return new Iterable<V>() {
        public Iterator<V> iterator() {
          return new Iterator<V>() {
            final Iterator<? extends BiValue<K, V>> source =
                filteredElements.iterator();

            public boolean hasNext() {
              return source.hasNext();
            }

            public V next() {
              return source.next().getValue();
            }

            public void remove() {
              source.remove();
            }
          };
        }
      };
    }

    public static <K, V> MapStream<K, V> merge(MapStream<K, V> s,
        MapStream<K, V> other) {
      Map<K, V> union = new HashMap<K, V>();
      s.into(union);
      other.into(union);
      return of(union.entrySet());
    }

    public static <K, V, A extends Map<? super K, ? super V>> A into(
        MapStream<K, V> s, A destination) {
      for (BiValue<K, V> kv : s) {
        destination.put(kv.getKey(), kv.getValue());
      }
      return destination;
    }

    public static <K,
        V,
        A extends Map<? super K, C>,
        C extends Collection<? super V>>
    A intoMulti(MapStream<K, V> s, A destination, Factory<C> factory) {
      for (BiValue<K, V> kv : s) {
        C c = factory.make();
        c.add(kv.getValue());
        destination.put(kv.getKey(), c);
      }
      return destination;
    }

    public static <K, V> MapStream<K, V> of(
        final Iterable<Map.Entry<K, V>> iterable) {
      return new AbstractMapStream<K, V>() {
        public Iterable<BiValue<K, V>> asIterable() {
          return new Iterable<BiValue<K, V>>() {
            final Iterator<Map.Entry<K, V>> x = iterable.iterator();

            public Iterator<BiValue<K, V>> iterator() {
              return new Iterator<BiValue<K, V>>() {
                public boolean hasNext() {
                  return x.hasNext();
                }

                public BiValue<K, V> next() {
                  Map.Entry<K, V> o = x.next();
                  return BiVal.of(o.getKey(), o.getValue());
                }

                public void remove() {
                  x.remove();
                }
              };
            }
          };
        }
      };
    }
  }

  abstract class AbstractMapStream<K, V> implements MapStream<K, V> {
    public boolean isEmpty() {
      return Impl.isEmpty(this);
    }

    public boolean allMatch(BiPredicate<? super K, ? super V> biPredicate) {
      return Impl.allMatch(this, biPredicate);
    }

    public BiValue<K, V> getFirst() {
      return Impl.getFirst(this);
    }

    public BiValue<K, V> getOnly() {
      return Impl.getOnly(this);
    }

    public BiValue<K, V> getAny() {
      return Impl.getAny(this);
    }

    public MapStream<K, Iterable<V>> asMultiStream() {
      return Impl.asMultiStream(this);
    }

    public void forEach(BiBlock<? super K, ? super V> biBlock) {
      Impl.forEach(this, biBlock);
    }

    public MapStream<K, V> filter(
        BiPredicate<? super K, ? super V> biPredicate) {
      return Impl.filter(this, biPredicate);
    }

    public MapStream<K, V> filterKeys(Predicate<K> filter) {
      return Impl.filterKeys(this, filter);
    }

    public MapStream<K, V> filterValues(Predicate<V> filter) {
      return Impl.filterValues(this, filter);
    }

    public <W> MapStream<K, W> map(BiMapper<K, V, W> kvwBiMapper) {
      return Impl.map(this, kvwBiMapper);
    }

    public <W> MapStream<K, W> mapValues(Mapper<V, W> vwMapper) {
      return Impl.mapValues(this, vwMapper);
    }

    public <W> MapStream<K, Iterable<W>> mapValuesMulti(
        BiMapper<? super K, ? super V, Iterable<W>> iterableBiMapper) {
      return Impl.mapValuesMulti(this, iterableBiMapper);
    }

    public <A extends Map<? super K, ? super V>> A into(A destination) {
      return Impl.into(this, destination);
    }

    public <A extends Map<? super K, C>, C extends Collection<? super V>>
    A intoMulti(A destination, Factory<C> factory) {
      return Impl.intoMulti(this, destination, factory);
    }

    public boolean anyMatch(BiPredicate<? super K, ? super V> biPredicate) {
      return Impl.anyMatch(this, biPredicate);
    }

    public boolean noneMatch(BiPredicate<? super K, ? super V> biPredicate) {
      return !Impl.anyMatch(this, biPredicate);
    }

    public MapStream<K, V> sorted(Comparator<? super K> comparator) {
      return Impl.sorted(this, comparator);
    }

    public MapStream<V, K> swap() {
      return Impl.swap(this);
    }

    public MapStream<K, V> merge(MapStream<K, V> other) {
      return Impl.merge(this, other);
    }

    public Iterator<BiValue<K, V>> iterator() {
      return asIterable().iterator();
    }

    public Iterable<K> inputs() {
      return Impl.inputs(this);
    }

    public Iterable<V> values() {
      return Impl.values(this);
    }
  }

  class BiVal<K, V> implements BiValue<K, V> {
    private final K k;
    private final V v;

    BiVal(K k, V v) {
      this.k = k;
      this.v = v;
    }

    public K getKey() {
      return k;
    }

    public V getValue() {
      return v;
    }

    public static <K, V> BiVal<K, V> of(K k, V v) {
      return new BiVal<K, V>(k, v);
    }
  }
}

// End MapStream.java
