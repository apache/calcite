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

import org.apache.calcite.linq4j.function.Function2;

import org.checkerframework.checker.nullness.qual.KeyFor;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link Lookup} that uses an underlying map.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
class LookupImpl<K, V> extends AbstractEnumerable<Grouping<K, V>>
    implements Lookup<K, V> {
  private final Map<K, List<V>> map;

  /**
   * Creates a MultiMapImpl.
   *
   * @param map Underlying map
   */
  LookupImpl(Map<K, List<V>> map) {
    this.map = map;
  }

  @Override public Enumerator<Grouping<K, V>> enumerator() {
    return new Enumerator<Grouping<K, V>>() {
      final Enumerator<Entry<K, List<V>>> enumerator =
          Linq4j.enumerator(map.entrySet());

      @Override public Grouping<K, V> current() {
        final Entry<K, List<V>> keyAndList = enumerator.current();
        return new GroupingImpl<>(keyAndList.getKey(),
            keyAndList.getValue());
      }

      @Override public boolean moveNext() {
        return enumerator.moveNext();
      }

      @Override public void reset() {
        enumerator.reset();
      }

      @Override public void close() {
        enumerator.close();
      }
    };
  }

  // Map methods

  @Override public int size() {
    return map.size();
  }

  @Override public boolean isEmpty() {
    return map.isEmpty();
  }

  @SuppressWarnings("contracts.conditional.postcondition.not.satisfied")
  @Override public boolean containsKey(@Nullable Object key) {
    return map.containsKey(key);
  }

  @Override public boolean containsValue(@Nullable Object value) {
    @SuppressWarnings("unchecked")
    List<V> list = (List<V>) value;
    return map.containsValue(list);
  }

  @Override public @Nullable Enumerable<V> get(@Nullable Object key) {
    final List<V> list = map.get(key);
    return list == null ? null : Linq4j.asEnumerable(list);
  }

  @SuppressWarnings("contracts.postcondition.not.satisfied")
  @Override public @Nullable Enumerable<V> put(K key, Enumerable<V> value) {
    final List<V> list = map.put(key, value.toList());
    return list == null ? null : Linq4j.asEnumerable(list);
  }

  @Override public @Nullable Enumerable<V> remove(@Nullable Object key) {
    final List<V> list = map.remove(key);
    return list == null ? null : Linq4j.asEnumerable(list);
  }

  @Override public void putAll(Map<? extends K, ? extends Enumerable<V>> m) {
    for (Entry<? extends K, ? extends Enumerable<V>> entry : m.entrySet()) {
      map.put(entry.getKey(), entry.getValue().toList());
    }
  }

  @Override public void clear() {
    map.clear();
  }

  @SuppressWarnings("return.type.incompatible")
  @Override public Set<@KeyFor("this") K> keySet() {
    return map.keySet();
  }

  @Override public Collection<Enumerable<V>> values() {
    final Collection<List<V>> lists = map.values();
    return new AbstractCollection<Enumerable<V>>() {
      @Override public Iterator<Enumerable<V>> iterator() {
        return new Iterator<Enumerable<V>>() {
          final Iterator<List<V>> iterator = lists.iterator();

          @Override public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override public Enumerable<V> next() {
            return Linq4j.asEnumerable(iterator.next());
          }

          @Override public void remove() {
            iterator.remove();
          }
        };
      }

      @Override public int size() {
        return lists.size();
      }
    };
  }

  @SuppressWarnings("return.type.incompatible")
  @Override public Set<Entry<@KeyFor({"this"}) K, Enumerable<V>>> entrySet() {
    final Set<Entry<@KeyFor("map") K, List<V>>> entries = map.entrySet();
    return new AbstractSet<Entry<K, Enumerable<V>>>() {
      @Override public Iterator<Entry<K, Enumerable<V>>> iterator() {
        final Iterator<Entry<K, List<V>>> iterator = entries.iterator();
        return new Iterator<Entry<K, Enumerable<V>>>() {
          @Override public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override public Entry<K, Enumerable<V>> next() {
            final Entry<K, List<V>> entry = iterator.next();
            return new AbstractMap.SimpleEntry<>(entry.getKey(),
                Linq4j.asEnumerable(entry.getValue()));
          }

          @Override public void remove() {
            iterator.remove();
          }
        };
      }

      @Override public int size() {
        return entries.size();
      }
    };
  }

  @Override public <TResult> Enumerable<TResult> applyResultSelector(
      final Function2<K, Enumerable<V>, TResult> resultSelector) {
    return new AbstractEnumerable<TResult>() {
      @Override public Enumerator<TResult> enumerator() {
        final Enumerator<Grouping<K, V>> groupingEnumerator =
            LookupImpl.this.enumerator();
        return new Enumerator<TResult>() {
          @Override public TResult current() {
            final Grouping<K, V> grouping = groupingEnumerator.current();
            return resultSelector.apply(grouping.getKey(), grouping);
          }

          @Override public boolean moveNext() {
            return groupingEnumerator.moveNext();
          }

          @Override public void reset() {
            groupingEnumerator.reset();
          }

          @Override public void close() {
            groupingEnumerator.close();
          }
        };
      }
    };
  }

  /**
   * Returns an enumerable over the values in this lookup, in map order.
   * If the map is sorted, the values will be emitted sorted by key.
   */
  public Enumerable<V> valuesEnumerable() {
    return new AbstractEnumerable<V>() {
      @Override public Enumerator<V> enumerator() {
        final Enumerator<Enumerable<V>> listEnumerator =
            Linq4j.iterableEnumerator(values());
        return new Enumerator<V>() {
          Enumerator<V> enumerator = Linq4j.emptyEnumerator();

          @Override public V current() {
            return enumerator.current();
          }

          @Override public boolean moveNext() {
            for (;;) {
              if (enumerator.moveNext()) {
                return true;
              }
              enumerator.close();
              if (!listEnumerator.moveNext()) {
                enumerator = Linq4j.emptyEnumerator();
                return false;
              }
              enumerator = listEnumerator.current().enumerator();
            }
          }

          @Override public void reset() {
            listEnumerator.reset();
            enumerator = Linq4j.emptyEnumerator();
          }

          @Override public void close() {
            enumerator.close();
          }
        };
      }
    };
  }
}
