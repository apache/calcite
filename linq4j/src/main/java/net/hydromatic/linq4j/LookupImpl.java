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

import net.hydromatic.linq4j.function.Function2;

import java.util.*;

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

  public Enumerator<Grouping<K, V>> enumerator() {
    return new Enumerator<Grouping<K, V>>() {
      Enumerator<Entry<K, List<V>>> enumerator = Linq4j.enumerator(
          map.entrySet());

      public Grouping<K, V> current() {
        final Entry<K, List<V>> keyAndList = enumerator.current();
        return new GroupingImpl<K, V>(keyAndList.getKey(),
            keyAndList.getValue());
      }

      public boolean moveNext() {
        return enumerator.moveNext();
      }

      public void reset() {
        enumerator.reset();
      }

      public void close() {
        enumerator.close();
      }
    };
  }

  // Map methods

  public int size() {
    return map.size();
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  public boolean containsValue(Object value) {
    @SuppressWarnings("unchecked")
    List<V> list = (List<V>) value;
    Enumerable<V> enumerable = Linq4j.asEnumerable(list);
    return map.containsValue(enumerable);
  }

  public Enumerable<V> get(Object key) {
    final List<V> list = map.get(key);
    return list == null ? null : Linq4j.asEnumerable(list);
  }

  public Enumerable<V> put(K key, Enumerable<V> value) {
    final List<V> list = map.put(key, value.toList());
    return list == null ? null : Linq4j.asEnumerable(list);
  }

  public Enumerable<V> remove(Object key) {
    final List<V> list = map.remove(key);
    return list == null ? null : Linq4j.asEnumerable(list);
  }

  public void putAll(Map<? extends K, ? extends Enumerable<V>> m) {
    for (Entry<? extends K, ? extends Enumerable<V>> entry : m.entrySet()) {
      map.put(entry.getKey(), entry.getValue().toList());
    }
  }

  public void clear() {
    map.clear();
  }

  public Set<K> keySet() {
    return map.keySet();
  }

  public Collection<Enumerable<V>> values() {
    final Collection<List<V>> lists = map.values();
    return new AbstractCollection<Enumerable<V>>() {
      public Iterator<Enumerable<V>> iterator() {
        return new Iterator<Enumerable<V>>() {
          final Iterator<List<V>> iterator = lists.iterator();

          public boolean hasNext() {
            return iterator.hasNext();
          }

          public Enumerable<V> next() {
            return Linq4j.asEnumerable(iterator.next());
          }

          public void remove() {
            iterator.remove();
          }
        };
      }

      public int size() {
        return lists.size();
      }
    };
  }

  public Set<Entry<K, Enumerable<V>>> entrySet() {
    final Set<Entry<K, List<V>>> entries = map.entrySet();
    return new AbstractSet<Entry<K, Enumerable<V>>>() {
      public Iterator<Entry<K, Enumerable<V>>> iterator() {
        final Iterator<Entry<K, List<V>>> iterator = entries.iterator();
        return new Iterator<Entry<K, Enumerable<V>>>() {
          public boolean hasNext() {
            return iterator.hasNext();
          }

          public Entry<K, Enumerable<V>> next() {
            final Entry<K, List<V>> entry = iterator.next();
            return new AbstractMap.SimpleEntry<K, Enumerable<V>>(entry.getKey(),
                Linq4j.asEnumerable(entry.getValue()));
          }

          public void remove() {
            iterator.remove();
          }
        };
      }

      public int size() {
        return entries.size();
      }
    };
  }

  public <TResult> Enumerable<TResult> applyResultSelector(
      final Function2<K, Enumerable<V>, TResult> resultSelector) {
    return new AbstractEnumerable<TResult>() {
      public Enumerator<TResult> enumerator() {
        final Enumerator<Grouping<K, V>> groupingEnumerator =
            LookupImpl.this.enumerator();
        return new Enumerator<TResult>() {
          public TResult current() {
            final Grouping<K, V> grouping = groupingEnumerator.current();
            return resultSelector.apply(grouping.getKey(), grouping);
          }

          public boolean moveNext() {
            return groupingEnumerator.moveNext();
          }

          public void reset() {
            groupingEnumerator.reset();
          }

          public void close() {
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
      public Enumerator<V> enumerator() {
        final Enumerator<Enumerable<V>> listEnumerator =
            Linq4j.iterableEnumerator(values());
        return new Enumerator<V>() {
          Enumerator<V> enumerator = Linq4j.emptyEnumerator();

          public V current() {
            return enumerator.current();
          }

          public boolean moveNext() {
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

          public void reset() {
            listEnumerator.reset();
            enumerator = Linq4j.emptyEnumerator();
          }

          public void close() {
            enumerator.close();
          }
        };
      }
    };
  }
}

// End LookupImpl.java
