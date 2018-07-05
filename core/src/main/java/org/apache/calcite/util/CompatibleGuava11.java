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
package org.apache.calcite.util;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ForwardingSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** Helper methods to provide modern Guava functionality based on Guava 11.
 *
 * @see Compatible
 */
class CompatibleGuava11 {
  private CompatibleGuava11() {}

  public static <K, V> Map<K, V> asMap(
      Set<K> set, Function<? super K, V> function) {
    return new AsMapView<K, V>(set, function);
  }

  /**
   * {@link AbstractSet} substitute without the potentially-quadratic
   * {@code removeAll} implementation.
   *
   * @param <E> element type
   */
  abstract static class ImprovedAbstractSet<E> extends AbstractSet<E> {
    @Override public boolean removeAll(Collection<?> c) {
      return removeAllImpl(this, c);
    }

    @Override public boolean retainAll(Collection<?> c) {
      return super.retainAll(Objects.requireNonNull(c)); // GWT compatibility
    }
  }

  /**
   * Remove each element in an iterable from a set.
   */
  static boolean removeAllImpl(Set<?> set, Iterator<?> iterator) {
    boolean changed = false;
    while (iterator.hasNext()) {
      changed |= set.remove(iterator.next());
    }
    return changed;
  }

  static boolean removeAllImpl(Set<?> set, Collection<?> collection) {
    Objects.requireNonNull(collection); // for GWT
    if (collection instanceof Multiset) {
      collection = ((Multiset<?>) collection).elementSet();
    }

    // AbstractSet.removeAll(List) has quadratic behavior if the list size
    // is just less than the set's size.  We augment the test by
    // assuming that sets have fast contains() performance, and other
    // collections don't.  See
    // http://code.google.com/p/guava-libraries/issues/detail?id=1013
    if (collection instanceof Set && collection.size() > set.size()) {
      Iterator<?> setIterator = set.iterator();
      boolean changed = false;
      while (setIterator.hasNext()) {
        if (collection.contains(setIterator.next())) {
          changed = true;
          setIterator.remove();
        }
      }
      return changed;
    } else {
      return removeAllImpl(set, collection.iterator());
    }
  }

  /** ImprovedAbstractMap.
   *
   * @param <K> key type
   * @param <V> value type */
  abstract static class ImprovedAbstractMap<K, V> extends AbstractMap<K, V> {
    /**
     * Creates the entry set to be returned by {@link #entrySet()}. This method
     * is invoked at most once on a given map, at the time when {@code entrySet}
     * is first called.
     */
    protected abstract Set<Entry<K, V>> createEntrySet();

    private Set<Entry<K, V>> entrySet;

    @Override public Set<Entry<K, V>> entrySet() {
      Set<Entry<K, V>> result = entrySet;
      if (result == null) {
        entrySet = result = createEntrySet();
      }
      return result;
    }

    private Set<K> keySet;

    @Override public Set<K> keySet() {
      Set<K> result = keySet;
      if (result == null) {
        return keySet = new KeySet<K, V>() {
          @Override Map<K, V> map() {
            return ImprovedAbstractMap.this;
          }
        };
      }
      return result;
    }

    private Collection<V> values;

    @Override public Collection<V> values() {
      Collection<V> result = values;
      if (result == null) {
        return values = new Values<K, V>() {
          @Override Map<K, V> map() {
            return ImprovedAbstractMap.this;
          }
        };
      }
      return result;
    }
  }

  static <K, V> Iterator<K> keyIterator(
      Iterator<Map.Entry<K, V>> entryIterator) {
    return new TransformedIterator<Map.Entry<K, V>, K>(entryIterator) {
      @Override K transform(Map.Entry<K, V> entry) {
        return entry.getKey();
      }
    };
  }

  /** KeySet.
   *
   * @param <K> key type
   * @param <V> value type */
  abstract static class KeySet<K, V> extends ImprovedAbstractSet<K> {
    abstract Map<K, V> map();

    @Override public Iterator<K> iterator() {
      return keyIterator(map().entrySet().iterator());
    }

    @Override public int size() {
      return map().size();
    }

    @Override public boolean isEmpty() {
      return map().isEmpty();
    }

    @Override public boolean contains(Object o) {
      return map().containsKey(o);
    }

    @Override public boolean remove(Object o) {
      if (contains(o)) {
        map().remove(o);
        return true;
      }
      return false;
    }

    @Override public void clear() {
      map().clear();
    }
  }

  private static <E> Set<E> removeOnlySet(final Set<E> set) {
    return new ForwardingSet<E>() {
      @Override protected Set<E> delegate() {
        return set;
      }

      @Override public boolean add(E element) {
        throw new UnsupportedOperationException();
      }

      @Override public boolean addAll(Collection<? extends E> es) {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static <K, V> Iterator<Map.Entry<K, V>> asSetEntryIterator(
      Set<K> set, final Function<? super K, V> function) {
    return new TransformedIterator<K, Map.Entry<K, V>>(set.iterator()) {
      @Override Map.Entry<K, V> transform(K key) {
        return Maps.immutableEntry(key, function.apply(key));
      }
    };
  }

  /** AsMapView.
   *
   * @param <K> key type
   * @param <V> value type */
  private static class AsMapView<K, V> extends ImprovedAbstractMap<K, V> {
    private final Set<K> set;
    final Function<? super K, V> function;

    Set<K> backingSet() {
      return set;
    }

    AsMapView(Set<K> set, Function<? super K, V> function) {
      this.set = Objects.requireNonNull(set);
      this.function = Objects.requireNonNull(function);
    }

    @Override public Set<K> keySet() {
      // probably not worth caching
      return removeOnlySet(backingSet());
    }

    @Override public Collection<V> values() {
      // probably not worth caching
      return Collections2.transform(set, function);
    }

    @Override public int size() {
      return backingSet().size();
    }

    @Override public boolean containsKey(Object key) {
      return backingSet().contains(key);
    }

    @Override public V get(Object key) {
      if (backingSet().contains(key)) {
        @SuppressWarnings("unchecked") // unsafe, but Javadoc warns about it
            K k = (K) key;
        return function.apply(k);
      } else {
        return null;
      }
    }

    @Override public V remove(Object key) {
      if (backingSet().remove(key)) {
        @SuppressWarnings("unchecked") // unsafe, but Javadoc warns about it
            K k = (K) key;
        return function.apply(k);
      } else {
        return null;
      }
    }

    @Override public void clear() {
      backingSet().clear();
    }

    @Override protected Set<Map.Entry<K, V>> createEntrySet() {
      return new EntrySet<K, V>() {
        @Override Map<K, V> map() {
          return AsMapView.this;
        }

        @Override public Iterator<Map.Entry<K, V>> iterator() {
          return asSetEntryIterator(backingSet(), function);
        }
      };
    }
  }

  /** EntrySet.
   *
   * @param <K> key type
   * @param <V> value type */
  abstract static class EntrySet<K, V>
      extends ImprovedAbstractSet<Map.Entry<K, V>> {
    abstract Map<K, V> map();

    @Override public int size() {
      return map().size();
    }

    @Override public void clear() {
      map().clear();
    }

    @Override public boolean contains(Object o) {
      if (o instanceof Map.Entry) {
        Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
        Object key = entry.getKey();
        V value = map().get(key);
        return Objects.equals(value, entry.getValue())
            && (value != null || map().containsKey(key));
      }
      return false;
    }

    @Override public boolean isEmpty() {
      return map().isEmpty();
    }

    @Override public boolean remove(Object o) {
      if (contains(o)) {
        Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
        return map().keySet().remove(entry.getKey());
      }
      return false;
    }

    @Override public boolean removeAll(Collection<?> c) {
      try {
        return super.removeAll(Objects.requireNonNull(c));
      } catch (UnsupportedOperationException e) {
        // if the iterators don't support remove
        boolean changed = true;
        for (Object o : c) {
          changed |= remove(o);
        }
        return changed;
      }
    }

    @Override public boolean retainAll(Collection<?> c) {
      try {
        return super.retainAll(Objects.requireNonNull(c));
      } catch (UnsupportedOperationException e) {
        // if the iterators don't support remove
        Set<Object> keys = Sets.newHashSetWithExpectedSize(c.size());
        for (Object o : c) {
          if (contains(o)) {
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
            keys.add(entry.getKey());
          }
        }
        return map().keySet().retainAll(keys);
      }
    }
  }

  static <K, V> Iterator<V> valueIterator(
      Iterator<Map.Entry<K, V>> entryIterator) {
    return new TransformedIterator<Map.Entry<K, V>, V>(entryIterator) {
      @Override V transform(Map.Entry<K, V> entry) {
        return entry.getValue();
      }
    };
  }

  /** Values.
   *
   * @param <K> key type
   * @param <V> value type */
  abstract static class Values<K, V> extends AbstractCollection<V> {
    abstract Map<K, V> map();

    @Override public Iterator<V> iterator() {
      return valueIterator(map().entrySet().iterator());
    }

    @Override public boolean remove(Object o) {
      try {
        return super.remove(o);
      } catch (UnsupportedOperationException e) {
        for (Map.Entry<K, V> entry : map().entrySet()) {
          if (Objects.equals(o, entry.getValue())) {
            map().remove(entry.getKey());
            return true;
          }
        }
        return false;
      }
    }

    @Override public boolean removeAll(Collection<?> c) {
      try {
        return super.removeAll(Objects.requireNonNull(c));
      } catch (UnsupportedOperationException e) {
        Set<K> toRemove = new HashSet<>();
        for (Map.Entry<K, V> entry : map().entrySet()) {
          if (c.contains(entry.getValue())) {
            toRemove.add(entry.getKey());
          }
        }
        return map().keySet().removeAll(toRemove);
      }
    }

    @Override public boolean retainAll(Collection<?> c) {
      try {
        return super.retainAll(Objects.requireNonNull(c));
      } catch (UnsupportedOperationException e) {
        Set<K> toRetain = new HashSet<>();
        for (Map.Entry<K, V> entry : map().entrySet()) {
          if (c.contains(entry.getValue())) {
            toRetain.add(entry.getKey());
          }
        }
        return map().keySet().retainAll(toRetain);
      }
    }

    @Override public int size() {
      return map().size();
    }

    @Override public boolean isEmpty() {
      return map().isEmpty();
    }

    @Override public boolean contains(Object o) {
      return map().containsValue(o);
    }

    @Override public void clear() {
      map().clear();
    }
  }

  /** TransformedIterator.
   *
   * @param <F> from type
   * @param <T> to type */
  abstract static class TransformedIterator<F, T> implements Iterator<T> {
    final Iterator<? extends F> backingIterator;

    TransformedIterator(Iterator<? extends F> backingIterator) {
      this.backingIterator = Objects.requireNonNull(backingIterator);
    }

    abstract T transform(F from);

    public final boolean hasNext() {
      return backingIterator.hasNext();
    }

    public final T next() {
      return transform(backingIterator.next());
    }

    public final void remove() {
      backingIterator.remove();
    }
  }

  /** Implements {@link Compatible#navigableSet}. */
  static <E> NavigableSet<E> navigableSet(ImmutableSortedSet<E> set) {
    if (set instanceof NavigableSet) {
      // In Guava 12 and later, ImmutableSortedSet implements NavigableSet.
      //noinspection unchecked
      return (NavigableSet) set;
    } else {
      // In Guava 11, we have to make a copy.
      return new TreeSet<E>(set);
    }
  }

  /** Implements {@link Compatible#navigableMap}. */
  static <K, V> NavigableMap<K, V> navigableMap(ImmutableSortedMap<K, V> map) {
    if (map instanceof NavigableMap) {
      // In Guava 12 and later, ImmutableSortedMap implements NavigableMap.
      //noinspection unchecked
      return (NavigableMap) map;
    } else {
      // In Guava 11, we have to make a copy.
      return new TreeMap<K, V>(map);
    }
  }
}

// End CompatibleGuava11.java
