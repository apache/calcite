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
package org.eigenbase.util;

import java.util.*;

/**
 * Map which contains more than one value per key.
 *
 * <p>You can either use a <code>MultiMap</code> as a regular map, or you can
 * use the additional methods {@link #putMulti} and {@link #getMulti}. Values
 * are returned in the order in which they were added.</p>
 *
 * <p>TODO jvs 21-Jul-2007: unit test for this class
 */
public class MultiMap<K, V>
{
    //~ Instance fields --------------------------------------------------------

    private final Map<K, Object> map = new HashMap<K, Object>();

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the number of keys in this MultiMap.
     *
     * @return number of keys in this MultiMap
     */
    public int size()
    {
        return map.size();
    }

    private Object get(K key)
    {
        return map.get(key);
    }

    private Object put(K key, V value)
    {
        return map.put(key, value);
    }

    /**
     * Returns a list of values for a given key; returns an empty list if not
     * found.
     *
     * @post return != null
     */
    public List<V> getMulti(K key)
    {
        Object o = get(key);
        if (o == null) {
            return Collections.emptyList();
        } else if (o instanceof ValueList) {
            return (ValueList<V>) o;
        } else {
            // FIXME jvs 21-Jul-2007:  This list is immutable, meaning callers
            // have to avoid deleting from it.  That's inconsistent with
            // ValueList, which goes to the effort to support deletion.
            return Collections.singletonList((V) o);
        }
    }

    /**
     * Adds a value for this key.
     */
    public void putMulti(
        K key,
        V value)
    {
        final Object o = put(key, value);
        if (o != null) {
            // We knocked something out. It might be a list, or a singleton
            // object.
            ValueList<V> list;
            if (o instanceof ValueList) {
                list = (ValueList<V>) o;
            } else {
                list = new ValueList<V>();
                list.add((V) o);
            }
            list.add(value);
            map.put(key, list);
        }
    }

    /**
     * Removes a value for this key.
     */
    public boolean removeMulti(
        K key,
        V value)
    {
        final Object o = get(key);
        if (o == null) {
            // key not found, so nothing changed
            return false;
        } else {
            if (o instanceof ValueList) {
                ValueList<V> list = (ValueList<V>) o;
                if (list.remove(value)) {
                    if (list.size() == 1) {
                        // now just one value left, so forget the list, and
                        // keep its only element
                        put(
                            key,
                            list.get(0));
                    } else if (list.isEmpty()) {
                        // have just removed the last value belonging to this
                        // key, so remove the key
                        remove(key);
                    }
                    return true;
                } else {
                    // nothing changed
                    return false;
                }
            } else {
                if (o.equals(value)) {
                    // have just removed the last value belonging to this key,
                    // so remove the key.
                    remove(key);
                    return true;
                } else {
                    // the value they asked to remove was not the one present,
                    // so nothing changed
                    return false;
                }
            }
        }
    }

    /**
     * Like entrySet().iterator(), but returns one Map.Entry per value rather
     * than one per key.
     */
    public EntryIter entryIterMulti()
    {
        return new EntryIter();
    }

    public Object remove(K key)
    {
        return map.remove(key);
    }

    public boolean containsKey(K key)
    {
        return map.containsKey(key);
    }

    public void clear()
    {
        map.clear();
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Holder class, ensures that user's values are never interpreted as
     * multiple values.
     */
    private static class ValueList<V>
        extends ArrayList<V>
    {
    }

    /**
     * Implementation for entryIterMulti(). Note that this assumes that empty
     * ValueLists will never be encountered, and also preserves this property
     * when remove() is called.
     */
    private class EntryIter
        implements Iterator<Map.Entry<K, V>>
    {
        K key;
        Iterator<K> keyIter;
        List<V> valueList;
        Iterator<V> valueIter;

        EntryIter()
        {
            keyIter = map.keySet().iterator();
            if (keyIter.hasNext()) {
                nextKey();
            } else {
                valueList = Collections.emptyList();
                valueIter = valueList.iterator();
            }
        }

        private void nextKey()
        {
            key = keyIter.next();
            valueList = getMulti(key);
            valueIter = valueList.iterator();
        }

        public boolean hasNext()
        {
            return keyIter.hasNext() || valueIter.hasNext();
        }

        public Map.Entry<K, V> next()
        {
            if (!valueIter.hasNext()) {
                nextKey();
            }
            final K savedKey = key;
            final V value = valueIter.next();
            return new Map.Entry<K, V>() {
                public K getKey()
                {
                    return savedKey;
                }

                public V getValue()
                {
                    return value;
                }

                public boolean equals(Object o)
                {
                    throw new UnsupportedOperationException();
                }

                public int hashCode()
                {
                    throw new UnsupportedOperationException();
                }

                public V setValue(V value)
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        public void remove()
        {
            if (valueList instanceof ValueList) {
                valueIter.remove();
                if (valueList.isEmpty()) {
                    keyIter.remove();
                }
            } else {
                keyIter.remove();
            }
        }
    }
}

// End MultiMap.java
