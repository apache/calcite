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
package org.apache.calcite.avatica.util;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

/**
 * Abstract implementation of {@link org.apache.calcite.avatica.util.Cursor}
 * that caches its current row.
 *
 * @param <T> Element type
 */
public abstract class PositionedCursor<T> extends AbstractCursor {
  /**
   * Returns the current row.
   *
   * @return current row
   *
   * @throws java.util.NoSuchElementException if the iteration has no more
   * elements
   */
  protected abstract T current();

  /** Implementation of
   * {@link org.apache.calcite.avatica.util.AbstractCursor.Getter}
   * that reads from records that are arrays. */
  protected class ArrayGetter extends AbstractGetter {
    protected final int field;

    public ArrayGetter(int field) {
      this.field = field;
    }

    public Object getObject() {
      Object o = ((Object[]) current())[field];
      wasNull[0] = o == null;
      return o;
    }
  }

  /** Implementation of
   * {@link org.apache.calcite.avatica.util.AbstractCursor.Getter}
   * that reads items from a list. */
  protected class ListGetter extends AbstractGetter {
    protected final int index;

    public ListGetter(int index) {
      this.index = index;
    }

    public Object getObject() {
      Object o = ((List) current()).get(index);
      wasNull[0] = o == null;
      return o;
    }
  }

  /** Implementation of
   * {@link org.apache.calcite.avatica.util.AbstractCursor.Getter}
   * for records that consist of a single field.
   *
   * <p>Each record is represented as an object, and the value of the sole
   * field is that object. */
  protected class ObjectGetter extends AbstractGetter {
    public ObjectGetter(int field) {
      assert field == 0;
    }

    public Object getObject() {
      Object o = current();
      wasNull[0] = o == null;
      return o;
    }
  }

  /** Implementation of
   * {@link org.apache.calcite.avatica.util.AbstractCursor.Getter}
   * that reads fields via reflection. */
  protected class FieldGetter extends AbstractGetter {
    protected final Field field;

    public FieldGetter(Field field) {
      this.field = field;
    }

    public Object getObject() {
      Object o;
      try {
        o = field.get(current());
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      wasNull[0] = o == null;
      return o;
    }
  }

  /** Implementation of
   * {@link org.apache.calcite.avatica.util.AbstractCursor.Getter}
   * that reads entries from a {@link java.util.Map}. */
  protected class MapGetter<K> extends AbstractGetter {
    protected final K key;

    public MapGetter(K key) {
      this.key = key;
    }

    public Object getObject() {
      @SuppressWarnings("unchecked") final Map<K, Object> map =
          (Map<K, Object>) current();
      Object o = map.get(key);
      wasNull[0] = o == null;
      return o;
    }
  }
}

// End PositionedCursor.java
