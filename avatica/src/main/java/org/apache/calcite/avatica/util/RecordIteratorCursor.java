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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link org.apache.calcite.avatica.util.Cursor} on top of an
 * {@link java.util.Iterator} that
 * returns a record for each row. The record is a synthetic class whose fields
 * are all public.
 *
 * @param <E> Element type
 */
public class RecordIteratorCursor<E> extends IteratorCursor<E> {
  private final List<Field> fields;

  /**
   * Creates a RecordIteratorCursor.
   *
   * @param iterator Iterator
   * @param clazz Element type
   */
  public RecordIteratorCursor(Iterator<E> iterator, Class<E> clazz) {
    this(iterator, clazz, Arrays.asList(clazz.getFields()));
  }

  /**
   * Creates a RecordIteratorCursor that projects particular fields.
   *
   * @param iterator Iterator
   * @param clazz Element type
   * @param fields Fields to project
   */
  public RecordIteratorCursor(Iterator<E> iterator, Class<E> clazz,
      List<Field> fields) {
    super(iterator);
    this.fields = fields;
  }

  protected Getter createGetter(int ordinal) {
    return new FieldGetter(fields.get(ordinal));
  }
}

// End RecordIteratorCursor.java
