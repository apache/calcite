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

import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link Cursor} on top of an
 * {@link java.util.Iterator} that
 * returns a {@link List} for each row.
 */
public class ListIteratorCursor extends IteratorCursor<List<Object>> {

  /**
   * Creates a RecordEnumeratorCursor.
   *
   * @param iterator Iterator
   */
  public ListIteratorCursor(Iterator<List<Object>> iterator) {
    super(iterator);
  }

  protected Getter createGetter(int ordinal) {
    return new ListGetter(ordinal);
  }
}

// End ListIteratorCursor.java
