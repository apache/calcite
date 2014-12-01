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
package org.apache.calcite.runtime;

import org.apache.calcite.linq4j.Enumerator;

/**
 * Implementation of {@link org.apache.calcite.avatica.Cursor} on top of an
 * {@link org.apache.calcite.linq4j.Enumerator} that
 * returns an array of {@link Object} for each row.
 */
public class ArrayEnumeratorCursor extends EnumeratorCursor<Object[]> {
  /**
   * Creates an ArrayEnumeratorCursor.
   *
   * @param enumerator Enumerator
   */
  public ArrayEnumeratorCursor(Enumerator<Object[]> enumerator) {
    super(enumerator);
  }

  protected Getter createGetter(int ordinal) {
    return new ArrayEnumeratorGetter(ordinal);
  }

  /** Implementation of {@link Getter} that reads from records that are
   * arrays. */
  class ArrayEnumeratorGetter extends AbstractGetter {
    protected final int field;

    public ArrayEnumeratorGetter(int field) {
      this.field = field;
    }

    public Object getObject() {
      Object o = current()[field];
      wasNull[0] = o == null;
      return o;
    }
  }
}

// End ArrayEnumeratorCursor.java
