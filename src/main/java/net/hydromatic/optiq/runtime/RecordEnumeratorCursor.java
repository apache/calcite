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
package net.hydromatic.optiq.runtime;

import net.hydromatic.linq4j.Enumerator;

import java.lang.reflect.Field;

/**
 * Implementation of {@link net.hydromatic.optiq.runtime.Cursor} on top of an
 * {@link net.hydromatic.linq4j.Enumerator} that
 * returns a record for each row. The record is a synthetic class whose fields
 * are all public.
 */
public class RecordEnumeratorCursor<E> extends AbstractCursor {
  private final Enumerator<E> enumerator;
  private final Class<E> clazz;

  /**
   * Creates a RecordEnumeratorCursor.
   *
   * @param enumerator Enumerator
   * @param clazz Element type
   */
  public RecordEnumeratorCursor(
      Enumerator<E> enumerator,
      Class<E> clazz) {
    this.enumerator = enumerator;
    this.clazz = clazz;
  }

  @Override
  protected Getter createGetter(int ordinal) {
    return new RecordEnumeratorGetter(clazz.getFields()[ordinal]);
  }

  @Override
  public boolean next() {
    return enumerator.moveNext();
  }

  class RecordEnumeratorGetter implements Getter {
    protected final Field field;

    public RecordEnumeratorGetter(Field field) {
      this.field = field;
    }

    public Object getObject() {
      Object o;
      try {
        o = field.get(enumerator.current());
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      wasNull[0] = (o == null);
      return o;
    }

    public boolean wasNull() {
      return wasNull[0];
    }
  }
}

// End RecordEnumeratorCursor.java
