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
package net.hydromatic.optiq.runtime;

import net.hydromatic.linq4j.Enumerator;

/**
 * Implementation of {@link net.hydromatic.avatica.Cursor} on top of an
 * {@link net.hydromatic.linq4j.Enumerator} that
 * returns a record for each row. The returned record is cached to avoid
 * multiple computations of current row.
 * For instance,
 * {@link net.hydromatic.optiq.rules.java.JavaRules.EnumerableCalcRel}
 * computes result just in {@code current()} method, thus it makes sense to
 * cache the result and make it available for all the accesors.
 *
 * @param <T> Element type
 */
public abstract class EnumeratorCursor<T> extends AbstractCursor {
  private final Enumerator<T> enumerator;
  private T current;

  /**
   * Creates a {@code EnumeratorCursor}
   * @param enumerator input enumerator
   */
  protected EnumeratorCursor(Enumerator<T> enumerator) {
    this.enumerator = enumerator;
  }

  public boolean next() {
    if (enumerator.moveNext()) {
      current = enumerator.current();
      return true;
    }
    current = null;
    return false;
  }

  public void close() {
    current = null;
    enumerator.close();
  }

  /**
   * Returns current row.
   * @return current row
   */
  protected T current() {
    return current;
  }
}

// End EnumeratorCursor.java
