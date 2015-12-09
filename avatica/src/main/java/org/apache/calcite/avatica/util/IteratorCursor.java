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
import java.util.NoSuchElementException;

/**
 * Implementation of {@link org.apache.calcite.avatica.util.Cursor}
 * on top of an {@link Iterator} that
 * returns a record for each row. The returned record is cached to avoid
 * multiple computations of current row.
 *
 * @param <E> Element type
 */
public abstract class IteratorCursor<E> extends PositionedCursor<E> {
  private Position position = Position.BEFORE_START;
  private final Iterator<E> iterator;
  private E current = null;

  /**
   * Creates an {@code IteratorCursor}.
   *
   * @param iterator input iterator
   */
  protected IteratorCursor(Iterator<E> iterator) {
    this.iterator = iterator;
  }

  public boolean next() {
    if (iterator.hasNext()) {
      current = iterator.next();
      position = Position.OK;
      return true;
    }
    current = null;
    position = Position.AFTER_END;
    return false;
  }

  public void close() {
    current = null;
    position = Position.CLOSED;
    if (iterator instanceof AutoCloseable) {
      try {
        ((AutoCloseable) iterator).close();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected E current() {
    if (position != Position.OK) {
      throw new NoSuchElementException();
    }
    return current;
  }

  /** Are we positioned on a valid row? */
  private enum Position {
    CLOSED,
    BEFORE_START,
    OK,
    AFTER_END
  }
}

// End IteratorCursor.java
