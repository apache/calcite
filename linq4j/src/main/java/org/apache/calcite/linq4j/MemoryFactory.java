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
package org.apache.calcite.linq4j;

import java.util.Arrays;

/**
 * Contains the State and changes internally.
 * with the {@link #create()} method one can get immutable Snapshots.
 * @param <E> Type of the base Object
 */
public class MemoryFactory<E> {

  private final int history;
  private final int future;
  // Index:      0   1   2   3   4
  // Idea       -2  -1   0  +1  +2
  ModularInteger offset;
  private Object[] values;

  public MemoryFactory(int history, int future) {
    this.history = history;
    this.future = future;
    this.values = new Object[history + future + 1];
    this.offset = new ModularInteger(0, history + future + 1);
  }

  public void add(E current) {
    values[offset.get()] = current;
    this.offset = offset.plus(1);
  }

  public Memory<E> create() {
    return new Memory<>(history, future, offset, values.clone());
  }

  /**
   * Contents of a "memory segment", used for implementing the
   * {@code MATCH_RECOGNIZE} operator.
   *
   * <p>Memory maintains a "window" of records preceding and following a record;
   * the records can be browsed using the {@link #get()} or {@link #get(int)}
   * methods.
   *
   * @param <E> Row type
   */
  public static class Memory<E> {
    private final int history;
    private final int future;
    private final ModularInteger offset;
    private final Object[] values;

    public Memory(int history, int future,
        ModularInteger offset, Object[] values) {
      this.history = history;
      this.future = future;
      this.offset = offset;
      this.values = values;
    }

    @Override public String toString() {
      return Arrays.toString(this.values);
    }

    public E get() {
      return get(0);
    }

    public E get(int position) {
      if (position < 0 && position < -1 * history) {
        throw new IllegalArgumentException("History can only go back " + history
            + " points in time, you wanted " + Math.abs(position));
      }
      if (position > 0 && position > future) {
        throw new IllegalArgumentException("Future can only see next " + future
            + " points in time, you wanted " + position);
      }
      return (E) this.values[this.offset.plus(position - 1 - future).get()];
    }
  }
}

// End MemoryFactory.java
