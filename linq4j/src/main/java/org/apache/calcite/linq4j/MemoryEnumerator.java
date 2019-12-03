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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Enumerator that keeps some recent and some "future" values.
 *
 * @param <E> Row value
 */
public class MemoryEnumerator<E> implements Enumerator<MemoryFactory.Memory<E>> {
  private final Enumerator<E> enumerator;
  private final MemoryFactory<E> memoryFactory;
  private final AtomicInteger prevCounter;
  private final AtomicInteger postCounter;

  /**
   * Creates a MemoryEnumerator.
   *
   * <p>Use factory method {@link MemoryEnumerable#enumerator()}.
   *
   * @param enumerator The Enumerator that memory should be "wrapped" around
   * @param history Number of present steps to remember
   * @param future Number of future steps to "remember"
   */
  MemoryEnumerator(Enumerator<E> enumerator, int history, int future) {
    this.enumerator = enumerator;
    this.memoryFactory = new MemoryFactory<>(history, future);
    this.prevCounter = new AtomicInteger(future);
    this.postCounter = new AtomicInteger(future);
  }

  @Override public MemoryFactory.Memory<E> current() {
    return memoryFactory.create();
  }

  @Override public boolean moveNext() {
    if (prevCounter.get() > 0) {
      boolean lastMove = false;
      while (prevCounter.getAndDecrement() >= 0) {
        lastMove = moveNextInternal();
      }
      return lastMove;
    } else {
      return moveNextInternal();
    }
  }

  private boolean moveNextInternal() {
    final boolean moveNext = enumerator.moveNext();
    if (moveNext) {
      memoryFactory.add(enumerator.current());
      return true;
    } else {
      // Check if we have to add "history" additional values
      if (postCounter.getAndDecrement() > 0) {
        memoryFactory.add(null);
        return true;
      }
    }
    return false;
  }

  @Override public void reset() {
    enumerator.reset();
  }

  @Override public void close() {
    enumerator.close();
  }
}

// End MemoryEnumerator.java
