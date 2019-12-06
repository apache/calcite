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

/**
 * Enumerable that has a (limited) memory for n past and m future steps.
 *
 * @param <E> Type of the Enumerable items to remember
 */
public class MemoryEnumerable<E> extends AbstractEnumerable<MemoryFactory.Memory<E>> {
  private final Enumerable<E> input;
  private final int history;
  private final int future;

  /**
   * Creates a MemoryEnumerable.
   *
   * @param input The Enumerable which the memory should be "wrapped" around
   * @param history Number of present steps to remember
   * @param future Number of future steps to remember
   */
  MemoryEnumerable(Enumerable<E> input, int history, int future) {
    this.input = input;
    this.history = history;
    this.future = future;
  }

  @Override public Enumerator<MemoryFactory.Memory<E>> enumerator() {
    return new MemoryEnumerator<>(input.enumerator(), history, future);
  }

}

// End MemoryEnumerable.java
