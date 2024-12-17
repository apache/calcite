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
package org.apache.calcite.util;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * This class can is able to do a lazy initiaization
 * of an object based on a {@code Supplier}.
 *
 * <p>In contrast to {@code Suppliers.memoize}, the supplier is passed
 * at the later point in time, during the call to {@code getOrCompute}.
 * This allows the usage in abstract base classes with a supplier
 * method, implemented in a derived class.
 *
 * @param <T> Element Type
 */
public class LazyReference<T> {

  private final AtomicReference<T> value = new AtomicReference<>();

  /**
   * Atomically sets the value to {@code supplier.get()}
   * if the current value was not set yet.
   *
   * <p>This method is reentrant. Different threads will
   * get the same result.
   *
   * @param supplier supplier for the new value
   * @return the current value.
   */
  public T getOrCompute(Supplier<T> supplier) {
    while (true) {
      T result = value.get();
      if (result != null) {
        return result;
      }
      T computed = supplier.get();
      if (value.compareAndSet((T) null, computed)) {
        return computed;
      }
    }
  }

  /**
   * Resets the current value.
   */
  public void reset() {
    value.set((T) null);
  }
}
