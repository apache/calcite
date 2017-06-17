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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Helper that holds onto {@link AutoCloseable} resources and releases them
 * when its {@code #close} method is called.
 *
 * <p>Similar to {@code com.google.common.io.Closer} but can deal with
 * {@link AutoCloseable}, and doesn't throw {@link IOException}. */
public final class Closer implements AutoCloseable {
  private final List<AutoCloseable> list = new ArrayList<>();

  /** Registers a resource. */
  public <E extends AutoCloseable> E add(E e) {
    list.add(e);
    return e;
  }

  public void close() {
    for (AutoCloseable closeable : list) {
      try {
        closeable.close();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}

// End Closer.java
