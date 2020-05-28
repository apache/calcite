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
package org.apache.calcite.schema;

import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Mix-in interface that allows you to find sub-objects.
 */
public interface Wrapper {
  /** Finds an instance of an interface implemented by this object,
   * or returns null if this object does not support that interface. */
  <C extends Object> @Nullable C unwrap(Class<C> aClass);

  /** Finds an instance of an interface implemented by this object,
   * or throws NullPointerException if this object does not support
   * that interface. */
  @API(since = "1.27", status = API.Status.INTERNAL)
  default <C extends Object> C unwrapOrThrow(Class<C> aClass) {
    return requireNonNull(unwrap(aClass),
        () -> "Can't unwrap " + aClass + " from " + this);
  }
}
