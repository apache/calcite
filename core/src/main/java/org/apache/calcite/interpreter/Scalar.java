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
package org.apache.calcite.interpreter;

import org.apache.calcite.DataContext;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Function;

/**
 * Compiled scalar expression.
 */
public interface Scalar {
  @Nullable Object execute(Context context);
  void execute(Context context, @Nullable Object[] results);

  /** Produces a {@link Scalar} when a query is executed.
   *
   * <p>Call {@code producer.apply(DataContext)} to get a Scalar. */
  interface Producer extends Function<DataContext, Scalar> {
  }
}
