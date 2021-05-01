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
package org.apache.calcite.adapter.enumerable.impl;

import org.apache.calcite.adapter.enumerable.AggResultContext;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.core.AggregateCall;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of
 * {@link org.apache.calcite.adapter.enumerable.AggResultContext}.
 */
public class AggResultContextImpl extends AggResetContextImpl
    implements AggResultContext {
  private final @Nullable AggregateCall call;
  private final @Nullable ParameterExpression key;
  private final @Nullable PhysType keyPhysType;

  /**
   * Creates aggregate result context.
   *
   * @param block Code block that will contain the result calculation statements
   * @param call Aggregate call
   * @param accumulator Accumulator variables that store the intermediate
   *                    aggregate state
   * @param key Key
   */
  public AggResultContextImpl(BlockBuilder block, @Nullable AggregateCall call,
      List<Expression> accumulator, @Nullable ParameterExpression key,
      @Nullable PhysType keyPhysType) {
    super(block, accumulator);
    this.call = call; // null for AggAddContextImpl
    this.key = key;
    this.keyPhysType = keyPhysType; // null for AggAddContextImpl
  }

  @Override public @Nullable Expression key() {
    return key;
  }

  @Override public Expression keyField(int i) {
    return requireNonNull(keyPhysType, "keyPhysType")
        .fieldReference(requireNonNull(key, "key"), i);
  }

  @Override public AggregateCall call() {
    return requireNonNull(call, "call");
  }
}
