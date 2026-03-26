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
package org.apache.calcite.adapter.arrow;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A structured representation of a single Gandiva predicate condition.
 *
 * <p>A condition is either unary (e.g. {@code IS NULL}) or binary
 * (e.g. {@code =}, {@code <}). Unary conditions have a field name
 * and operator; binary conditions additionally have a literal value
 * and its type.
 *
 * @see ArrowTranslator
 */
class ConditionToken {
  final String fieldName;
  final String operator;
  final @Nullable String value;
  final @Nullable String valueType;

  private ConditionToken(String fieldName, String operator,
      @Nullable String value, @Nullable String valueType) {
    this.fieldName = requireNonNull(fieldName, "fieldName");
    this.operator = requireNonNull(operator, "operator");
    this.value = value;
    this.valueType = valueType;
  }

  /** Creates a binary condition token
   * (e.g. {@code intField equal 12 integer}). */
  static ConditionToken binary(String fieldName, String operator,
      String value, String valueType) {
    return new ConditionToken(fieldName, operator,
        requireNonNull(value, "value"),
        requireNonNull(valueType, "valueType"));
  }

  /** Creates a unary condition token
   * (e.g. {@code intField isnull}). */
  static ConditionToken unary(String fieldName, String operator) {
    return new ConditionToken(fieldName, operator, null, null);
  }

  /** Returns whether this is a binary condition. */
  boolean isBinary() {
    return value != null;
  }

  /** Converts this token to a string list for serialization
   * through code generation.
   *
   * <p>The result is either {@code [fieldName, operator]} for unary
   * conditions or {@code [fieldName, operator, value, valueType]} for
   * binary conditions. */
  List<String> toTokenList() {
    if (isBinary()) {
      return ImmutableList.of(fieldName, operator,
          requireNonNull(value, "value"),
          requireNonNull(valueType, "valueType"));
    }
    return ImmutableList.of(fieldName, operator);
  }

  /** Creates a {@code ConditionToken} from a serialized string list. */
  static ConditionToken fromTokenList(List<String> tokens) {
    final int size = tokens.size();
    if (size == 4) {
      return binary(tokens.get(0), tokens.get(1),
          tokens.get(2), tokens.get(3));
    } else if (size == 2) {
      return unary(tokens.get(0), tokens.get(1));
    }
    throw new IllegalArgumentException("Invalid condition tokens: " + tokens);
  }
}
