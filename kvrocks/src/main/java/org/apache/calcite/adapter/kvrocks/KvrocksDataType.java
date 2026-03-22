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
package org.apache.calcite.adapter.kvrocks;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Data types supported by Kvrocks.
 *
 * <p>Kvrocks supports all classic Redis data types plus additional types
 * such as JSON, Stream, and Bloom Filter.
 */
public enum KvrocksDataType {

  /** Basic key-value string. */
  STRING("string"),

  /** Field-value map. */
  HASH("hash"),

  /** Ordered list of strings. */
  LIST("list"),

  /** Unordered collection of unique strings. */
  SET("set"),

  /** Scored, sorted collection of unique strings. */
  SORTED_SET("zset"),

  /** Append-only log with consumer group support (since Kvrocks 2.2). */
  STREAM("stream"),

  /** Native JSON document storage (since Kvrocks 2.7). */
  JSON("ReJSON-RL"),

  /** Probabilistic membership filter (since Kvrocks 2.6). */
  BLOOM("MBbloom--");

  private final String typeName;

  KvrocksDataType(String typeName) {
    this.typeName = typeName;
  }

  /** Resolves a Kvrocks {@code TYPE} command response to an enum value. */
  public static @Nullable KvrocksDataType fromTypeName(String typeName) {
    for (KvrocksDataType type : values()) {
      if (type.typeName.equals(typeName)) {
        return type;
      }
    }
    return null;
  }

  public String getTypeName() {
    return typeName;
  }
}
