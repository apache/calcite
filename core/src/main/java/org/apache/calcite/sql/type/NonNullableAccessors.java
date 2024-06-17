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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCollation;

import org.apiguardian.api.API;

import java.nio.charset.Charset;

import static java.util.Objects.requireNonNull;

/**
 * This class provides non-nullable accessors for common getters.
 */
public class NonNullableAccessors {
  private NonNullableAccessors() {
  }

  @API(since = "1.27", status = API.Status.EXPERIMENTAL)
  public static Charset getCharset(RelDataType type) {
    return requireNonNull(type.getCharset(),
        () -> "charset is null for " + type);
  }

  @API(since = "1.27", status = API.Status.EXPERIMENTAL)
  public static SqlCollation getCollation(RelDataType type) {
    return requireNonNull(type.getCollation(),
        () -> !SqlTypeUtil.inCharFamily(type)
            ? "collation is null for " + type
            : "RelDataType object should have been assigned "
                + "a (default) collation when calling deriveType, type=" + type);
  }

  @API(since = "1.27", status = API.Status.EXPERIMENTAL)
  public static RelDataType getComponentTypeOrThrow(RelDataType type) {
    return requireNonNull(type.getComponentType(),
        () -> "componentType is null for " + type);
  }

  @API(since = "1.37", status = API.Status.EXPERIMENTAL)
  public static RelDataType getKeyTypeOrThrow(RelDataType type) {
    return requireNonNull(type.getKeyType(),
        () -> "keyType is null for " + type);
  }
}
