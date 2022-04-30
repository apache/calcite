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
package org.apache.calcite.rel.externalize;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlInsertKeyword;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlJsonQueryWrapperBehavior;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.fun.SqlTrimFunction;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/** Registry of {@link Enum} classes that can be serialized to JSON.
 *
 * <p>Suppose you want to serialize the value
 * {@link SqlTrimFunction.Flag#LEADING} to JSON.
 * First, make sure that {@link SqlTrimFunction.Flag} is registered.
 * The type will be serialized as "SYMBOL".
 * The value will be serialized as the string "LEADING".
 *
 * <p>When we deserialize, we rely on the fact that the registered
 * {@code enum} classes have distinct values. Therefore, knowing that
 * {@code (type="SYMBOL", value="LEADING")} we can convert the string "LEADING"
 * to the enum {@code Flag.LEADING}. */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class RelEnumTypes {
  private RelEnumTypes() {}

  private static final ImmutableMap<String, Enum<?>> ENUM_BY_NAME;

  static {
    // Build a mapping from enum constants (e.g. LEADING) to the enum
    // that contains them (e.g. SqlTrimFunction.Flag). If there two
    // enum constants have the same name, the builder will throw.
    final ImmutableMap.Builder<String, Enum<?>> enumByName =
        ImmutableMap.builder();
    register(enumByName, JoinConditionType.class);
    register(enumByName, JoinType.class);
    register(enumByName, SqlExplain.Depth.class);
    register(enumByName, SqlExplainFormat.class);
    register(enumByName, SqlExplainLevel.class);
    register(enumByName, SqlInsertKeyword.class);
    register(enumByName, SqlJsonConstructorNullClause.class);
    register(enumByName, SqlJsonQueryWrapperBehavior.class);
    register(enumByName, SqlJsonValueEmptyOrErrorBehavior.class);
    register(enumByName, SqlMatchRecognize.AfterOption.class);
    register(enumByName, SqlSelectKeyword.class);
    register(enumByName, SqlTrimFunction.Flag.class);
    register(enumByName, TimeUnitRange.class);
    register(enumByName, TableModify.Operation.class);
    ENUM_BY_NAME = enumByName.build();
  }

  private static void register(ImmutableMap.Builder<String, Enum<?>> builder,
      Class<? extends Enum> aClass) {
    for (Enum enumConstant : castNonNull(aClass.getEnumConstants())) {
      builder.put(enumConstant.name(), enumConstant);
    }
  }

  /** Converts a literal into a value that can be serialized to JSON.
   * In particular, if is an enum, converts it to its name. */
  public static @Nullable Object fromEnum(@Nullable Object value) {
    return value instanceof Enum ? fromEnum((Enum) value) : value;
  }

  /** Converts an enum into its name.
   * Throws if the enum's class is not registered. */
  public static String fromEnum(Enum enumValue) {
    if (ENUM_BY_NAME.get(enumValue.name()) != enumValue) {
      throw new AssertionError("cannot serialize enum value to JSON: "
          + enumValue.getDeclaringClass().getCanonicalName() + "."
          + enumValue);
    }
    return enumValue.name();
  }

  /** Converts a string to an enum value.
   * The converse of {@link #fromEnum(Enum)}.
   *
   * @throws NullPointerException if there is no corresponding registered {@link Enum}
   * */
  static <E extends Enum<E>> @NonNull E toEnum(String name) {
    return (E) requireNonNull(ENUM_BY_NAME.get(name));
  }
}
