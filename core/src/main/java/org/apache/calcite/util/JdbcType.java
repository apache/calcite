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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Maps Java types to their corresponding getters in JDBC.
 *
 * <p>In future we could add more types,
 * and add more methods
 * (e.g. {@link java.sql.PreparedStatement#setInt(int, int)}).
 *
 * @param <T> Value type
 */
@SuppressWarnings("unchecked")
public interface JdbcType<T extends @Nullable Object> {
  JdbcType<Boolean> BOOLEAN = JdbcTypeImpl.BOOLEAN;
  JdbcType<@Nullable Boolean> BOOLEAN_NULLABLE = JdbcTypeImpl.BOOLEAN_NULLABLE;
  JdbcType<BigDecimal> BIG_DECIMAL = JdbcTypeImpl.BIG_DECIMAL;
  JdbcType<@Nullable BigDecimal> BIG_DECIMAL_NULLABLE =
      JdbcTypeImpl.BIG_DECIMAL_NULLABLE;
  JdbcType<Double> DOUBLE = JdbcTypeImpl.DOUBLE;
  JdbcType<@Nullable Double> DOUBLE_NULLABLE = JdbcTypeImpl.DOUBLE_NULLABLE;
  JdbcType<Integer> INTEGER = JdbcTypeImpl.INTEGER;
  JdbcType<@Nullable Integer> INTEGER_NULLABLE = JdbcTypeImpl.INTEGER_NULLABLE;
  JdbcType<String> STRING = JdbcTypeImpl.STRING;
  JdbcType<@Nullable String> STRING_NULLABLE = JdbcTypeImpl.STRING_NULLABLE;

  /** Returns the value of column {@code column} from a JDBC result set.
   *
   * <p>For example, {@code INTEGER.get(i, resultSet)} calls
   * {@link ResultSet#getInt(int)}. */
  T get(int column, ResultSet resultSet) throws SQLException;
}
