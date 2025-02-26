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

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link JdbcType}.
 *
 * <p>It is frustrating that we can't use an {@code enum} to implement an
 * interface with a type parameter. At times like this, we wish Java had
 * Generalized Algebraic Data Types (GADTs). */
@SuppressWarnings("rawtypes")
enum JdbcTypeImpl implements JdbcType {
  BIG_DECIMAL(BigDecimal.class, false) {
    @Override public BigDecimal get(int column,
        ResultSet resultSet) throws SQLException {
      return requireNonNull(resultSet.getBigDecimal(column), "getBigDecimal");
    }
  },

  BIG_DECIMAL_NULLABLE(BigDecimal.class, true) {
    @Override public @Nullable BigDecimal get(int column,
        ResultSet resultSet) throws SQLException {
      return resultSet.getBigDecimal(column);
    }
  },

  BOOLEAN(Boolean.class, false) {
    @Override public Boolean get(int column,
        ResultSet resultSet) throws SQLException {
      boolean v = resultSet.getBoolean(column);
      checkArgument(v || !resultSet.wasNull());
      return v;
    }
  },

  BOOLEAN_NULLABLE(Boolean.class, true) {
    @Override public @Nullable Boolean get(int column,
        ResultSet resultSet) throws SQLException {
      boolean v = resultSet.getBoolean(column);
      return !v && resultSet.wasNull() ? null : v;
    }
  },

  DOUBLE(Double.class, false) {
    @Override public Double get(int column,
        ResultSet resultSet) throws SQLException {
      double v = resultSet.getDouble(column);
      checkArgument(v != 0 || !resultSet.wasNull());
      return v;
    }
  },

  DOUBLE_NULLABLE(Double.class, true) {
    @Override public @Nullable Double get(int column,
        ResultSet resultSet) throws SQLException {
      double v = resultSet.getDouble(column);
      return v == 0 && resultSet.wasNull() ? null : v;
    }
  },

  INTEGER(Integer.class, false) {
    @Override public Integer get(int column,
        ResultSet resultSet) throws SQLException {
      int v = resultSet.getInt(column);
      checkArgument(v != 0 || !resultSet.wasNull());
      return v;
    }
  },

  INTEGER_NULLABLE(Integer.class, true) {
    @Override public @Nullable Integer get(int column,
        ResultSet resultSet) throws SQLException {
      int v = resultSet.getInt(column);
      return v == 0 && resultSet.wasNull() ? null : v;
    }
  },

  STRING(String.class, false) {
    @Override public String get(int column,
        ResultSet resultSet) throws SQLException {
      return requireNonNull(resultSet.getString(column), "getString");
    }
  },

  STRING_NULLABLE(String.class, true) {
    @Override public @Nullable String get(int column,
        ResultSet resultSet) throws SQLException {
      return resultSet.getString(column);
    }
  };

  JdbcTypeImpl(Class<?> unusedClass, boolean unusedNullable) {
  }
}
