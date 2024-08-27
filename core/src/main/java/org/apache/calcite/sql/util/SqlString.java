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
package org.apache.calcite.sql.util;

import org.apache.calcite.sql.SqlDialect;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import static java.util.Objects.requireNonNull;

/**
 * String that represents a kocher SQL statement, expression, or fragment.
 *
 * <p>A SqlString just contains a regular Java string, but the SqlString wrapper
 * indicates that the string has been created carefully guarding against all SQL
 * dialect and injection issues.
 *
 * <p>The easiest way to do build a SqlString is to use a {@link SqlBuilder}.
 */
public class SqlString {
  private final String sql;
  private final SqlDialect dialect;
  private final @Nullable ImmutableList<Integer> dynamicParameters;

  /**
   * Creates a SqlString.
   */
  public SqlString(SqlDialect dialect, String sql) {
    this(dialect, sql, ImmutableList.of());
  }

  /**
   * Creates a SqlString. The SQL might contain dynamic parameters, dynamicParameters
   * designate the order of the parameters.
   *
   * @param sql text
   * @param dynamicParameters indices
   */
  public SqlString(SqlDialect dialect, String sql,
      @Nullable ImmutableList<Integer> dynamicParameters) {
    this.dialect = requireNonNull(dialect, "dialect");
    this.sql = requireNonNull(sql, "sql");
    this.dynamicParameters = dynamicParameters;
  }

  @Override public int hashCode() {
    return sql.hashCode();
  }

  @Override public boolean equals(@Nullable Object obj) {
    return obj == this
        || obj instanceof SqlString
        && sql.equals(((SqlString) obj).sql);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns the SQL string.
   *
   * @return SQL string
   * @see #getSql()
   */
  @Override public String toString() {
    return sql;
  }

  /**
   * Returns the SQL string.
   *
   * @return SQL string
   */
  public String getSql() {
    return sql;
  }

  /**
   * Returns indices of dynamic parameters.
   *
   * @return indices of dynamic parameters
   */
  @Pure
  public @Nullable ImmutableList<Integer> getDynamicParameters() {
    return dynamicParameters;
  }

  /**
   * Returns the dialect.
   */
  public SqlDialect getDialect() {
    return dialect;
  }
}
