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
package net.hydromatic.optiq.impl.jdbc;

import net.hydromatic.linq4j.expressions.Primitive;
import net.hydromatic.linq4j.function.*;

import org.eigenbase.sql.SqlDialect;
import org.eigenbase.util.IntList;
import org.eigenbase.util.Pair;
import org.eigenbase.util14.DateTimeUtil;

import com.google.common.collect.ImmutableList;

import java.sql.*;
import java.sql.Date;
import java.util.*;
import javax.sql.DataSource;

/**
 * Utilities for the JDBC provider.
 */
final class JdbcUtils {
  private JdbcUtils() {
    throw new AssertionError("no instances!");
  }

  /** Pool of dialects. */
  public static class DialectPool {
    final Map<List, SqlDialect> map = new HashMap<List, SqlDialect>();

    public static final DialectPool INSTANCE = new DialectPool();

    SqlDialect get(DataSource dataSource) {
      Connection connection = null;
      try {
        connection = dataSource.getConnection();
        DatabaseMetaData metaData = connection.getMetaData();
        String productName = metaData.getDatabaseProductName();
        String productVersion = metaData.getDatabaseProductVersion();
        List key = ImmutableList.of(productName, productVersion);
        SqlDialect dialect = map.get(key);
        if (dialect == null) {
          final SqlDialect.DatabaseProduct product =
              SqlDialect.getProduct(productName, productVersion);
          dialect =
              new SqlDialect(
                  product,
                  productName,
                  metaData.getIdentifierQuoteString());
          map.put(key, dialect);
        }
        connection.close();
        connection = null;
        return dialect;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      } finally {
        if (connection != null) {
          try {
            connection.close();
          } catch (SQLException e) {
            // ignore
          }
        }
      }
    }
  }

  /** Builder that calls {@link ResultSet#getObject(int)} for every column,
   * or {@code getXxx} if the result type is a primitive {@code xxx},
   * and returns an array of objects for each row. */
  public static class ObjectArrayRowBuilder implements Function0<Object[]> {
    private final ResultSet resultSet;
    private final int columnCount;
    private final Primitive[] primitives;
    private final int[] types;

    public ObjectArrayRowBuilder(
        ResultSet resultSet, Primitive[] primitives, int[] types)
        throws SQLException {
      this.resultSet = resultSet;
      this.primitives = primitives;
      this.types = types;
      this.columnCount = resultSet.getMetaData().getColumnCount();
    }

    public static Function1<ResultSet, Function0<Object[]>> factory(
        final List<Pair<Primitive, Integer>> list) {
      return new Function1<ResultSet, Function0<Object[]>>() {
        public Function0<Object[]> apply(ResultSet resultSet) {
          try {
            return new ObjectArrayRowBuilder(
                resultSet,
                Pair.left(list).toArray(new Primitive[list.size()]),
                IntList.toArray(Pair.right(list)));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }

    public Object[] apply() {
      try {
        final Object[] values = new Object[columnCount];
        for (int i = 0; i < columnCount; i++) {
          values[i] = value(i);
        }
        return values;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Gets a value from a given column in a JDBC result set.
     *
     * @param i Ordinal of column (1-based, per JDBC)
     */
    private Object value(int i) throws SQLException {
      // MySQL returns timestamps shifted into local time. Using
      // getTimestamp(int, Calendar) with a UTC calendar should prevent this,
      // but does not. So we shift explicitly.
      switch (types[i]) {
      case Types.TIMESTAMP:
        return shift(resultSet.getTimestamp(i + 1));
      case Types.TIME:
        return shift(resultSet.getTime(i + 1));
      case Types.DATE:
        return shift(resultSet.getDate(i + 1));
      }
      return primitives[i].jdbcGet(resultSet, i + 1);
    }

    private static Timestamp shift(Timestamp v) {
      if (v == null) {
        return null;
      }
      long time = v.getTime();
      int offset = TimeZone.getDefault().getOffset(time);
      return new Timestamp(time + offset);
    }

    private static Time shift(Time v) {
      if (v == null) {
        return null;
      }
      long time = v.getTime();
      int offset = TimeZone.getDefault().getOffset(time);
      return new Time((time + offset) % DateTimeUtil.MILLIS_PER_DAY);
    }

    private static Date shift(Date v) {
      if (v == null) {
        return null;
      }
      long time = v.getTime();
      int offset = TimeZone.getDefault().getOffset(time);
      return new Date(time + offset);
    }
  }
}

// End JdbcUtils.java
