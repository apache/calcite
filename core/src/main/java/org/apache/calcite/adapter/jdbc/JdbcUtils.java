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
package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import org.apache.commons.dbcp2.BasicDataSource;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.primitives.Ints;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.TimeZone;
import javax.sql.DataSource;

/**
 * Utilities for the JDBC provider.
 */
final class JdbcUtils {
  private JdbcUtils() {
    throw new AssertionError("no instances!");
  }

  /** Returns a function that, given a {@link ResultSet}, returns a function
   * that will yield successive rows from that result set. */
  static Function1<ResultSet, Function0<@Nullable Object[]>> rowBuilderFactory(
      final List<Pair<ColumnMetaData.Rep, Integer>> list) {
    ColumnMetaData.Rep[] reps =
        Pair.left(list).toArray(new ColumnMetaData.Rep[0]);
    int[] types = Ints.toArray(Pair.right(list));
    return resultSet -> new ObjectArrayRowBuilder1(resultSet, reps, types);
  }

  /** Returns a function that, given a {@link ResultSet}, returns a function
   * that will yield successive rows from that result set;
   * as {@link #rowBuilderFactory(List)} except that values are in Calcite's
   * internal format (e.g. DATE represented as int). */
  static Function1<ResultSet, Function0<@Nullable Object[]>> rowBuilderFactory2(
      final List<Pair<ColumnMetaData.Rep, Integer>> list) {
    ColumnMetaData.Rep[] reps =
        Pair.left(list).toArray(new ColumnMetaData.Rep[0]);
    int[] types = Ints.toArray(Pair.right(list));
    return resultSet -> new ObjectArrayRowBuilder2(resultSet, reps, types);
  }

  /** Pool of dialects. */
  static class DialectPool {
    public static final DialectPool INSTANCE = new DialectPool();

    private final LoadingCache<Pair<SqlDialectFactory, DataSource>, SqlDialect> cache =
        CacheBuilder.newBuilder().softValues()
            .build(CacheLoader.from(DialectPool::dialect));

    private static SqlDialect dialect(
        Pair<SqlDialectFactory, DataSource> key) {
      SqlDialectFactory dialectFactory = key.left;
      DataSource dataSource = key.right;
      Connection connection = null;
      try {
        connection = dataSource.getConnection();
        DatabaseMetaData metaData = connection.getMetaData();
        SqlDialect dialect = dialectFactory.create(metaData);
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

    public SqlDialect get(SqlDialectFactory dialectFactory, DataSource dataSource) {
      final Pair<SqlDialectFactory, DataSource> key =
          Pair.of(dialectFactory, dataSource);
      return cache.getUnchecked(key);
    }
  }

  /** Builder that calls {@link ResultSet#getObject(int)} for every column,
   * or {@code getXxx} if the result type is a primitive {@code xxx},
   * and returns an array of objects for each row. */
  abstract static class ObjectArrayRowBuilder
      implements Function0<@Nullable Object[]> {
    protected final ResultSet resultSet;
    protected final int columnCount;
    protected final ColumnMetaData.Rep[] reps;
    protected final int[] types;

    ObjectArrayRowBuilder(ResultSet resultSet, ColumnMetaData.Rep[] reps,
        int[] types) {
      this.resultSet = resultSet;
      this.reps = reps;
      this.types = types;
      try {
        this.columnCount = resultSet.getMetaData().getColumnCount();
      } catch (SQLException e) {
        throw Util.throwAsRuntime(e);
      }
    }

    @Override public @Nullable Object[] apply() {
      try {
        final @Nullable Object[] values = new Object[columnCount];
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
    protected abstract @Nullable Object value(int i) throws SQLException;

    long timestampToLong(Timestamp v) {
      return v.getTime();
    }

    long timeToLong(Time v) {
      return v.getTime();
    }

    long dateToLong(Date v) {
      return v.getTime();
    }
  }

  /** Row builder that shifts DATE, TIME, TIMESTAMP values into local time
   * zone. */
  static class ObjectArrayRowBuilder1 extends ObjectArrayRowBuilder {
    final TimeZone timeZone = TimeZone.getDefault();

    ObjectArrayRowBuilder1(ResultSet resultSet, ColumnMetaData.Rep[] reps,
        int[] types) {
      super(resultSet, reps, types);
    }

    @Override protected @Nullable Object value(int i) throws SQLException {
      // MySQL returns timestamps shifted into local time. Using
      // getTimestamp(int, Calendar) with a UTC calendar should prevent this,
      // but does not. So we shift explicitly.
      switch (types[i]) {
      case Types.TIMESTAMP:
        final Timestamp timestamp = resultSet.getTimestamp(i + 1);
        return timestamp == null ? null : new Timestamp(timestampToLong(timestamp));
      case Types.TIME:
        final Time time = resultSet.getTime(i + 1);
        return time == null ? null : new Time(timeToLong(time));
      case Types.DATE:
        final Date date = resultSet.getDate(i + 1);
        return date == null ? null : new Date(dateToLong(date));
      default:
        break;
      }
      return reps[i].jdbcGet(resultSet, i + 1);
    }

    @Override long timestampToLong(Timestamp v) {
      long time = v.getTime();
      int offset = timeZone.getOffset(time);
      return time + offset;
    }

    @Override long timeToLong(Time v) {
      long time = v.getTime();
      int offset = timeZone.getOffset(time);
      return (time + offset) % DateTimeUtils.MILLIS_PER_DAY;
    }

    @Override long dateToLong(Date v) {
      long time = v.getTime();
      int offset = timeZone.getOffset(time);
      return time + offset;
    }
  }

  /** Row builder that converts JDBC values into internal values. */
  static class ObjectArrayRowBuilder2 extends ObjectArrayRowBuilder1 {
    ObjectArrayRowBuilder2(ResultSet resultSet, ColumnMetaData.Rep[] reps,
        int[] types) {
      super(resultSet, reps, types);
    }

    @Override protected @Nullable Object value(int i) throws SQLException {
      switch (types[i]) {
      case Types.TIMESTAMP:
        final Timestamp timestamp = resultSet.getTimestamp(i + 1);
        return timestamp == null ? null : timestampToLong(timestamp);
      case Types.TIME:
        final Time time = resultSet.getTime(i + 1);
        return time == null ? null : (int) timeToLong(time);
      case Types.DATE:
        final Date date = resultSet.getDate(i + 1);
        return date == null ? null
            : (int) (dateToLong(date) / DateTimeUtils.MILLIS_PER_DAY);
      default:
        return reps[i].jdbcGet(resultSet, i + 1);
      }
    }
  }

  /** Ensures that if two data sources have the same definition, they will use
   * the same object.
   *
   * <p>This in turn makes it easier to cache
   * {@link org.apache.calcite.sql.SqlDialect} objects. Otherwise, each time we
   * see a new data source, we have to open a connection to find out what
   * database product and version it is. */
  static class DataSourcePool {
    public static final DataSourcePool INSTANCE = new DataSourcePool();

    private final LoadingCache<List<@Nullable String>, BasicDataSource> cache =
        CacheBuilder.newBuilder().softValues()
            .build(CacheLoader.from(DataSourcePool::dataSource));

    private static BasicDataSource dataSource(
          List<? extends @Nullable String> key) {
      BasicDataSource dataSource = new BasicDataSource();
      dataSource.setUrl(key.get(0));
      dataSource.setUsername(key.get(1));
      dataSource.setPassword(key.get(2));
      dataSource.setDriverClassName(key.get(3));
      return dataSource;
    }

    public DataSource get(String url, @Nullable String driverClassName,
        @Nullable String username, @Nullable String password) {
      // Get data source objects from a cache, so that we don't have to sniff
      // out what kind of database they are quite as often.
      final List<@Nullable String> key =
          ImmutableNullableList.of(url, username, password, driverClassName);
      return cache.getUnchecked(key);
    }
  }
}
