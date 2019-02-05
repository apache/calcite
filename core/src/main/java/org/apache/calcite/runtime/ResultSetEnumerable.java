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
package org.apache.calcite.runtime;

import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.util.Static;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * Executes a SQL statement and returns the result as an {@link Enumerable}.
 *
 * @param <T> Element type
 */
public class ResultSetEnumerable<T> extends AbstractEnumerable<T> {
  private final DataSource dataSource;
  private final String sql;
  private final Function1<ResultSet, Function0<T>> rowBuilderFactory;
  private final PreparedStatementEnricher preparedStatementEnricher;

  private static final Logger LOGGER = LoggerFactory.getLogger(
      ResultSetEnumerable.class);

  private static final Function1<ResultSet, Function0<Object>> AUTO_ROW_BUILDER_FACTORY =
      resultSet -> {
        final ResultSetMetaData metaData;
        final int columnCount;
        try {
          metaData = resultSet.getMetaData();
          columnCount = metaData.getColumnCount();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        if (columnCount == 1) {
          return () -> {
            try {
              return resultSet.getObject(1);
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          };
        } else {
          //noinspection unchecked
          return (Function0) () -> {
            try {
              final List<Object> list = new ArrayList<>();
              for (int i = 0; i < columnCount; i++) {
                if (metaData.getColumnType(i + 1) == Types.TIMESTAMP) {
                  long v = resultSet.getLong(i + 1);
                  if (v == 0 && resultSet.wasNull()) {
                    list.add(null);
                  } else {
                    list.add(v);
                  }
                } else {
                  list.add(resultSet.getObject(i + 1));
                }
              }
              return list.toArray();
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          };
        }
      };

  private ResultSetEnumerable(
      DataSource dataSource,
      String sql,
      Function1<ResultSet, Function0<T>> rowBuilderFactory,
      PreparedStatementEnricher preparedStatementEnricher) {
    this.dataSource = dataSource;
    this.sql = sql;
    this.rowBuilderFactory = rowBuilderFactory;
    this.preparedStatementEnricher = preparedStatementEnricher;
  }

  private ResultSetEnumerable(
      DataSource dataSource,
      String sql,
      Function1<ResultSet, Function0<T>> rowBuilderFactory) {
    this(dataSource, sql, rowBuilderFactory, null);
  }

  /**
   * Creates an ResultSetEnumerable.
   */
  public static Enumerable<Object> of(DataSource dataSource, String sql) {
    return of(dataSource, sql, AUTO_ROW_BUILDER_FACTORY);
  }

  /**
   * Creates an ResultSetEnumerable that retrieves columns as specific
   * Java types.
   */
  public static Enumerable<Object> of(DataSource dataSource, String sql,
                                      Primitive[] primitives) {
    return of(dataSource, sql, primitiveRowBuilderFactory(primitives));
  }

  /**
   * Executes a SQL query and returns the results as an enumerator, using a
   * row builder to convert JDBC column values into rows.
   */
  public static <T> Enumerable<T> of(
      DataSource dataSource,
      String sql,
      Function1<ResultSet, Function0<T>> rowBuilderFactory) {
    return new ResultSetEnumerable<>(dataSource, sql, rowBuilderFactory);
  }

  /**
   * Executes a SQL query and returns the results as an enumerator, using a
   * row builder to convert JDBC column values into rows.
   *
   * <p>It uses a {@link PreparedStatement} for computing the query result,
   * and that means that it can bind parameters.
   */
  public static <T> Enumerable<T> of(
      DataSource dataSource,
      String sql,
      Function1<ResultSet, Function0<T>> rowBuilderFactory,
      PreparedStatementEnricher consumer) {
    return new ResultSetEnumerable<>(dataSource, sql, rowBuilderFactory, consumer);
  }

  /**
   * Called from generated code that proposes to create a
   * {@code ResultSetEnumerable} over a prepared statement.
   */
  public static PreparedStatementEnricher createEnricher(Integer[] indexes,
                                                         DataContext context) {
    return preparedStatement -> {
      for (int i = 0; i < indexes.length; i++) {
        final int index = indexes[i];
        setDynamicParam(preparedStatement, i + 1,
            (TypedValue) context.get("?" + index));
      }
    };
  }

  /**
   * Assigns a value to a dynamic parameter in a prepared statement, calling
   * the appropriate {@code setXxx} method based on the type of the value.
   */
  private static void setDynamicParam(PreparedStatement preparedStatement,
                                      int i, TypedValue value) throws SQLException {
    if (value == null) {
      preparedStatement.setObject(i, null, SqlType.ANY.id);
      return;
    }
    switch (value.type) {
    case PRIMITIVE_BOOLEAN:
    case BOOLEAN:
      preparedStatement.setBoolean(i, (Boolean) value.value);
      break;
    case PRIMITIVE_BYTE:
    case BYTE:
      preparedStatement.setByte(i, (Byte) value.value);
      break;
    case PRIMITIVE_CHAR:
    case CHARACTER:
    case STRING:
      preparedStatement.setString(i, Objects.toString(value.value));
      break;
    case PRIMITIVE_SHORT:
    case SHORT:
      preparedStatement.setShort(i, (Short) value.value);
      break;
    case PRIMITIVE_INT:
    case INTEGER:
      preparedStatement.setInt(i, (Integer) value.value);
      break;
    case PRIMITIVE_LONG:
    case LONG:
      preparedStatement.setLong(i, (Long) value.value);
      break;
    case PRIMITIVE_FLOAT:
    case FLOAT:
      preparedStatement.setFloat(i, (Float) value.value);
      break;
    case PRIMITIVE_DOUBLE:
    case DOUBLE:
      preparedStatement.setDouble(i, (Double) value.value);
      break;
    case ARRAY:
      preparedStatement.setArray(i, (Array) value.value);
      break;
    case JAVA_SQL_TIME:
      preparedStatement.setTime(i, new Time(((Number) value.value).longValue()));
      break;
    case JAVA_SQL_TIMESTAMP:
      preparedStatement.setTimestamp(i, new Timestamp(((Number) value.value).longValue()));
      break;
    case JAVA_SQL_DATE:
    case JAVA_UTIL_DATE:
      preparedStatement.setDate(i, new Date(((Number) value.value).longValue() * 86400000L));
      break;
    case OBJECT:
      preparedStatement.setObject(i, value.value);
      break;
    }
  }

  public Enumerator<T> enumerator() {
    if (preparedStatementEnricher == null) {
      return enumeratorBasedOnStatement();
    } else {
      return enumeratorBasedOnPreparedStatement();
    }
  }

  private Enumerator<T> enumeratorBasedOnStatement() {
    Connection connection = null;
    Statement statement = null;
    try {
      connection = dataSource.getConnection();
      statement = connection.createStatement();
      setTimeoutIfPossible(statement);
      if (statement.execute(sql)) {
        final ResultSet resultSet = statement.getResultSet();
        statement = null;
        connection = null;
        return new ResultSetEnumerator<>(resultSet, rowBuilderFactory);
      } else {
        Integer updateCount = statement.getUpdateCount();
        return Linq4j.singletonEnumerator((T) updateCount);
      }
    } catch (SQLException e) {
      throw Static.RESOURCE.exceptionWhilePerformingQueryOnJdbcSubSchema(sql)
          .ex(e);
    } finally {
      closeIfPossible(connection, statement);
    }
  }

  private Enumerator<T> enumeratorBasedOnPreparedStatement() {
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    try {
      connection = dataSource.getConnection();
      preparedStatement = connection.prepareStatement(sql);
      setTimeoutIfPossible(preparedStatement);
      preparedStatementEnricher.enrich(preparedStatement);
      if (preparedStatement.execute()) {
        final ResultSet resultSet = preparedStatement.getResultSet();
        preparedStatement = null;
        connection = null;
        return new ResultSetEnumerator<>(resultSet, rowBuilderFactory);
      } else {
        Integer updateCount = preparedStatement.getUpdateCount();
        return Linq4j.singletonEnumerator((T) updateCount);
      }
    } catch (SQLException e) {
      throw Static.RESOURCE.exceptionWhilePerformingQueryOnJdbcSubSchema(sql)
          .ex(e);
    } finally {
      closeIfPossible(connection, preparedStatement);
    }
  }

  private void setTimeoutIfPossible(Statement statement) throws SQLException {
    try {
      statement.setQueryTimeout(10);
    } catch (SQLFeatureNotSupportedException e) {
      LOGGER.debug("Failed to set query timeout.");
    }
  }

  private void closeIfPossible(Connection connection, Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        // ignore
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  /**
   * Implementation of {@link Enumerator} that reads from a
   * {@link ResultSet}.
   *
   * @param <T> element type
   */
  private static class ResultSetEnumerator<T> implements Enumerator<T> {
    private final Function0<T> rowBuilder;
    private ResultSet resultSet;

    ResultSetEnumerator(
        ResultSet resultSet,
        Function1<ResultSet, Function0<T>> rowBuilderFactory) {
      this.resultSet = resultSet;
      this.rowBuilder = rowBuilderFactory.apply(resultSet);
    }

    public T current() {
      return rowBuilder.apply();
    }

    public boolean moveNext() {
      try {
        return resultSet.next();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    public void reset() {
      try {
        resultSet.beforeFirst();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    public void close() {
      ResultSet savedResultSet = resultSet;
      if (savedResultSet != null) {
        try {
          resultSet = null;
          final Statement statement = savedResultSet.getStatement();
          savedResultSet.close();
          if (statement != null) {
            final Connection connection = statement.getConnection();
            statement.close();
            if (connection != null) {
              connection.close();
            }
          }
        } catch (SQLException e) {
          // ignore
        }
      }
    }
  }

  private static Function1<ResultSet, Function0<Object>>
      primitiveRowBuilderFactory(final Primitive[] primitives) {
    return resultSet -> {
      final ResultSetMetaData metaData;
      final int columnCount;
      try {
        metaData = resultSet.getMetaData();
        columnCount = metaData.getColumnCount();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      assert columnCount == primitives.length;
      if (columnCount == 1) {
        return () -> {
          try {
            return resultSet.getObject(1);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        };
      }
      //noinspection unchecked
      return (Function0) () -> {
        try {
          final List<Object> list = new ArrayList<>();
          for (int i = 0; i < columnCount; i++) {
            list.add(primitives[i].jdbcGet(resultSet, i + 1));
          }
          return list.toArray();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      };
    };
  }

  /**
   * Consumer for decorating a {@link PreparedStatement}, that is, setting
   * its parameters.
   */
  public interface PreparedStatementEnricher {
    void enrich(PreparedStatement statement) throws SQLException;
  }
}

// End ResultSetEnumerable.java
