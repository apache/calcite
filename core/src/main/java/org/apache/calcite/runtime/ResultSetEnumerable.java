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

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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

  private Long queryStart;
  private long timeout;
  private boolean timeoutSetFailed;

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

  /** Creates an ResultSetEnumerable. */
  public static ResultSetEnumerable<Object> of(DataSource dataSource, String sql) {
    return of(dataSource, sql, AUTO_ROW_BUILDER_FACTORY);
  }

  /** Creates an ResultSetEnumerable that retrieves columns as specific
   * Java types. */
  public static ResultSetEnumerable<Object> of(DataSource dataSource, String sql,
      Primitive[] primitives) {
    return of(dataSource, sql, primitiveRowBuilderFactory(primitives));
  }

  /** Executes a SQL query and returns the results as an enumerator, using a
   * row builder to convert JDBC column values into rows. */
  public static <T> ResultSetEnumerable<T> of(
      DataSource dataSource,
      String sql,
      Function1<ResultSet, Function0<T>> rowBuilderFactory) {
    return new ResultSetEnumerable<>(dataSource, sql, rowBuilderFactory);
  }

  /** Executes a SQL query and returns the results as an enumerator, using a
   * row builder to convert JDBC column values into rows.
   *
   * <p>It uses a {@link PreparedStatement} for computing the query result,
   * and that means that it can bind parameters. */
  public static <T> ResultSetEnumerable<T> of(
      DataSource dataSource,
      String sql,
      Function1<ResultSet, Function0<T>> rowBuilderFactory,
      PreparedStatementEnricher consumer) {
    return new ResultSetEnumerable<>(dataSource, sql, rowBuilderFactory, consumer);
  }

  public void setTimeout(DataContext context) {
    this.queryStart = (Long) context.get(DataContext.Variable.UTC_TIMESTAMP.camelName);
    Object timeout = context.get(DataContext.Variable.TIMEOUT.camelName);
    if (timeout instanceof Long) {
      this.timeout = (Long) timeout;
    } else {
      if (timeout != null) {
        LOGGER.debug("Variable.TIMEOUT should be `long`. Given value was {}", timeout);
      }
      this.timeout = 0;
    }
  }

  /** Called from generated code that proposes to create a
   * {@code ResultSetEnumerable} over a prepared statement. */
  public static PreparedStatementEnricher createEnricher(Integer[] indexes,
      DataContext context) {
    return preparedStatement -> {
      for (int i = 0; i < indexes.length; i++) {
        final int index = indexes[i];
        setDynamicParam(preparedStatement, i + 1,
            context.get("?" + index));
      }
    };
  }

  /** Assigns a value to a dynamic parameter in a prepared statement, calling
   * the appropriate {@code setXxx} method based on the type of the value. */
  private static void setDynamicParam(PreparedStatement preparedStatement,
      int i, Object value) throws SQLException {
    if (value == null) {
      preparedStatement.setObject(i, null, SqlType.ANY.id);
    } else if (value instanceof Timestamp) {
      preparedStatement.setTimestamp(i, (Timestamp) value);
    } else if (value instanceof Time) {
      preparedStatement.setTime(i, (Time) value);
    } else if (value instanceof String) {
      preparedStatement.setString(i, (String) value);
    } else if (value instanceof Integer) {
      preparedStatement.setInt(i, (Integer) value);
    } else if (value instanceof Double) {
      preparedStatement.setDouble(i, (Double) value);
    } else if (value instanceof java.sql.Array) {
      preparedStatement.setArray(i, (java.sql.Array) value);
    } else if (value instanceof BigDecimal) {
      preparedStatement.setBigDecimal(i, (BigDecimal) value);
    } else if (value instanceof Boolean) {
      preparedStatement.setBoolean(i, (Boolean) value);
    } else if (value instanceof Blob) {
      preparedStatement.setBlob(i, (Blob) value);
    } else if (value instanceof Byte) {
      preparedStatement.setByte(i, (Byte) value);
    } else if (value instanceof NClob) {
      preparedStatement.setNClob(i, (NClob) value);
    } else if (value instanceof Clob) {
      preparedStatement.setClob(i, (Clob) value);
    } else if (value instanceof byte[]) {
      preparedStatement.setBytes(i, (byte[]) value);
    } else if (value instanceof Date) {
      preparedStatement.setDate(i, (Date) value);
    } else if (value instanceof Float) {
      preparedStatement.setFloat(i, (Float) value);
    } else if (value instanceof Long) {
      preparedStatement.setLong(i, (Long) value);
    } else if (value instanceof Ref) {
      preparedStatement.setRef(i, (Ref) value);
    } else if (value instanceof RowId) {
      preparedStatement.setRowId(i, (RowId) value);
    } else if (value instanceof Short) {
      preparedStatement.setShort(i, (Short) value);
    } else if (value instanceof URL) {
      preparedStatement.setURL(i, (URL) value);
    } else if (value instanceof SQLXML) {
      preparedStatement.setSQLXML(i, (SQLXML) value);
    } else {
      preparedStatement.setObject(i, value);
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
    if (timeout == 0) {
      return;
    }
    long now = System.currentTimeMillis();
    long secondsLeft = (queryStart + timeout - now) / 1000;
    if (secondsLeft <= 0) {
      throw Static.RESOURCE.queryExecutionTimeoutReached(
          String.valueOf(timeout),
          String.valueOf(Instant.ofEpochMilli(queryStart))).ex();
    }
    if (secondsLeft > Integer.MAX_VALUE) {
      // Just ignore the timeout if it happens to be too big, we can't squeeze it into int
      return;
    }
    try {
      statement.setQueryTimeout((int) secondsLeft);
    } catch (SQLFeatureNotSupportedException e) {
      if (!timeoutSetFailed && LOGGER.isDebugEnabled()) {
        // We don't really want to print this again and again if enumerable is used multiple times
        LOGGER.debug("Failed to set query timeout " + secondsLeft + " seconds", e);
        timeoutSetFailed = true;
      }
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

  /** Implementation of {@link Enumerator} that reads from a
   * {@link ResultSet}.
   *
   * @param <T> element type */
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
