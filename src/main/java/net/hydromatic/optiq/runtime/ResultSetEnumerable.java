/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.runtime;

import net.hydromatic.linq4j.AbstractEnumerable;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.function.Function0;
import net.hydromatic.linq4j.function.Function1;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/**
 * Executes a SQL statement and returns the result as an {@link Enumerable}.
 */
public class ResultSetEnumerable<T> extends AbstractEnumerable<T> {
  private final DataSource dataSource;
  private final String sql;
  private final Function1<ResultSet, Function0<T>> rowBuilderFactory;

  private static final Function1<ResultSet, Function0<Object[]>>
      ARRAY_ROW_BUILDER_FACTORY =
      new Function1<ResultSet, Function0<Object[]>>() {
        public Function0<Object[]> apply(final ResultSet resultSet) {
          final int columnCount;
          try {
            columnCount = resultSet.getMetaData().getColumnCount();
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
          return new Function0<Object[]>() {
            public Object[] apply() {
              try {
                final List<Object> list = new ArrayList<Object>();
                for (int i = 0; i < columnCount; i++) {
                  list.add(resultSet.getObject(i + 1));
                }
                return list.toArray();
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          };
        }
      };

  private ResultSetEnumerable(
      DataSource dataSource,
      String sql,
      Function1<ResultSet, Function0<T>> rowBuilderFactory) {
    this.dataSource = dataSource;
    this.sql = sql;
    this.rowBuilderFactory = rowBuilderFactory;
  }

  /** Creates an ResultSetEnumerable. */
  public static Enumerable<Object[]> of(
      DataSource dataSource, String sql) {
    return of(dataSource, sql, ARRAY_ROW_BUILDER_FACTORY);
  }

  /** Executes a SQL query and returns the results as an enumerator. The
   * parameterization not withstanding, the result type must be an array of
   * objects. */
  public static <T> Enumerable<T> of(
      DataSource dataSource,
      String sql,
      Function1<ResultSet, Function0<T>> rowBuilderFactory) {
    return new ResultSetEnumerable<T>(dataSource, sql, rowBuilderFactory);
  }

  public Enumerator<T> enumerator() {
    Connection connection = null;
    Statement statement = null;
    try {
      connection = dataSource.getConnection();
      statement = connection.createStatement();
      final ResultSet resultSet = statement.executeQuery(sql);
      statement = null;
      connection = null;
      return new ResultSetEnumerator<T>(resultSet, rowBuilderFactory);
    } catch (SQLException e) {
      throw new RuntimeException("while executing SQL [" + sql + "]", e);
    } finally {
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
  }

  private static class ResultSetEnumerator<T> implements Enumerator<T> {
    private final Function0<T> rowBuilder;
    private ResultSet resultSet;

    public ResultSetEnumerator(
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

    // TODO: use this
    public void close() {
      if (resultSet != null) {
        try {
          ResultSet savedResultSet = resultSet;
          resultSet = null;
          savedResultSet.close();
          savedResultSet.getStatement().close();
          savedResultSet.getStatement().getConnection().close();
        } catch (SQLException e) {
          // ignore
        }
      }
    }
  }
}

// End ResultSetEnumerable.java
