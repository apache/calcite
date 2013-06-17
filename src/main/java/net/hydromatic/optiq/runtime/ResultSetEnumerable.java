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
import net.hydromatic.linq4j.expressions.Primitive;
import net.hydromatic.linq4j.function.Function0;
import net.hydromatic.linq4j.function.Function1;

import java.sql.*;
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

  private static final Function1<ResultSet, Function0<Object>>
      AUTO_ROW_BUILDER_FACTORY =
      new Function1<ResultSet, Function0<Object>>() {
        public Function0<Object> apply(final ResultSet resultSet) {
          final ResultSetMetaData metaData;
          final int columnCount;
          try {
            metaData = resultSet.getMetaData();
            columnCount = metaData.getColumnCount();
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
          if (columnCount == 1) {
            return new Function0<Object>() {
              public Object apply() {
                try {
                  return resultSet.getObject(1);
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            };
          } else {
            //noinspection unchecked
            return (Function0) new Function0<Object[]>() {
              public Object[] apply() {
                try {
                  final List<Object> list = new ArrayList<Object>();
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
              }
            };
          }
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
  public static Enumerable<Object> of(DataSource dataSource, String sql) {
    return of(dataSource, sql, AUTO_ROW_BUILDER_FACTORY);
  }

  /** Creates an ResultSetEnumerable that retrieves columns as specific
   * Java types. */
  public static Enumerable<Object> of(DataSource dataSource, String sql,
      Primitive[] primitives) {
    return of(dataSource, sql, primitiveRowBuilderFactory(primitives));
  }

  /** Executes a SQL query and returns the results as an enumerator, using a
   * row builder to convert JDBC column values into rows. */
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
      statement.setQueryTimeout(10);
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
    return new Function1<ResultSet, Function0<Object>>() {
      public Function0<Object> apply(final ResultSet resultSet) {
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
          return new Function0<Object>() {
            public Object apply() {
              try {
                return resultSet.getObject(1);
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          };
        }
        //noinspection unchecked
        return (Function0) new Function0<Object[]>() {
          public Object[] apply() {
            try {
              final List<Object> list = new ArrayList<Object>();
              for (int i = 0; i < columnCount; i++) {
                list.add(primitives[i].jdbcGet(resultSet, i + 1));
              }
              return list.toArray();
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }
        };
      }
    };
  }
}

// End ResultSetEnumerable.java
