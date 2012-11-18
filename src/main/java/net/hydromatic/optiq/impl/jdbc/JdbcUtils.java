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
package net.hydromatic.optiq.impl.jdbc;

import net.hydromatic.linq4j.Enumerator;

import net.hydromatic.linq4j.expressions.Primitive;

import org.eigenbase.sql.SqlDialect;

import java.sql.*;
import java.util.*;
import javax.sql.DataSource;

/**
 * Utilities for the JDBC provider.
 *
 * @author jhyde
 */
final class JdbcUtils {
    private JdbcUtils() {
        throw new AssertionError("no instances!");
    }

    /** Executes a SQL query and returns the results as an enumerator. The
     * parameterization not withstanding, the result type must be an array of
     * objects. */
    static <T> Enumerator<T> sqlEnumerator(
        String sql,
        JdbcSchema dataContext,
        final Primitive[] primitives)
    {
        Connection connection;
        Statement statement;
        try {
            connection = dataContext.dataSource.getConnection();
            statement = connection.createStatement();
            final ResultSet resultSet;
            resultSet = statement.executeQuery(sql);
            final int columnCount = resultSet.getMetaData().getColumnCount();
            return new Enumerator<T>() {
                public T current() {
                    Object[] os = new Object[columnCount];
                    try {
                        for (int i = 0; i < os.length; i++) {
                            os[i] = value(i);
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return (T) os;
                }

                private Object value(int i) throws SQLException {
                    switch (primitives[i]) {
                    case BOOLEAN:
                        return resultSet.getBoolean(i + 1);
                    case BYTE:
                        return resultSet.getByte(i + 1);
                    case CHARACTER:
                        return (char) resultSet.getShort(i + 1);
                    case DOUBLE:
                        return resultSet.getDouble(i + 1);
                    case FLOAT:
                        return resultSet.getFloat(i + 1);
                    case INT:
                        return resultSet.getInt(i + 1);
                    case LONG:
                        return resultSet.getLong(i + 1);
                    case SHORT:
                        return resultSet.getShort(i + 1);
                    default:
                        return resultSet.getObject(i + 1);
                    }
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
                        resultSet.first();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


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
                List key = Arrays.asList(productName, productVersion);
                SqlDialect dialect = map.get(key);
                if (dialect == null) {
                    dialect =
                        new SqlDialect(
                            SqlDialect.getProduct(
                                productName, productVersion),
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
}

// End JdbcUtils.java
