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
package net.hydromatic.optiq.test;

import net.hydromatic.linq4j.function.Function1;

import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.impl.jdbc.JdbcQueryProvider;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.runtime.Hook;

import org.eigenbase.util.Util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;


/**
 * Fluid DSL for testing Optiq connections and queries.
 */
public class OptiqAssert {
  private static final DateFormat UTC_DATE_FORMAT;
  private static final DateFormat UTC_TIME_FORMAT;
  private static final DateFormat UTC_TIMESTAMP_FORMAT;
  static {
    final TimeZone utc = TimeZone.getTimeZone("UTC");
    UTC_DATE_FORMAT = new SimpleDateFormat("YYYY-MM-dd");
    UTC_DATE_FORMAT.setTimeZone(utc);
    UTC_TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");
    UTC_TIME_FORMAT.setTimeZone(utc);
    UTC_TIMESTAMP_FORMAT = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss'Z'");
    UTC_TIMESTAMP_FORMAT.setTimeZone(utc);
  }

  /** Implementation of {@link AssertThat} that does nothing. */
  private static final AssertThat DISABLED =
      new AssertThat((Config) null) {
        @Override
        public AssertThat with(Config config) {
          return this;
        }

        @Override
        public AssertThat with(ConnectionFactory connectionFactory) {
          return this;
        }

        @Override
        public AssertThat with(String name, Object schema) {
          return this;
        }

        @Override
        public AssertThat withModel(String model) {
          return this;
        }

        @Override
        public AssertQuery query(String sql) {
          return ASD(sql);
        }

        @Override
        public void connectThrows(String message) {
          // nothing
        }

        @Override
        public void connectThrows(Function1<Throwable, Void> exceptionChecker) {
          // nothing
        }

        @Override
        public <T> AssertThat doWithConnection(Function1<OptiqConnection, T> fn)
            throws Exception {
          return this;
        }

        @Override
        public AssertThat withSchema(String schema) {
          return this;
        }

        @Override
        public AssertThat enable(boolean enabled) {
          return this;
        }
      };

  /** Returns an implementation of {@link AssertQuery} that does nothing. */
  private static AssertQuery ASD(final String sql) {
    return new AssertQuery(null, sql) {
      @Override
      protected Connection createConnection() throws Exception {
        throw new AssertionError("disabled");
      }

      @Override
      public AssertQuery returns(String expected) {
        return this;
      }

      @Override
      public AssertQuery returns(Function1<ResultSet, Void> checker) {
        return this;
      }

      @Override
      public AssertQuery throws_(String message) {
        return this;
      }

      @Override
      public AssertQuery runs() {
        return this;
      }

      @Override
      public AssertQuery explainContains(String expected) {
        return this;
      }

      @Override
      public AssertQuery planContains(String expected) {
        return this;
      }

      @Override
      public AssertQuery planHasSql(String expected) {
        return this;
      }
    };
  }

  public static AssertThat assertThat() {
    return new AssertThat(Config.REGULAR);
  }

  static Function1<Throwable, Void> checkException(
      final String expected) {
    return new Function1<Throwable, Void>() {
      public Void apply(Throwable p0) {
        assertNotNull(
            "expected exception but none was thrown", p0);
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        p0.printStackTrace(printWriter);
        printWriter.flush();
        String stack = stringWriter.toString();
        assertTrue(stack, stack.contains(expected));
        return null;
      }
    };
  }

  static Function1<ResultSet, Void> checkResult(final String expected) {
    return new Function1<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          final String resultString = OptiqAssert.toString(resultSet);
          assertEquals(expected, resultString);
          return null;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  static Function1<ResultSet, Void> checkResultUnordered(
      final String... lines) {
    return new Function1<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          final Collection<String> actualSet = new TreeSet<String>();
          OptiqAssert.toStringList(resultSet, actualSet);
          final TreeSet<String> expectedSet =
              new TreeSet<String>(Arrays.asList(lines));
          assertEquals(expectedSet, actualSet);
          return null;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  public static Function1<ResultSet, Void> checkResultContains(
      final String expected) {
    return new Function1<ResultSet, Void>() {
      public Void apply(ResultSet s) {
        try {
          final String actual = OptiqAssert.toString(s);
          assertTrue(actual, actual.contains(expected));
          return null;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  static void assertQuery(
      Connection connection,
      String sql,
      int limit,
      Function1<ResultSet, Void> resultChecker,
      Function1<Throwable, Void> exceptionChecker)
      throws Exception {
    Statement statement = connection.createStatement();
    statement.setMaxRows(limit <= 0 ? limit : Math.max(limit, 1));
    ResultSet resultSet;
    try {
      resultSet = statement.executeQuery(sql);
      if (exceptionChecker != null) {
        exceptionChecker.apply(null);
        return;
      }
    } catch (Exception e) {
      if (exceptionChecker != null) {
        exceptionChecker.apply(e);
        return;
      }
      throw e;
    } catch (Error e) {
      if (exceptionChecker != null) {
        exceptionChecker.apply(e);
        return;
      }
      throw e;
    }
    if (resultChecker != null) {
      resultChecker.apply(resultSet);
    }
    resultSet.close();
    statement.close();
    connection.close();
  }

  static String toString(ResultSet resultSet) throws SQLException {
    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      int n = resultSet.getMetaData().getColumnCount();
      if (n > 0) {
        for (int i = 1;; i++) {
          buf.append(resultSet.getMetaData().getColumnLabel(i))
              .append("=")
              .append(str(resultSet, i));
          if (i == n) {
            break;
          }
          buf.append("; ");
        }
      }
      buf.append("\n");
    }
    return buf.toString();
  }

  static void toStringList(ResultSet resultSet, Collection<String> list)
      throws SQLException {
    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      int n = resultSet.getMetaData().getColumnCount();
      if (n > 0) {
        for (int i = 1;; i++) {
          buf.append(resultSet.getMetaData().getColumnLabel(i))
              .append("=")
              .append(str(resultSet, i));
          if (i == n) {
            break;
          }
          buf.append("; ");
        }
      }
      list.add(buf.toString());
      buf.setLength(0);
    }
  }

  private static String str(ResultSet resultSet, int i) throws SQLException {
    final int columnType = resultSet.getMetaData().getColumnType(i);
    switch (columnType) {
    case Types.DATE:
      final Date date = resultSet.getDate(i, null);
      return date == null ? "null" : UTC_DATE_FORMAT.format(date);
    case Types.TIME:
      final Time time = resultSet.getTime(i, null);
      return time == null ? "null" : UTC_TIME_FORMAT.format(time);
    case Types.TIMESTAMP:
      final Timestamp timestamp = resultSet.getTimestamp(i, null);
      return timestamp == null
          ? "null" : UTC_TIMESTAMP_FORMAT.format(timestamp);
    default:
      return String.valueOf(resultSet.getObject(i));
    }
  }

  /**
   * Result of calling {@link OptiqAssert#assertThat}.
   */
  public static class AssertThat {
    private final ConnectionFactory connectionFactory;

    private AssertThat(Config config) {
      this(new ConfigConnectionFactory(config));
    }

    private AssertThat(ConnectionFactory connectionFactory) {
      this.connectionFactory = connectionFactory;
    }

    public AssertThat with(Config config) {
      return new AssertThat(config);
    }

    public AssertThat with(ConnectionFactory connectionFactory) {
      return new AssertThat(connectionFactory);
    }

    /** Sets the default schema to a reflective schema based on a given
     * object. */
    public AssertThat with(final String name, final Object schema) {
      return with(
          new OptiqAssert.ConnectionFactory() {
            public OptiqConnection createConnection() throws Exception {
              Class.forName("net.hydromatic.optiq.jdbc.Driver");
              Connection connection =
                  DriverManager.getConnection("jdbc:optiq:");
              OptiqConnection optiqConnection =
                  connection.unwrap(OptiqConnection.class);
              MutableSchema rootSchema =
                  optiqConnection.getRootSchema();
              ReflectiveSchema.create(rootSchema, name, schema);
              optiqConnection.setSchema(name);
              return optiqConnection;
            }
          });
    }

    public AssertThat withModel(final String model) {
      return new AssertThat(
          new OptiqAssert.ConnectionFactory() {
            public OptiqConnection createConnection() throws Exception {
              Class.forName("net.hydromatic.optiq.jdbc.Driver");
              final Properties info = new Properties();
              info.setProperty("model", "inline:" + model);
              return (OptiqConnection) DriverManager.getConnection(
                  "jdbc:optiq:", info);
            }
          });
    }

    public AssertQuery query(String sql) {
      System.out.println(sql);
      return new AssertQuery(connectionFactory, sql);
    }

    /** Asserts that there is an exception with the given message while
     * creating a connection. */
    public void connectThrows(String message) {
      connectThrows(checkException(message));
    }

    /** Asserts that there is an exception that matches the given predicate
     * while creating a connection. */
    public void connectThrows(
        Function1<Throwable, Void> exceptionChecker) {
      Throwable throwable;
      try {
        Connection x = connectionFactory.createConnection();
        try {
          x.close();
        } catch (SQLException e) {
          // ignore
        }
        throwable = null;
      } catch (Throwable e) {
        throwable = e;
      }
      exceptionChecker.apply(throwable);
    }

    /** Creates a connection and executes a callback. */
    public <T> AssertThat doWithConnection(Function1<OptiqConnection, T> fn)
        throws Exception {
      Connection connection = connectionFactory.createConnection();
      try {
        T t = fn.apply((OptiqConnection) connection);
        Util.discard(t);
        return AssertThat.this;
      } finally {
        connection.close();
      }
    }

    public AssertThat withSchema(String schema) {
      return new AssertThat(
          new SchemaConnectionFactory(connectionFactory, schema));
    }

    public AssertThat enable(boolean enabled) {
      return enabled ? this : DISABLED;
    }
  }

  public interface ConnectionFactory {
    OptiqConnection createConnection() throws Exception;
  }

  private static class ConfigConnectionFactory implements ConnectionFactory {
    private final Config config;

    public ConfigConnectionFactory(Config config) {
      this.config = config;
    }

    public OptiqConnection createConnection() throws Exception {
      switch (config) {
      case REGULAR:
        return JdbcTest.getConnection("hr", "foodmart");
      case REGULAR_PLUS_METADATA:
        return JdbcTest.getConnection("hr", "foodmart", "metadata");
      case JDBC_FOODMART2:
        return JdbcTest.getConnection(null, false);
      case JDBC_FOODMART:
        return JdbcTest.getConnection(
            JdbcQueryProvider.INSTANCE, false);
      case FOODMART_CLONE:
        return JdbcTest.getConnection(JdbcQueryProvider.INSTANCE, true);
      default:
        throw Util.unexpected(config);
      }
    }
  }

  private static class DelegatingConnectionFactory
      implements ConnectionFactory {
    private final ConnectionFactory factory;

    public DelegatingConnectionFactory(ConnectionFactory factory) {
      this.factory = factory;
    }

    public OptiqConnection createConnection() throws Exception {
      return factory.createConnection();
    }
  }

  private static class SchemaConnectionFactory
      extends DelegatingConnectionFactory {
    private final String schema;

    public SchemaConnectionFactory(ConnectionFactory factory, String schema) {
      super(factory);
      this.schema = schema;
    }

    @Override
    public OptiqConnection createConnection() throws Exception {
      OptiqConnection connection = super.createConnection();
      connection.setSchema(schema);
      return connection;
    }
  }

  public static class AssertQuery {
    private final String sql;
    private ConnectionFactory connectionFactory;
    private String plan;
    private int limit;

    private AssertQuery(ConnectionFactory connectionFactory, String sql) {
      this.sql = sql;
      this.connectionFactory = connectionFactory;
    }

    protected Connection createConnection() throws Exception {
      return connectionFactory.createConnection();
    }

    public AssertQuery returns(String expected) {
      return returns(checkResult(expected));
    }

    public AssertQuery returns(Function1<ResultSet, Void> checker) {
      try {
        assertQuery(createConnection(), sql, limit, checker, null);
        return this;
      } catch (Exception e) {
        throw new RuntimeException(
            "exception while executing [" + sql + "]", e);
      }
    }

    public AssertQuery returnsUnordered(String... lines) {
      return returns(checkResultUnordered(lines));
    }

    public AssertQuery throws_(String message) {
      try {
        assertQuery(createConnection(), sql, limit, null,
            checkException(message));
        return this;
      } catch (Exception e) {
        throw new RuntimeException(
            "exception while executing [" + sql + "]", e);
      }
    }

    public AssertQuery runs() {
      try {
        assertQuery(createConnection(), sql, limit, null, null);
        return this;
      } catch (Exception e) {
        throw new RuntimeException(
            "exception while executing [" + sql + "]", e);
      }
    }

    public AssertQuery explainContains(final String expected) {
      try {
        assertQuery(
            createConnection(),
            "explain plan for " + sql, limit, new Function1<ResultSet, Void>() {
              public Void apply(ResultSet s) {
                try {
                  final String actual = OptiqAssert.toString(s);
                  assertTrue(actual, actual.contains(expected));
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            }, null);
        return this;
      } catch (Exception e) {
        throw new RuntimeException(
            "exception while explaining [" + sql + "]", e);
      }
    }

    public AssertQuery planContains(String expected) {
      ensurePlan();
      assertTrue(
          "Plan [" + plan + "] contains [" + expected + "]",
          plan.contains(expected));
      return this;
    }

    public AssertQuery planHasSql(String expected) {
      return planContains(
          "getDataSource(), \"" + expected.replaceAll("\n", "\\\\n")
              + "\")");
    }

    private void ensurePlan() {
      if (plan != null) {
        return;
      }
      final Hook.Closeable hook = Hook.JAVA_PLAN.add(
          new Function1<Object, Object>() {
            public Object apply(Object a0) {
              plan = (String) a0;
              return null;
            }
          });
      try {
        assertQuery(createConnection(), sql, limit, null, null);
        assertNotNull(plan);
      } catch (Exception e) {
        throw new RuntimeException(
            "exception while executing [" + sql + "]", e);
      } finally {
        hook.close();
      }
    }

    /** Sets a limit on the number of rows returned. -1 means no limit. */
    public AssertQuery limit(int limit) {
      this.limit = limit;
      return this;
    }
  }

  public enum Config {
    /**
     * Configuration that creates a connection with two in-memory data sets:
     * {@link net.hydromatic.optiq.test.JdbcTest.HrSchema} and
     * {@link net.hydromatic.optiq.test.JdbcTest.FoodmartSchema}.
     */
    REGULAR,

    /**
     * Configuration that creates a connection to a MySQL server. Tables
     * such as "customer" and "sales_fact_1997" are available. Queries
     * are processed by generating Java that calls linq4j operators
     * such as
     * {@link net.hydromatic.linq4j.Enumerable#where(net.hydromatic.linq4j.function.Predicate1)}.
     */
    JDBC_FOODMART,
    JDBC_FOODMART2,

    /** Configuration that contains an in-memory clone of the FoodMart
     * database. */
    FOODMART_CLONE,

    /** Configuration that includes the metadata schema. */
    REGULAR_PLUS_METADATA,
  }
}

// End OptiqAssert.java
