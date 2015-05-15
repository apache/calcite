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
package org.apache.calcite.test;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.clone.CloneSchema;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteMetaImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Lists;

import net.hydromatic.foodmart.data.hsqldb.FoodmartHsqldb;
import net.hydromatic.scott.data.hsqldb.ScottHsqldb;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Fluid DSL for testing Calcite connections and queries.
 */
public class CalciteAssert {
  private CalciteAssert() {}

  /** Which database to use for tests that require a JDBC data source. By
   * default the test suite runs against the embedded hsqldb database.
   *
   * <p>We recommend that casual users use hsqldb, and frequent Calcite
   * developers use MySQL. The test suite runs faster against the MySQL database
   * (mainly because of the 0.1s versus 6s startup time). You have to populate
   * MySQL manually with the foodmart data set, otherwise there will be test
   * failures.  To run against MySQL, specify '-Dcalcite.test.db=mysql' on the
   * java command line. */
  public static final DatabaseInstance DB =
      DatabaseInstance.valueOf(
          Util.first(System.getProperty("calcite.test.db"), "HSQLDB")
              .toUpperCase());

  /** Whether to enable slow tests. Default is false. */
  public static final boolean ENABLE_SLOW =
      Util.first(Boolean.getBoolean("calcite.test.slow"), false);

  private static final DateFormat UTC_DATE_FORMAT;
  private static final DateFormat UTC_TIME_FORMAT;
  private static final DateFormat UTC_TIMESTAMP_FORMAT;
  static {
    final TimeZone utc = TimeZone.getTimeZone("UTC");
    UTC_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    UTC_DATE_FORMAT.setTimeZone(utc);
    UTC_TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");
    UTC_TIME_FORMAT.setTimeZone(utc);
    UTC_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    UTC_TIMESTAMP_FORMAT.setTimeZone(utc);
  }

  private static final ConnectionFactory EMPTY_CONNECTION_FACTORY =
      new MapConnectionFactory(ImmutableMap.<String, String>of(),
          ImmutableList.<ConnectionPostProcessor>of());

  /** Implementation of {@link AssertThat} that does nothing. */
  private static final AssertThat DISABLED =
      new AssertThat(EMPTY_CONNECTION_FACTORY) {
        @Override public AssertThat with(Config config) {
          return this;
        }

        @Override public AssertThat with(ConnectionFactory connectionFactory) {
          return this;
        }

        @Override public AssertThat with(String property, String value) {
          return this;
        }

        @Override public AssertThat withSchema(String name, Schema schema) {
          return this;
        }

        @Override public AssertQuery query(String sql) {
          return NopAssertQuery.of(sql);
        }

        @Override public AssertThat connectThrows(
            Function<Throwable, Void> exceptionChecker) {
          return this;
        }

        @Override public <T> AssertThat doWithConnection(
            Function<CalciteConnection, T> fn)
            throws Exception {
          return this;
        }

        @Override public AssertThat withDefaultSchema(String schema) {
          return this;
        }

        @Override public AssertThat with(SchemaSpec... specs) {
          return this;
        }

        @Override public AssertThat with(Lex lex) {
          return this;
        }

        @Override public AssertThat with(
            ConnectionPostProcessor postProcessor) {
          return this;
        }

        @Override public AssertThat enable(boolean enabled) {
          return this;
        }

        @Override public AssertThat pooled() {
          return this;
        }
      };

  /** Creates an instance of {@code CalciteAssert} with the empty
   * configuration. */
  public static AssertThat that() {
    return AssertThat.EMPTY;
  }

  /** Creates an instance of {@code CalciteAssert} with a given
   * configuration. */
  public static AssertThat that(Config config) {
    return that().with(config);
  }

  /** Short-hand for
   *  {@code CalciteAssert.that().with(Config.EMPTY).withModel(model)}. */
  public static AssertThat model(String model) {
    return that().withModel(model);
  }

  /** Short-hand for {@code CalciteAssert.that().with(Config.REGULAR)}. */
  public static AssertThat hr() {
    return that(Config.REGULAR);
  }

  static Function<RelNode, Void> checkRel(final String expected,
      final AtomicInteger counter) {
    return new Function<RelNode, Void>() {
      public Void apply(RelNode relNode) {
        if (counter != null) {
          counter.incrementAndGet();
        }
        String s = Util.toLinux(RelOptUtil.toString(relNode));
        assertThat(s, containsString(expected));
        return null;
      }
    };
  }

  static Function<Throwable, Void> checkException(
      final String expected) {
    return new Function<Throwable, Void>() {
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

  static Function<ResultSet, Void> checkResult(final String expected) {
    return new Function<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          final String resultString = CalciteAssert.toString(resultSet);
          assertEquals(expected, Util.toLinux(resultString));
          return null;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  static Function<ResultSet, Void> checkResultValue(final String expected) {
    return new Function<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          if (!resultSet.next()) {
            throw new AssertionError("too few rows");
          }
          if (resultSet.getMetaData().getColumnCount() != 1) {
            throw new AssertionError("expected 1 column");
          }
          final String resultString = resultSet.getString(1);
          assertEquals(expected, Util.toLinux(resultString));
          return null;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  static Function<ResultSet, Void> checkResultCount(final int expected) {
    return new Function<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          final int count = CalciteAssert.countRows(resultSet);
          assertEquals(expected, count);
          return null;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  /** Checks that the result of the second and subsequent executions is the same
   * as the first.
   *
   * @param ordered Whether order should be the same both times
   */
  static Function<ResultSet, Void> consistentResult(final boolean ordered) {
    return new Function<ResultSet, Void>() {
      int executeCount = 0;
      Collection expected;

      public Void apply(ResultSet resultSet) {
        ++executeCount;
        try {
          final Collection result =
              CalciteAssert.toStringList(resultSet,
                  ordered ? new ArrayList<String>() : new TreeSet<String>());
          if (executeCount == 1) {
            expected = result;
          } else {
            if (!expected.equals(result)) {
              // compare strings to get better error message
              assertThat(newlineList(result), equalTo(newlineList(expected)));
              fail("oops");
            }
          }
          return null;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  static String newlineList(Collection collection) {
    final StringBuilder buf = new StringBuilder();
    for (Object o : collection) {
      buf.append(o).append('\n');
    }
    return buf.toString();
  }

  static Function<ResultSet, Void> checkResultUnordered(
      final String... lines) {
    return new Function<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          final List<String> expectedList = Lists.newArrayList(lines);
          Collections.sort(expectedList);

          final List<String> actualList = Lists.newArrayList();
          CalciteAssert.toStringList(resultSet, actualList);
          Collections.sort(actualList);

          if (!actualList.equals(expectedList)) {
            assertThat(Util.lines(actualList),
                equalTo(Util.lines(expectedList)));
          }
          return null;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  public static Function<ResultSet, Void> checkResultContains(
      final String expected) {
    return new Function<ResultSet, Void>() {
      public Void apply(ResultSet s) {
        try {
          final String actual = Util.toLinux(CalciteAssert.toString(s));
          assertThat(actual, containsString(expected));
          return null;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  public static Function<ResultSet, Void> checkMaskedResultContains(
      final String expected) {
    return new Function<ResultSet, Void>() {
      public Void apply(ResultSet s) {
        try {
          final String actual = Util.toLinux(CalciteAssert.toString(s));
          final String maskedActual =
              actual.replaceAll(", id = [0-9]+", "");
          assertThat(maskedActual, containsString(expected));
          return null;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  public static Function<ResultSet, Void> checkResultType(
      final String expected) {
    return new Function<ResultSet, Void>() {
      public Void apply(ResultSet s) {
        try {
          final String actual = typeString(s.getMetaData());
          assertEquals(expected, actual);
          return null;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static String typeString(ResultSetMetaData metaData)
      throws SQLException {
    final List<String> list = new ArrayList<String>();
    for (int i = 0; i < metaData.getColumnCount(); i++) {
      list.add(
          metaData.getColumnName(i + 1)
              + " "
              + metaData.getColumnTypeName(i + 1)
              + (metaData.isNullable(i + 1) == ResultSetMetaData.columnNoNulls
              ? " NOT NULL"
              : ""));
    }
    return list.toString();
  }

  static void assertQuery(
      Connection connection,
      String sql,
      int limit,
      boolean materializationsEnabled,
      List<Pair<Hook, Function>> hooks,
      Function<ResultSet, Void> resultChecker,
      Function<Throwable, Void> exceptionChecker) throws Exception {
    final String message =
        "With materializationsEnabled=" + materializationsEnabled
            + ", limit=" + limit;
    final List<Hook.Closeable> closeableList = Lists.newArrayList();
    try {
      if (connection instanceof CalciteConnection) {
        CalciteConnection calciteConnection = (CalciteConnection) connection;
        calciteConnection.getProperties().setProperty(
            CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
            Boolean.toString(materializationsEnabled));
        calciteConnection.getProperties().setProperty(
            CalciteConnectionProperty.CREATE_MATERIALIZATIONS.camelName(),
            Boolean.toString(materializationsEnabled));
      }
      for (Pair<Hook, Function> hook : hooks) {
        closeableList.add(hook.left.addThread(hook.right));
      }
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
    } catch (Error e) {
      // We ignore extended message for non-runtime exception, however
      // it does not matter much since it is better to have AssertionError
      // at the very top level of the exception stack.
      throw e;
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException(message, e);
    } finally {
      for (Hook.Closeable closeable : closeableList) {
        closeable.close();
      }
    }
  }

  static void assertPrepare(
      Connection connection,
      String sql,
      boolean materializationsEnabled,
      final Function<RelNode, Void> convertChecker,
      final Function<RelNode, Void> substitutionChecker) throws Exception {
    final String message =
        "With materializationsEnabled=" + materializationsEnabled;
    final Hook.Closeable closeable =
        convertChecker == null
            ? Hook.Closeable.EMPTY
            : Hook.TRIMMED.addThread(
                new Function<RelNode, Void>() {
                  public Void apply(RelNode rel) {
                    convertChecker.apply(rel);
                    return null;
                  }
                });
    final Hook.Closeable closeable2 =
        substitutionChecker == null
            ? Hook.Closeable.EMPTY
            : Hook.SUB.addThread(
                new Function<RelNode, Void>() {
                  public Void apply(RelNode rel) {
                    substitutionChecker.apply(rel);
                    return null;
                  }
                });
    try {
      ((CalciteConnection) connection).getProperties().setProperty(
          CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
          Boolean.toString(materializationsEnabled));
      ((CalciteConnection) connection).getProperties().setProperty(
          CalciteConnectionProperty.CREATE_MATERIALIZATIONS.camelName(),
          Boolean.toString(materializationsEnabled));
      PreparedStatement statement = connection.prepareStatement(sql);
      statement.close();
      connection.close();
    } catch (Throwable e) {
      throw new RuntimeException(message, e);
    } finally {
      closeable.close();
      closeable2.close();
    }
  }

  static String toString(ResultSet resultSet) throws SQLException {
    final StringBuilder buf = new StringBuilder();
    final ResultSetMetaData metaData = resultSet.getMetaData();
    while (resultSet.next()) {
      rowToString(resultSet, buf, metaData).append("\n");
    }
    return buf.toString();
  }

  /** Converts one row to a string. */
  static StringBuilder rowToString(ResultSet resultSet, StringBuilder buf,
      ResultSetMetaData metaData) throws SQLException {
    int n = metaData.getColumnCount();
    if (n > 0) {
      for (int i = 1;; i++) {
        buf.append(metaData.getColumnLabel(i))
            .append("=")
            .append(resultSet.getString(i));
        if (i == n) {
          break;
        }
        buf.append("; ");
      }
    }
    return buf;
  }

  static int countRows(ResultSet resultSet) throws SQLException {
    int n = 0;
    while (resultSet.next()) {
      ++n;
    }
    return n;
  }

  static Collection<String> toStringList(ResultSet resultSet,
      Collection<String> list) throws SQLException {
    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      rowToString(resultSet, buf, resultSet.getMetaData());
      list.add(buf.toString());
      buf.setLength(0);
    }
    return list;
  }

  static ImmutableMultiset<String> toSet(ResultSet resultSet)
      throws SQLException {
    return ImmutableMultiset.copyOf(
        toStringList(resultSet, new ArrayList<String>()));
  }

  /** Calls a non-static method via reflection. Useful for testing methods that
   * don't exist in certain versions of the JDK. */
  static Object call(Object o, String methodName, Object... args)
      throws NoSuchMethodException, InvocationTargetException,
      IllegalAccessException {
    return method(o, methodName, args).invoke(o, args);
  }

  /** Finds a non-static method based on its target, name and arguments.
   * Throws if not found. */
  static Method method(Object o, String methodName, Object[] args) {
    for (Class<?> aClass = o.getClass();;) {
    loop:
      for (Method method1 : aClass.getMethods()) {
        if (method1.getName().equals(methodName)
            && method1.getParameterTypes().length == args.length
            && Modifier.isPublic(method1.getDeclaringClass().getModifiers())) {
          for (Pair<Object, Class<?>> pair
              : Pair.zip(args, method1.getParameterTypes())) {
            if (!pair.right.isInstance(pair.left)) {
              continue loop;
            }
          }
          return method1;
        }
      }
      if (aClass.getSuperclass() != null
          && aClass.getSuperclass() != Object.class) {
        aClass = aClass.getSuperclass();
      } else {
        final Class<?>[] interfaces = aClass.getInterfaces();
        if (interfaces.length > 0) {
          aClass = interfaces[0];
        } else {
          break;
        }
      }
    }
    throw new AssertionError("method " + methodName + " not found");
  }

  public static SchemaPlus addSchema(SchemaPlus rootSchema, SchemaSpec schema) {
    SchemaPlus foodmart;
    SchemaPlus jdbcScott;
    final ConnectionSpec cs;
    final DataSource dataSource;
    switch (schema) {
    case REFLECTIVE_FOODMART:
      return rootSchema.add("foodmart",
          new ReflectiveSchema(new JdbcTest.FoodmartSchema()));
    case JDBC_SCOTT:
      cs = DatabaseInstance.HSQLDB.scott;
      dataSource = JdbcSchema.dataSource(cs.url, cs.driver, cs.username,
          cs.password);
      return rootSchema.add("jdbc_scott",
          JdbcSchema.create(rootSchema, "jdbc_scott", dataSource, null, null));
    case JDBC_FOODMART:
      cs = DB.foodmart;
      dataSource =
          JdbcSchema.dataSource(cs.url, cs.driver, cs.username, cs.password);
      return rootSchema.add("foodmart",
          JdbcSchema.create(rootSchema, "foodmart", dataSource, null,
              "foodmart"));
    case JDBC_FOODMART_WITH_LATTICE:
      foodmart = rootSchema.getSubSchema("foodmart");
      if (foodmart == null) {
        foodmart =
            CalciteAssert.addSchema(rootSchema, SchemaSpec.JDBC_FOODMART);
      }
      foodmart.add("lattice",
          Lattice.create(foodmart.unwrap(CalciteSchema.class),
              "select 1 from \"foodmart\".\"sales_fact_1997\" as s\n"
                  + "join \"foodmart\".\"time_by_day\" as t using (\"time_id\")\n"
                  + "join \"foodmart\".\"customer\" as c using (\"customer_id\")\n"
                  + "join \"foodmart\".\"product\" as p using (\"product_id\")\n"
                  + "join \"foodmart\".\"product_class\" as pc on p.\"product_class_id\" = pc.\"product_class_id\"",
              true));
      return foodmart;
    case SCOTT:
      jdbcScott = rootSchema.getSubSchema("jdbc_scott");
      if (jdbcScott == null) {
        jdbcScott =
            CalciteAssert.addSchema(rootSchema, SchemaSpec.JDBC_SCOTT);
      }
      return rootSchema.add("scott", new CloneSchema(jdbcScott));
    case CLONE_FOODMART:
      foodmart = rootSchema.getSubSchema("foodmart");
      if (foodmart == null) {
        foodmart =
            CalciteAssert.addSchema(rootSchema, SchemaSpec.JDBC_FOODMART);
      }
      return rootSchema.add("foodmart2", new CloneSchema(foodmart));
    case HR:
      return rootSchema.add("hr",
          new ReflectiveSchema(new JdbcTest.HrSchema()));
    case LINGUAL:
      return rootSchema.add("SALES",
          new ReflectiveSchema(new JdbcTest.LingualSchema()));
    case POST:
      final SchemaPlus post = rootSchema.add("POST", new AbstractSchema());
      post.add("EMP",
          ViewTable.viewMacro(post,
              "select * from (values\n"
                  + "    ('Jane', 10, 'F'),\n"
                  + "    ('Bob', 10, 'M'),\n"
                  + "    ('Eric', 20, 'M'),\n"
                  + "    ('Susan', 30, 'F'),\n"
                  + "    ('Alice', 30, 'F'),\n"
                  + "    ('Adam', 50, 'M'),\n"
                  + "    ('Eve', 50, 'F'),\n"
                  + "    ('Grace', 60, 'F'),\n"
                  + "    ('Wilma', cast(null as integer), 'F'))\n"
                  + "  as t(ename, deptno, gender)",
              ImmutableList.<String>of(), null));
      post.add("DEPT",
          ViewTable.viewMacro(post,
              "select * from (values\n"
                  + "    (10, 'Sales'),\n"
                  + "    (20, 'Marketing'),\n"
                  + "    (30, 'Engineering'),\n"
                  + "    (40, 'Empty')) as t(deptno, dname)",
              ImmutableList.<String>of(), null));
      post.add("EMPS",
          ViewTable.viewMacro(post,
              "select * from (values\n"
                  + "    (100, 'Fred',  10, CAST(NULL AS CHAR(1)), CAST(NULL AS VARCHAR(20)), 40,               25, TRUE,    FALSE, DATE '1996-08-03'),\n"
                  + "    (110, 'Eric',  20, 'M',                   'San Francisco',           3,                80, UNKNOWN, FALSE, DATE '2001-01-01'),\n"
                  + "    (110, 'John',  40, 'M',                   'Vancouver',               2, CAST(NULL AS INT), FALSE,   TRUE,  DATE '2002-05-03'),\n"
                  + "    (120, 'Wilma', 20, 'F',                   CAST(NULL AS VARCHAR(20)), 1,                 5, UNKNOWN, TRUE,  DATE '2005-09-07'),\n"
                  + "    (130, 'Alice', 40, 'F',                   'Vancouver',               2, CAST(NULL AS INT), FALSE,   TRUE,  DATE '2007-01-01'))\n"
                  + " as t(empno, name, deptno, gender, city, empid, age, slacker, manager, joinedat)",
              ImmutableList.<String>of(), null));
      return post;
    default:
      throw new AssertionError("unknown schema " + schema);
    }
  }

  /**
   * Asserts that two objects are equal. If they are not, an
   * {@link AssertionError} is thrown with the given message. If
   * <code>expected</code> and <code>actual</code> are <code>null</code>,
   * they are considered equal.
   *
   * <p>This method produces more user-friendly error messages than
   * {@link org.junit.Assert#assertArrayEquals(String, Object[], Object[])}
   *
   * @param message the identifying message for the {@link AssertionError} (<code>null</code>
   * okay)
   * @param expected expected value
   * @param actual actual value
   */
  public static void assertArrayEqual(
      String message, Object[] expected, Object[] actual) {
    Joiner joiner = Joiner.on('\n');
    String strExpected = expected == null ? null : joiner.join(expected);
    String strActual = actual == null ? null : joiner.join(actual);
    assertEquals(message, strExpected, strActual);
  }

  static <F, T> Function<F, T> constantNull() {
    //noinspection unchecked
    return (Function<F, T>) (Function) Functions.<T>constant(null);
  }

  /**
   * Result of calling {@link CalciteAssert#that}.
   */
  public static class AssertThat {
    private final ConnectionFactory connectionFactory;

    private static final AssertThat EMPTY =
        new AssertThat(EMPTY_CONNECTION_FACTORY);

    private AssertThat(ConnectionFactory connectionFactory) {
      this.connectionFactory = Preconditions.checkNotNull(connectionFactory);
    }

    public AssertThat with(Config config) {
      if (config == Config.SPARK) {
        return with("spark", "true");
      }

      switch (config) {
      case EMPTY:
        return EMPTY;
      case REGULAR:
        return with(SchemaSpec.HR, SchemaSpec.REFLECTIVE_FOODMART,
            SchemaSpec.POST);
      case REGULAR_PLUS_METADATA:
        return with(SchemaSpec.HR, SchemaSpec.REFLECTIVE_FOODMART);
      case LINGUAL:
        return with(SchemaSpec.LINGUAL);
      case JDBC_FOODMART:
        return with(CalciteAssert.SchemaSpec.JDBC_FOODMART);
      case FOODMART_CLONE:
        return with(SchemaSpec.CLONE_FOODMART);
      case JDBC_FOODMART_WITH_LATTICE:
        return with(SchemaSpec.JDBC_FOODMART_WITH_LATTICE);
      case SCOTT:
        return with(SchemaSpec.SCOTT);
      default:
        throw Util.unexpected(config);
      }
    }

    /** Creates a copy of this AssertThat, adding more schemas */
    public AssertThat with(SchemaSpec... specs) {
      AssertThat next = this;
      for (SchemaSpec spec : specs) {
        next = next.with(new AddSchemaSpecPostProcessor(spec));
      }
      return next;
    }

    /** Creates a copy of this AssertThat, overriding the connection factory. */
    public AssertThat with(ConnectionFactory connectionFactory) {
      return new AssertThat(connectionFactory);
    }

    public final AssertThat with(final Map<String, String> map) {
      AssertThat x = this;
      for (Map.Entry<String, String> entry : map.entrySet()) {
        x = with(entry.getKey(), entry.getValue());
      }
      return x;
    }

    public AssertThat with(String property, String value) {
      return new AssertThat(connectionFactory.with(property, value));
    }

    /** Sets Lex property **/
    public AssertThat with(Lex lex) {
      return with(CalciteConnectionProperty.LEX.name(), lex.toString());
    }

    /** Sets the default schema to a given schema. */
    public AssertThat withSchema(String name, Schema schema) {
      return new AssertThat(
          connectionFactory.with(new AddSchemaPostProcessor(name, schema)));
    }

    public AssertThat with(ConnectionPostProcessor postProcessor) {
      return new AssertThat(connectionFactory.with(postProcessor));
    }

    public final AssertThat withModel(String model) {
      return with("model", "inline:" + model);
    }

    /** Adds materializations to the schema. */
    public final AssertThat withMaterializations(String model,
        String... materializations) {
      assert materializations.length % 2 == 0;
      final JsonBuilder builder = new JsonBuilder();
      final List<Object> list = builder.list();
      for (int i = 0; i < materializations.length; i++) {
        String table = materializations[i++];
        final Map<String, Object> map = builder.map();
        map.put("table", table);
        map.put("view", table + "v");
        String sql = materializations[i];
        final String sql2 = sql
            .replaceAll("`", "\"");
        map.put("sql", sql2);
        list.add(map);
      }
      final String buf =
          "materializations: " + builder.toJsonString(list);
      final String model2;
      if (model.contains("defaultSchema: 'foodmart'")) {
        model2 = model.replace("]",
            ", { name: 'mat', "
            + buf
            + "}\n"
            + "]");
      } else if (model.contains("type: ")) {
        model2 = model.replace("type: ",
            buf + ",\n"
            + "type: ");
      } else {
        throw new AssertionError("do not know where to splice");
      }
      return withModel(model2);
    }

    public AssertQuery query(String sql) {
      return new AssertQuery(connectionFactory, sql);
    }

    /** Asserts that there is an exception with the given message while
     * creating a connection. */
    public AssertThat connectThrows(String message) {
      return connectThrows(checkException(message));
    }

    /** Asserts that there is an exception that matches the given predicate
     * while creating a connection. */
    public AssertThat connectThrows(
        Function<Throwable, Void> exceptionChecker) {
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
      return this;
    }

    /** Creates a {@link org.apache.calcite.jdbc.CalciteConnection}
     * and executes a callback. */
    public <T> AssertThat doWithConnection(Function<CalciteConnection, T> fn)
        throws Exception {
      Connection connection = connectionFactory.createConnection();
      try {
        T t = fn.apply((CalciteConnection) connection);
        Util.discard(t);
        return AssertThat.this;
      } finally {
        connection.close();
      }
    }

    /** Creates a {@link DataContext} and executes a callback. */
    public <T> AssertThat doWithDataContext(Function<DataContext, T> fn)
        throws Exception {
      CalciteConnection connection =
          (CalciteConnection) connectionFactory.createConnection();
      final DataContext dataContext = CalciteMetaImpl.createDataContext(
          connection);
      try {
        T t = fn.apply(dataContext);
        Util.discard(t);
        return AssertThat.this;
      } finally {
        connection.close();
      }
    }

    public AssertThat withDefaultSchema(String schema) {
      return new AssertThat(
          connectionFactory.with(
              new AddSchemaPostProcessor(schema, null)));
    }

    /** Use sparingly. Does not close the connection. */
    public Connection connect() throws SQLException {
      return connectionFactory.createConnection();
    }

    public AssertThat enable(boolean enabled) {
      return enabled ? this : DISABLED;
    }

    /** Returns a version that uses a single connection, as opposed to creating
     * a new one each time a test method is invoked. */
    public AssertThat pooled() {
      if (connectionFactory instanceof PoolingConnectionFactory) {
        return this;
      } else {
        return new AssertThat(new PoolingConnectionFactory(connectionFactory));
      }
    }

    public AssertMetaData metaData(Function<Connection, ResultSet> function) {
      return new AssertMetaData(connectionFactory, function);
    }
  }

  /**
   * Abstract implementation of connection factory whose {@code with}
   * methods throw.
   *
   * <p>Avoid creating new sub-classes otherwise it would be hard to support
   * {@code .with(property, value).with(...)} kind of chains.
   *
   * <p>If you want augment the connection, use {@link ConnectionPostProcessor}.
   **/
  public abstract static class ConnectionFactory {
    public abstract Connection createConnection() throws SQLException;

    public ConnectionFactory with(String property, String value) {
      throw new UnsupportedOperationException();
    }

    public ConnectionFactory with(ConnectionPostProcessor postProcessor) {
      throw new UnsupportedOperationException();
    }
  }

  /** Connection post processor */
  public interface ConnectionPostProcessor {
    Connection apply(Connection connection) throws SQLException;
  }

  /** Adds {@link Schema} and sets it as default. */
  public static class AddSchemaPostProcessor
      implements ConnectionPostProcessor {
    private final String name;
    private final Schema schema;

    public AddSchemaPostProcessor(String name, Schema schema) {
      this.name = name;
      this.schema = schema;
    }

    public Connection apply(Connection connection) throws SQLException {
      if (schema != null) {
        CalciteConnection con = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = con.getRootSchema();
        rootSchema.add(name, schema);
      }
      connection.setSchema(name);
      return connection;
    }
  }

  /** Adds {@link SchemaSpec} (set of schemes) to a connection. */
  public static class AddSchemaSpecPostProcessor
      implements ConnectionPostProcessor {
    private final SchemaSpec schemaSpec;

    public AddSchemaSpecPostProcessor(SchemaSpec schemaSpec) {
      this.schemaSpec = schemaSpec;
    }

    public Connection apply(Connection connection) throws SQLException {
      CalciteConnection con = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = con.getRootSchema();
      switch (schemaSpec) {
      case CLONE_FOODMART:
      case JDBC_FOODMART_WITH_LATTICE:
        addSchema(rootSchema, SchemaSpec.JDBC_FOODMART);
        /* fall through */
      default:
        addSchema(rootSchema, schemaSpec);
      }
      if (schemaSpec == SchemaSpec.CLONE_FOODMART) {
        con.setSchema("foodmart2");
      }
      return connection;
    }
  }

  /** Connection factory that uses the same instance of connections. */
  private static class PoolingConnectionFactory
      extends ConnectionFactory {

    /** Connection pool. */
    private static class Pool {
      private static final LoadingCache<ConnectionFactory, Connection> POOL =
          CacheBuilder.newBuilder().build(
              new CacheLoader<ConnectionFactory, Connection>() {
                public Connection load(ConnectionFactory key) throws Exception {
                  return key.createConnection();
                }
              });
    }

    private final ConnectionFactory factory;

    public PoolingConnectionFactory(final ConnectionFactory factory) {
      this.factory = factory;
    }

    public Connection createConnection() throws SQLException {
      try {
        return Pool.POOL.get(factory);
      } catch (ExecutionException e) {
        throw new SQLException(
            "Unable to get pooled connection for " + factory, e);
      }
    }
  }

  /** Connection factory that uses a given map of (name, value) pairs and
   * optionally an initial schema. */
  private static class MapConnectionFactory extends ConnectionFactory {
    private final ImmutableMap<String, String> map;
    private final ImmutableList<ConnectionPostProcessor> postProcessors;

    private MapConnectionFactory(ImmutableMap<String, String> map,
        ImmutableList<ConnectionPostProcessor> postProcessors) {
      this.map = Preconditions.checkNotNull(map);
      this.postProcessors = Preconditions.checkNotNull(postProcessors);
    }

    @Override public boolean equals(Object obj) {
      return this == obj
          || obj.getClass() == MapConnectionFactory.class
          && ((MapConnectionFactory) obj).map.equals(map)
          && ((MapConnectionFactory) obj).postProcessors.equals(postProcessors);
    }

    @Override public int hashCode() {
      return Objects.hashCode(map, postProcessors);
    }

    public Connection createConnection() throws SQLException {
      final Properties info = new Properties();
      for (Map.Entry<String, String> entry : map.entrySet()) {
        info.setProperty(entry.getKey(), entry.getValue());
      }
      Connection connection =
          DriverManager.getConnection("jdbc:calcite:", info);
      for (ConnectionPostProcessor postProcessor : postProcessors) {
        connection = postProcessor.apply(connection);
      }
      return connection;
    }

    public ConnectionFactory with(String property, String value) {
      ImmutableMap.Builder<String, String> b = ImmutableMap.builder();
      b.putAll(this.map);
      b.put(property, value);
      return new MapConnectionFactory(b.build(), postProcessors);
    }

    public ConnectionFactory with(
        ConnectionPostProcessor postProcessor) {
      ImmutableList.Builder<ConnectionPostProcessor> builder =
          ImmutableList.builder();
      builder.addAll(postProcessors);
      builder.add(postProcessor);
      return new MapConnectionFactory(map, builder.build());
    }
  }

  /** Fluent interface for building a query to be tested. */
  public static class AssertQuery {
    private final String sql;
    private ConnectionFactory connectionFactory;
    private String plan;
    private int limit;
    private boolean materializationsEnabled = false;
    private final List<Pair<Hook, Function>> hooks = Lists.newArrayList();

    private AssertQuery(ConnectionFactory connectionFactory, String sql) {
      this.sql = sql;
      this.connectionFactory = connectionFactory;
    }

    protected Connection createConnection() throws Exception {
      return connectionFactory.createConnection();
    }

    public AssertQuery enable(boolean enabled) {
      return enabled ? this : NopAssertQuery.of(sql);
    }

    public AssertQuery returns(String expected) {
      return returns(checkResult(expected));
    }

    public AssertQuery returnsValue(String expected) {
      return returns(checkResultValue(expected));
    }

    public AssertQuery returnsCount(int expectedCount) {
      return returns(checkResultCount(expectedCount));
    }

    public final AssertQuery returns(Function<ResultSet, Void> checker) {
      return returns(sql, checker);
    }

    protected AssertQuery returns(String sql,
        Function<ResultSet, Void> checker) {
      try {
        assertQuery(createConnection(), sql, limit, materializationsEnabled,
            hooks, checker, null);
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
        assertQuery(createConnection(), sql, limit, materializationsEnabled,
            hooks, null, checkException(message));
        return this;
      } catch (Exception e) {
        throw new RuntimeException(
            "exception while executing [" + sql + "]", e);
      }
    }

    public AssertQuery runs() {
      try {
        assertQuery(createConnection(), sql, limit, materializationsEnabled,
            hooks, null, null);
        return this;
      } catch (Exception e) {
        throw new RuntimeException(
            "exception while executing [" + sql + "]", e);
      }
    }

    public AssertQuery typeIs(String expected) {
      try {
        assertQuery(createConnection(), sql, limit, false,
            hooks, checkResultType(expected), null);
        return this;
      } catch (Exception e) {
        throw new RuntimeException(
            "exception while executing [" + sql + "]", e);
      }
    }

    /** Checks that when the query (which was set using
     * {@link AssertThat#query(String)}) is converted to a relational algebra
     * expression matching the given string. */
    public final AssertQuery convertContains(final String expected) {
      return convertMatches(checkRel(expected, null));
    }

    public AssertQuery convertMatches(final Function<RelNode, Void> checker) {
      try {
        assertPrepare(createConnection(), sql, this.materializationsEnabled,
            checker, null);
        return this;
      } catch (Exception e) {
        throw new RuntimeException("exception while preparing [" + sql + "]",
            e);
      }
    }

    public AssertQuery substitutionMatches(
        final Function<RelNode, Void> checker) {
      try {
        assertPrepare(createConnection(), sql, materializationsEnabled,
            null, checker);
        return this;
      } catch (Exception e) {
        throw new RuntimeException("exception while preparing [" + sql + "]",
            e);
      }
    }

    public AssertQuery explainContains(String expected) {
      return explainMatches("", checkResultContains(expected));
    }

    public final AssertQuery explainMatches(String extra,
        Function<ResultSet, Void> checker) {
      return returns("explain plan " + extra + "for " + sql, checker);
    }

    public AssertQuery planContains(String expected) {
      ensurePlan();
      assertTrue(
          "Plan [" + plan + "] contains [" + expected + "]",
          Util.toLinux(plan)
              .replaceAll("\\\\r\\\\n", "\\\\n")
              .contains(expected));
      return this;
    }

    public AssertQuery planHasSql(String expected) {
      return planContains(
          "getDataSource(), \""
          + expected.replace("\\", "\\\\")
              .replace("\"", "\\\"")
              .replaceAll("\n", "\\\\n")
          + "\"");
    }

    private void ensurePlan() {
      if (plan != null) {
        return;
      }
      addHook(Hook.JAVA_PLAN,
          new Function<String, Void>() {
            public Void apply(String a0) {
              plan = a0;
              return null;
            }
          });
      try {
        assertQuery(createConnection(), sql, limit, materializationsEnabled,
            hooks, null, null);
        assertNotNull(plan);
      } catch (Exception e) {
        throw new RuntimeException(
            "exception while executing [" + sql + "]", e);
      }
    }

    /** Runs the query and applies a checker to the generated third-party
     * queries. The checker should throw to fail the test if it does not see
     * what it wants. This method can be used to check whether a particular
     * MongoDB or SQL query is generated, for instance. */
    public AssertQuery queryContains(Function<List, Void> predicate1) {
      final List<Object> list = Lists.newArrayList();
      addHook(Hook.QUERY_PLAN,
          new Function<Object, Void>() {
            public Void apply(Object a0) {
              list.add(a0);
              return null;
            }
          });
      try {
        assertQuery(createConnection(), sql, limit, materializationsEnabled,
            hooks, null, null);
        predicate1.apply(list);
        return this;
      } catch (Exception e) {
        throw new RuntimeException(
            "exception while executing [" + sql + "]", e);
      }
    }

    /** Sets a limit on the number of rows returned. -1 means no limit. */
    public AssertQuery limit(int limit) {
      this.limit = limit;
      return this;
    }

    public void sameResultWithMaterializationsDisabled() {
      boolean save = materializationsEnabled;
      try {
        materializationsEnabled = false;
        final boolean ordered = sql.toUpperCase().contains("ORDER BY");
        final Function<ResultSet, Void> checker = consistentResult(ordered);
        returns(checker);
        materializationsEnabled = true;
        returns(checker);
      } finally {
        materializationsEnabled = save;
      }
    }

    public AssertQuery enableMaterializations(boolean enable) {
      this.materializationsEnabled = enable;
      return this;
    }

    /** Adds a hook and a handler for that hook. Calcite will create a thread
     * hook (by calling {@link Hook#addThread(com.google.common.base.Function)})
     * just before running the query, and remove the hook afterwards. */
    public <T> AssertQuery withHook(Hook hook, Function<T, Void> handler) {
      addHook(hook, handler);
      return this;
    }

    private <T> void addHook(Hook hook, Function<T, Void> handler) {
      hooks.add(Pair.of(hook, (Function) handler));
    }
  }

  /** Fluent interface for building a metadata query to be tested. */
  public static class AssertMetaData {
    private final ConnectionFactory connectionFactory;
    private final Function<Connection, ResultSet> function;

    AssertMetaData(ConnectionFactory connectionFactory,
        Function<Connection, ResultSet> function) {
      this.connectionFactory = connectionFactory;
      this.function = function;
    }

    public final AssertMetaData returns(Function<ResultSet, Void> checker) {
      try {
        Connection c = connectionFactory.createConnection();
        final ResultSet resultSet = function.apply(c);
        checker.apply(resultSet);
        resultSet.close();
        c.close();
        return this;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public AssertMetaData returns(String expected) {
      return returns(checkResult(expected));
    }
  }

  /** Connection configuration. Basically, a set of schemas that should be
   * instantiated in the connection. */
  public enum Config {
    /** Configuration that creates an empty connection. */
    EMPTY,

    /**
     * Configuration that creates a connection with two in-memory data sets:
     * {@link org.apache.calcite.test.JdbcTest.HrSchema} and
     * {@link org.apache.calcite.test.JdbcTest.FoodmartSchema}.
     */
    REGULAR,

    /**
     * Configuration that creates a connection with an in-memory data set
     * similar to the smoke test in Cascading Lingual.
     */
    LINGUAL,

    /**
     * Configuration that creates a connection to a MySQL server. Tables
     * such as "customer" and "sales_fact_1997" are available. Queries
     * are processed by generating Java that calls linq4j operators
     * such as
     * {@link org.apache.calcite.linq4j.Enumerable#where(org.apache.calcite.linq4j.function.Predicate1)}.
     */
    JDBC_FOODMART,

    /** Configuration that contains an in-memory clone of the FoodMart
     * database. */
    FOODMART_CLONE,

    /** Configuration that contains an in-memory clone of the FoodMart
     * database, plus a lattice to enable on-the-fly materializations. */
    JDBC_FOODMART_WITH_LATTICE,

    /** Configuration that includes the metadata schema. */
    REGULAR_PLUS_METADATA,

    /** Configuration that loads the "scott/tiger" database. */
    SCOTT,

    /** Configuration that loads Spark. */
    SPARK,
  }

  /** Implementation of {@link AssertQuery} that does nothing. */
  private static class NopAssertQuery extends AssertQuery {
    private NopAssertQuery(String sql) {
      super(null, sql);
    }

    /** Returns an implementation of {@link AssertQuery} that does nothing. */
    static AssertQuery of(final String sql) {
      return new NopAssertQuery(sql);
    }

    @Override protected Connection createConnection() throws Exception {
      throw new AssertionError("disabled");
    }

    @Override public AssertQuery returns(String sql,
        Function<ResultSet, Void> checker) {
      return this;
    }

    @Override public AssertQuery throws_(String message) {
      return this;
    }

    @Override public AssertQuery runs() {
      return this;
    }

    @Override public AssertQuery convertMatches(
        Function<RelNode, Void> checker) {
      return this;
    }

    @Override public AssertQuery substitutionMatches(
        Function<RelNode, Void> checker) {
      return this;
    }

    @Override public AssertQuery planContains(String expected) {
      return this;
    }

    @Override public AssertQuery planHasSql(String expected) {
      return this;
    }

    @Override public AssertQuery
    queryContains(Function<List, Void> predicate1) {
      return this;
    }
  }

  /** Information necessary to create a JDBC connection. Specify one to run
   * tests against a different database. (hsqldb is the default.) */
  public enum DatabaseInstance {
    HSQLDB(
        new ConnectionSpec(FoodmartHsqldb.URI, "FOODMART", "FOODMART",
            "org.hsqldb.jdbcDriver"),
        new ConnectionSpec(ScottHsqldb.URI, ScottHsqldb.USER,
            ScottHsqldb.PASSWORD, "org.hsqldb.jdbcDriver")),
    H2(
        new ConnectionSpec("jdbc:h2:" + getDataSetPath()
            + "/h2/target/foodmart;user=foodmart;password=foodmart",
            "foodmart", "foodmart", "org.h2.Driver"), null),
    MYSQL(
        new ConnectionSpec("jdbc:mysql://localhost/foodmart", "foodmart",
            "foodmart", "com.mysql.jdbc.Driver"), null),
    POSTGRESQL(
        new ConnectionSpec(
            "jdbc:postgresql://localhost/foodmart?user=foodmart&password=foodmart&searchpath=foodmart",
            "foodmart", "foodmart", "org.postgresql.Driver"), null);

    public final ConnectionSpec foodmart;
    public final ConnectionSpec scott;

    private static String getDataSetPath() {
      String path = System.getProperty("calcite.test.dataset");
      if (path != null) {
        return path;
      }
      final String[] dirs = {
        "../calcite-test-dataset",
        "../../calcite-test-dataset"
      };
      for (String s : dirs) {
        if (new File(s).exists() && new File(s, "vm").exists()) {
          return s;
        }
      }
      return ".";
    }

    DatabaseInstance(ConnectionSpec foodmart, ConnectionSpec scott) {
      this.foodmart = foodmart;
      this.scott = scott;
    }
  }

  /** Specification for common test schemas. */
  public enum SchemaSpec {
    REFLECTIVE_FOODMART,
    JDBC_FOODMART,
    CLONE_FOODMART,
    JDBC_FOODMART_WITH_LATTICE,
    HR,
    JDBC_SCOTT,
    SCOTT,
    LINGUAL,
    POST
  }
}

// End CalciteAssert.java
