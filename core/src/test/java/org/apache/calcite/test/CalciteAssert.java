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
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteMetaImpl;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.GeoFunctions;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Closer;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Smalls;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Lists;

import net.hydromatic.foodmart.data.hsqldb.FoodmartHsqldb;
import net.hydromatic.scott.data.hsqldb.ScottHsqldb;

import org.hamcrest.Matcher;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import static org.apache.calcite.test.Matchers.containsStringLinux;
import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
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

  /**
   * Which database to use for tests that require a JDBC data source.
   *
   * @see CalciteSystemProperty#TEST_DB
   **/
  public static final DatabaseInstance DB =
      DatabaseInstance.valueOf(CalciteSystemProperty.TEST_DB.value());

  private static final DateFormat UTC_DATE_FORMAT;
  private static final DateFormat UTC_TIME_FORMAT;
  private static final DateFormat UTC_TIMESTAMP_FORMAT;
  static {
    final TimeZone utc = DateTimeUtils.UTC_ZONE;
    UTC_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
    UTC_DATE_FORMAT.setTimeZone(utc);
    UTC_TIME_FORMAT = new SimpleDateFormat("HH:mm:ss", Locale.ROOT);
    UTC_TIME_FORMAT.setTimeZone(utc);
    UTC_TIMESTAMP_FORMAT =
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT);
    UTC_TIMESTAMP_FORMAT.setTimeZone(utc);
  }

  public static final ConnectionFactory EMPTY_CONNECTION_FACTORY =
      new MapConnectionFactory(ImmutableMap.of(), ImmutableList.of());

  /** Implementation of {@link AssertThat} that does nothing. */
  private static final AssertThat DISABLED =
      new AssertThat(EMPTY_CONNECTION_FACTORY) {
        @Override public AssertThat with(Config config) {
          return this;
        }

        @Override public AssertThat with(ConnectionFactory connectionFactory) {
          return this;
        }

        @Override public AssertThat with(String property, Object value) {
          return this;
        }

        @Override public AssertThat withSchema(String name, Schema schema) {
          return this;
        }

        @Override public AssertQuery query(String sql) {
          return NopAssertQuery.of(sql);
        }

        @Override public AssertThat connectThrows(
            Consumer<Throwable> exceptionChecker) {
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
    return relNode -> {
      if (counter != null) {
        counter.incrementAndGet();
      }
      String s = RelOptUtil.toString(relNode);
      assertThat(s, containsStringLinux(expected));
      return null;
    };
  }

  static Consumer<Throwable> checkException(final String expected) {
    return p0 -> {
      assertNotNull(
          "expected exception but none was thrown", p0);
      String stack = TestUtil.printStackTrace(p0);
      assertTrue(stack, stack.contains(expected));
    };
  }

  static Consumer<Throwable> checkValidationException(final String expected) {
    return new Consumer<Throwable>() {
      @Override public void accept(@Nullable Throwable throwable) {
        assertNotNull("Nothing was thrown", throwable);

        Exception exception = containsCorrectException(throwable);

        assertTrue("Expected to fail at validation, but did not", exception != null);
        if (expected != null) {
          String stack = TestUtil.printStackTrace(exception);
          assertTrue(stack, stack.contains(expected));
        }
      }

      private boolean isCorrectException(Throwable throwable) {
        return throwable instanceof SqlValidatorException
                || throwable instanceof CalciteException;
      }

      private Exception containsCorrectException(Throwable root) {
        Throwable currentCause = root;
        while (currentCause != null) {
          if (isCorrectException(currentCause)) {
            return (Exception) currentCause;
          }
          currentCause = currentCause.getCause();
        }
        return null;
      }
    };
  }

  static Consumer<ResultSet> checkResult(final String expected) {
    return checkResult(expected, new ResultSetFormatter());
  }

  static Consumer<ResultSet> checkResult(final String expected,
      final ResultSetFormatter resultSetFormatter) {
    return resultSet -> {
      try {
        resultSetFormatter.resultSet(resultSet);
        assertThat(resultSetFormatter.string(), isLinux(expected));
      } catch (SQLException e) {
        TestUtil.rethrow(e);
      }
    };
  }

  static Consumer<ResultSet> checkResultValue(final String expected) {
    return resultSet -> {
      try {
        if (!resultSet.next()) {
          throw new AssertionError("too few rows");
        }
        if (resultSet.getMetaData().getColumnCount() != 1) {
          throw new AssertionError("expected 1 column");
        }
        final String resultString = resultSet.getString(1);
        assertThat(resultString,
            expected == null ? nullValue(String.class) : isLinux(expected));
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  public static Consumer<ResultSet> checkResultCount(
      final Matcher<Integer> expected) {
    return resultSet -> {
      try {
        final int count = CalciteAssert.countRows(resultSet);
        assertThat(count, expected);
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  public static Consumer<Integer> checkUpdateCount(final int expected) {
    return updateCount -> {
      assertThat(updateCount, is(expected));
    };
  }

  /** Checks that the result of the second and subsequent executions is the same
   * as the first.
   *
   * @param ordered Whether order should be the same both times
   */
  static Consumer<ResultSet> consistentResult(final boolean ordered) {
    return new Consumer<ResultSet>() {
      int executeCount = 0;
      Collection expected;

      public void accept(ResultSet resultSet) {
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
        } catch (SQLException e) {
          throw TestUtil.rethrow(e);
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

  /** @see Matchers#returnsUnordered(String...) */
  static Consumer<ResultSet> checkResultUnordered(final String... lines) {
    return checkResult(true, false, lines);
  }

  /** @see Matchers#returnsUnordered(String...) */
  static Consumer<ResultSet> checkResult(final boolean sort,
      final boolean head, final String... lines) {
    return resultSet -> {
      try {
        final List<String> expectedList = Lists.newArrayList(lines);
        if (sort) {
          Collections.sort(expectedList);
        }
        final List<String> actualList = new ArrayList<>();
        CalciteAssert.toStringList(resultSet, actualList);
        if (sort) {
          Collections.sort(actualList);
        }
        final List<String> trimmedActualList;
        if (head && actualList.size() > expectedList.size()) {
          trimmedActualList = actualList.subList(0, expectedList.size());
        } else {
          trimmedActualList = actualList;
        }
        if (!trimmedActualList.equals(expectedList)) {
          assertThat(Util.lines(trimmedActualList),
              equalTo(Util.lines(expectedList)));
        }
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  public static Consumer<ResultSet> checkResultContains(
      final String... expected) {
    return s -> {
      try {
        final String actual = toString(s);
        for (String st : expected) {
          assertThat(actual, containsStringLinux(st));
        }
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  public static Consumer<ResultSet> checkResultContains(
      final String expected, final int count) {
    return s -> {
      try {
        final String actual = Util.toLinux(toString(s));
        assertTrue(
            actual + " should have " + count + " occurrence of " + expected,
            StringUtils.countMatches(actual, expected) == count);
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  public static Consumer<ResultSet> checkMaskedResultContains(
      final String expected) {
    return s -> {
      try {
        final String actual = Util.toLinux(toString(s));
        final String maskedActual =
            actual.replaceAll(", id = [0-9]+", "");
        assertThat(maskedActual, containsString(expected));
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  public static Consumer<ResultSet> checkResultType(final String expected) {
    return s -> {
      try {
        final String actual = typeString(s.getMetaData());
        assertEquals(expected, actual);
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  private static String typeString(ResultSetMetaData metaData)
      throws SQLException {
    final List<String> list = new ArrayList<>();
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
      List<Pair<Hook, Consumer>> hooks,
      Consumer<ResultSet> resultChecker,
      Consumer<Integer> updateChecker,
      Consumer<Throwable> exceptionChecker) {
    try (Closer closer = new Closer()) {
      if (connection.isWrapperFor(CalciteConnection.class)) {
        final CalciteConnection calciteConnection =
            connection.unwrap(CalciteConnection.class);
        final Properties properties = calciteConnection.getProperties();
        properties.setProperty(
            CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
            Boolean.toString(materializationsEnabled));
        properties.setProperty(
            CalciteConnectionProperty.CREATE_MATERIALIZATIONS.camelName(),
            Boolean.toString(materializationsEnabled));
        if (!properties
            .containsKey(CalciteConnectionProperty.TIME_ZONE.camelName())) {
          // Do not override id some test has already set this property.
          properties.setProperty(
              CalciteConnectionProperty.TIME_ZONE.camelName(),
              DateTimeUtils.UTC_ZONE.getID());
        }
      }
      for (Pair<Hook, Consumer> hook : hooks) {
        //noinspection unchecked
        closer.add(hook.left.addThread(hook.right));
      }
      Statement statement = connection.createStatement();
      statement.setMaxRows(limit <= 0 ? limit : Math.max(limit, 1));
      ResultSet resultSet = null;
      Integer updateCount = null;
      try {
        if (updateChecker == null) {
          resultSet = statement.executeQuery(sql);
        } else {
          updateCount = statement.executeUpdate(sql);
        }
        if (exceptionChecker != null) {
          exceptionChecker.accept(null);
          return;
        }
      } catch (Exception | Error e) {
        if (exceptionChecker != null) {
          exceptionChecker.accept(e);
          return;
        }
        throw e;
      }
      if (resultChecker != null) {
        resultChecker.accept(resultSet);
      }
      if (updateChecker != null) {
        updateChecker.accept(updateCount);
      }
      if (resultSet != null) {
        resultSet.close();
      }
      statement.close();
      connection.close();
    } catch (Throwable e) {
      String message = "With materializationsEnabled=" + materializationsEnabled
          + ", limit=" + limit;
      if (!TestUtil.hasMessage(e, sql)) {
        message += ", sql=" + sql;
      }
      throw TestUtil.rethrow(e, message);
    }
  }

  private static void assertPrepare(
      Connection connection,
      String sql,
      int limit,
      boolean materializationsEnabled,
      List<Pair<Hook, Consumer>> hooks,
      Consumer<ResultSet> resultChecker,
      Consumer<Integer> updateChecker,
      Consumer<Throwable> exceptionChecker,
      PreparedStatementConsumer consumer) {
    try (Closer closer = new Closer()) {
      if (connection.isWrapperFor(CalciteConnection.class)) {
        final CalciteConnection calciteConnection =
            connection.unwrap(CalciteConnection.class);
        final Properties properties = calciteConnection.getProperties();
        properties.setProperty(
            CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
            Boolean.toString(materializationsEnabled));
        properties.setProperty(
            CalciteConnectionProperty.CREATE_MATERIALIZATIONS.camelName(),
            Boolean.toString(materializationsEnabled));
        if (!properties
            .containsKey(CalciteConnectionProperty.TIME_ZONE.camelName())) {
          // Do not override id some test has already set this property.
          properties.setProperty(
              CalciteConnectionProperty.TIME_ZONE.camelName(),
              DateTimeUtils.UTC_ZONE.getID());
        }
      }
      for (Pair<Hook, Consumer> hook : hooks) {
        //noinspection unchecked
        closer.add(hook.left.addThread(hook.right));
      }
      PreparedStatement statement = connection.prepareStatement(sql);
      statement.setMaxRows(limit <= 0 ? limit : Math.max(limit, 1));
      ResultSet resultSet = null;
      Integer updateCount = null;
      try {
        consumer.accept(statement);
        if (updateChecker == null) {
          resultSet = statement.executeQuery();
        } else {
          updateCount = statement.executeUpdate(sql);
        }
        if (exceptionChecker != null) {
          exceptionChecker.accept(null);
          return;
        }
      } catch (Exception | Error e) {
        if (exceptionChecker != null) {
          exceptionChecker.accept(e);
          return;
        }
        throw e;
      }
      if (resultChecker != null) {
        resultChecker.accept(resultSet);
      }
      if (updateChecker != null) {
        updateChecker.accept(updateCount);
      }
      if (resultSet != null) {
        resultSet.close();
      }
      statement.close();
      connection.close();
    } catch (Throwable e) {
      String message = "With materializationsEnabled=" + materializationsEnabled
          + ", limit=" + limit;
      if (!TestUtil.hasMessage(e, sql)) {
        message += ", sql=" + sql;
      }
      throw TestUtil.rethrow(e, message);
    }
  }

  static void assertPrepare(
      Connection connection,
      String sql,
      boolean materializationsEnabled,
      final Function<RelNode, Void> convertChecker,
      final Function<RelNode, Void> substitutionChecker) {
    try (Closer closer = new Closer()) {
      if (convertChecker != null) {
        closer.add(
            Hook.TRIMMED.addThread((Consumer<RelNode>) convertChecker::apply));
      }
      if (substitutionChecker != null) {
        closer.add(
            Hook.SUB.addThread(
                (Consumer<RelNode>) substitutionChecker::apply));
      }
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
      String message = "With materializationsEnabled=" + materializationsEnabled;
      if (!TestUtil.hasMessage(e, sql)) {
        message += ", sql=" + sql;
      }
      throw TestUtil.rethrow(e, message);
    }
  }

  /** Converts a {@link ResultSet} to a string. */
  static String toString(ResultSet resultSet) throws SQLException {
    return new ResultSetFormatter().resultSet(resultSet).string();
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
    return new ResultSetFormatter().toStringList(resultSet, list);
  }

  static List<String> toList(ResultSet resultSet) throws SQLException {
    return (List<String>) toStringList(resultSet, new ArrayList<String>());
  }

  static ImmutableMultiset<String> toSet(ResultSet resultSet)
      throws SQLException {
    return ImmutableMultiset.copyOf(toList(resultSet));
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
          for (Pair<Object, Class> pair
              : Pair.zip(args, (Class[]) method1.getParameterTypes())) {
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
    final SchemaPlus foodmart;
    final SchemaPlus jdbcScott;
    final SchemaPlus scott;
    final ConnectionSpec cs;
    final DataSource dataSource;
    switch (schema) {
    case REFLECTIVE_FOODMART:
      return rootSchema.add(schema.schemaName,
          new ReflectiveSchema(new JdbcTest.FoodmartSchema()));
    case JDBC_SCOTT:
      cs = DatabaseInstance.HSQLDB.scott;
      dataSource = JdbcSchema.dataSource(cs.url, cs.driver, cs.username,
          cs.password);
      return rootSchema.add(schema.schemaName,
          JdbcSchema.create(rootSchema, schema.schemaName, dataSource,
              cs.catalog, cs.schema));
    case JDBC_FOODMART:
      cs = DB.foodmart;
      dataSource =
          JdbcSchema.dataSource(cs.url, cs.driver, cs.username, cs.password);
      return rootSchema.add(schema.schemaName,
          JdbcSchema.create(rootSchema, schema.schemaName, dataSource,
              cs.catalog, cs.schema));
    case JDBC_FOODMART_WITH_LATTICE:
      foodmart = addSchemaIfNotExists(rootSchema, SchemaSpec.JDBC_FOODMART);
      foodmart.add(schema.schemaName,
          Lattice.create(foodmart.unwrap(CalciteSchema.class),
              "select 1 from \"foodmart\".\"sales_fact_1997\" as s\n"
                  + "join \"foodmart\".\"time_by_day\" as t using (\"time_id\")\n"
                  + "join \"foodmart\".\"customer\" as c using (\"customer_id\")\n"
                  + "join \"foodmart\".\"product\" as p using (\"product_id\")\n"
                  + "join \"foodmart\".\"product_class\" as pc on p.\"product_class_id\" = pc.\"product_class_id\"",
              true));
      return foodmart;
    case SCOTT:
      jdbcScott = addSchemaIfNotExists(rootSchema, SchemaSpec.JDBC_SCOTT);
      return rootSchema.add(schema.schemaName, new CloneSchema(jdbcScott));
    case SCOTT_WITH_TEMPORAL:
      scott = addSchemaIfNotExists(rootSchema, SchemaSpec.SCOTT);
      scott.add("products_temporal", new StreamTest.ProductsTemporalTable());
      scott.add("orders",
          new StreamTest.OrdersHistoryTable(
              StreamTest.OrdersStreamTableFactory.getRowList()));
      return scott;
    case CLONE_FOODMART:
      foodmart = addSchemaIfNotExists(rootSchema, SchemaSpec.JDBC_FOODMART);
      return rootSchema.add("foodmart2", new CloneSchema(foodmart));
    case GEO:
      ModelHandler.addFunctions(rootSchema, null, ImmutableList.of(),
          GeoFunctions.class.getName(), "*", true);
      final SchemaPlus s =
          rootSchema.add(schema.schemaName, new AbstractSchema());
      ModelHandler.addFunctions(s, "countries", ImmutableList.of(),
          CountriesTableFunction.class.getName(), null, false);
      final String sql = "select * from table(\"countries\"(true))";
      final ViewTableMacro viewMacro = ViewTable.viewMacro(rootSchema, sql,
          ImmutableList.of("GEO"), ImmutableList.of(), false);
      s.add("countries", viewMacro);
      return s;
    case HR:
      return rootSchema.add(schema.schemaName,
          new ReflectiveSchema(new JdbcTest.HrSchema()));
    case LINGUAL:
      return rootSchema.add(schema.schemaName,
          new ReflectiveSchema(new JdbcTest.LingualSchema()));
    case BLANK:
      return rootSchema.add(schema.schemaName, new AbstractSchema());
    case ORINOCO:
      final SchemaPlus orinoco =
          rootSchema.add(schema.schemaName, new AbstractSchema());
      orinoco.add("ORDERS",
          new StreamTest.OrdersHistoryTable(
              StreamTest.OrdersStreamTableFactory.getRowList()));
      return orinoco;
    case POST:
      final SchemaPlus post =
          rootSchema.add(schema.schemaName, new AbstractSchema());
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
              ImmutableList.of(), ImmutableList.of("POST", "EMP"),
              null));
      post.add("DEPT",
          ViewTable.viewMacro(post,
              "select * from (values\n"
                  + "    (10, 'Sales'),\n"
                  + "    (20, 'Marketing'),\n"
                  + "    (30, 'Engineering'),\n"
                  + "    (40, 'Empty')) as t(deptno, dname)",
              ImmutableList.of(), ImmutableList.of("POST", "DEPT"),
              null));
      post.add("DEPT30",
          ViewTable.viewMacro(post,
              "select * from dept where deptno = 30",
              ImmutableList.of("POST"), ImmutableList.of("POST", "DEPT30"),
              null));
      post.add("EMPS",
          ViewTable.viewMacro(post,
              "select * from (values\n"
                  + "    (100, 'Fred',  10, CAST(NULL AS CHAR(1)), CAST(NULL AS VARCHAR(20)), 40,               25, TRUE,    FALSE, DATE '1996-08-03'),\n"
                  + "    (110, 'Eric',  20, 'M',                   'San Francisco',           3,                80, UNKNOWN, FALSE, DATE '2001-01-01'),\n"
                  + "    (110, 'John',  40, 'M',                   'Vancouver',               2, CAST(NULL AS INT), FALSE,   TRUE,  DATE '2002-05-03'),\n"
                  + "    (120, 'Wilma', 20, 'F',                   CAST(NULL AS VARCHAR(20)), 1,                 5, UNKNOWN, TRUE,  DATE '2005-09-07'),\n"
                  + "    (130, 'Alice', 40, 'F',                   'Vancouver',               2, CAST(NULL AS INT), FALSE,   TRUE,  DATE '2007-01-01'))\n"
                  + " as t(empno, name, deptno, gender, city, empid, age, slacker, manager, joinedat)",
              ImmutableList.of(), ImmutableList.of("POST", "EMPS"),
              null));
      post.add("TICKER",
          ViewTable.viewMacro(post,
            "select * from (values\n"
                + "    ('ACME', '2017-12-01', 12),\n"
                + "    ('ACME', '2017-12-02', 17),\n"
                + "    ('ACME', '2017-12-03', 19),\n"
                + "    ('ACME', '2017-12-04', 21),\n"
                + "    ('ACME', '2017-12-05', 25),\n"
                + "    ('ACME', '2017-12-06', 12),\n"
                + "    ('ACME', '2017-12-07', 15),\n"
                + "    ('ACME', '2017-12-08', 20),\n"
                + "    ('ACME', '2017-12-09', 24),\n"
                + "    ('ACME', '2017-12-10', 25),\n"
                + "    ('ACME', '2017-12-11', 19),\n"
                + "    ('ACME', '2017-12-12', 15),\n"
                + "    ('ACME', '2017-12-13', 25),\n"
                + "    ('ACME', '2017-12-14', 25),\n"
                + "    ('ACME', '2017-12-15', 14),\n"
                + "    ('ACME', '2017-12-16', 12),\n"
                + "    ('ACME', '2017-12-17', 14),\n"
                + "    ('ACME', '2017-12-18', 24),\n"
                + "    ('ACME', '2017-12-19', 23),\n"
                + "    ('ACME', '2017-12-20', 22))\n"
                + " as t(SYMBOL, tstamp, price)",
            ImmutableList.<String>of(), ImmutableList.of("POST", "TICKER"),
            null));
      return post;
    case AUX:
      SchemaPlus aux =
          rootSchema.add(schema.schemaName, new AbstractSchema());
      TableFunction tableFunction =
          TableFunctionImpl.create(Smalls.SimpleTableFunction.class, "eval");
      aux.add("TBLFUN", tableFunction);
      final String simpleSql = "select *\n"
          + "from (values\n"
          + "    ('ABC', 1),\n"
          + "    ('DEF', 2),\n"
          + "    ('GHI', 3))\n"
          + "  as t(strcol, intcol)";
      aux.add("SIMPLETABLE",
          ViewTable.viewMacro(aux, simpleSql, ImmutableList.of(),
              ImmutableList.of("AUX", "SIMPLETABLE"), null));
      final String lateralSql = "SELECT *\n"
          + "FROM AUX.SIMPLETABLE ST\n"
          + "CROSS JOIN LATERAL TABLE(AUX.TBLFUN(ST.INTCOL))";
      aux.add("VIEWLATERAL",
          ViewTable.viewMacro(aux, lateralSql, ImmutableList.of(),
              ImmutableList.of("AUX", "VIEWLATERAL"), null));
      return aux;
    case BOOKSTORE:
      return rootSchema.add(schema.schemaName,
          new ReflectiveSchema(new BookstoreSchema()));
    default:
      throw new AssertionError("unknown schema " + schema);
    }
  }

  private static SchemaPlus addSchemaIfNotExists(SchemaPlus rootSchema,
        SchemaSpec schemaSpec) {
    final SchemaPlus schema = rootSchema.getSubSchema(schemaSpec.schemaName);
    if (schema != null) {
      return schema;
    }
    return addSchema(rootSchema, schemaSpec);
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
    assertEquals(message, str(expected), str(actual));
  }

  private static String str(Object[] objects) {
    return objects == null
          ? null
          : Arrays.stream(objects).map(Object::toString)
              .collect(Collectors.joining("\n"));
  }

  /** Returns a {@link PropBuilder}. */
  static PropBuilder propBuilder() {
    return new PropBuilder();
  }

  /**
   * Result of calling {@link CalciteAssert#that}.
   */
  public static class AssertThat {
    private final ConnectionFactory connectionFactory;

    private static final AssertThat EMPTY =
        new AssertThat(EMPTY_CONNECTION_FACTORY);

    private AssertThat(ConnectionFactory connectionFactory) {
      this.connectionFactory = Objects.requireNonNull(connectionFactory);
    }

    public AssertThat with(Config config) {
      switch (config) {
      case EMPTY:
        return EMPTY;
      case REGULAR:
        return with(SchemaSpec.HR, SchemaSpec.REFLECTIVE_FOODMART,
            SchemaSpec.POST);
      case REGULAR_PLUS_METADATA:
        return with(SchemaSpec.HR, SchemaSpec.REFLECTIVE_FOODMART);
      case GEO:
        return with(SchemaSpec.GEO)
            .with(CalciteConnectionProperty.CONFORMANCE,
                SqlConformanceEnum.LENIENT);
      case LINGUAL:
        return with(SchemaSpec.LINGUAL);
      case JDBC_FOODMART:
        return with(CalciteAssert.SchemaSpec.JDBC_FOODMART);
      case FOODMART_CLONE:
        return with(SchemaSpec.CLONE_FOODMART);
      case JDBC_FOODMART_WITH_LATTICE:
        return with(SchemaSpec.JDBC_FOODMART_WITH_LATTICE);
      case JDBC_SCOTT:
        return with(SchemaSpec.JDBC_SCOTT);
      case SCOTT:
        return with(SchemaSpec.SCOTT);
      case SPARK:
        return with(CalciteConnectionProperty.SPARK, true);
      case AUX:
        return with(SchemaSpec.AUX, SchemaSpec.POST);
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

    public AssertThat with(String property, Object value) {
      return new AssertThat(connectionFactory.with(property, value));
    }

    public AssertThat with(ConnectionProperty property, Object value) {
      if (!property.type().valid(value, property.valueClass())) {
        throw new IllegalArgumentException();
      }
      return new AssertThat(connectionFactory.with(property, value));
    }

    /** Sets Lex property **/
    public AssertThat with(Lex lex) {
      return with(CalciteConnectionProperty.LEX, lex);
    }

    /** Sets the default schema to a given schema. */
    public AssertThat withSchema(String name, Schema schema) {
      return new AssertThat(
          connectionFactory.with(new AddSchemaPostProcessor(name, schema)));
    }

    /** Sets the default schema of the connection. Schema name may be null. */
    public AssertThat withDefaultSchema(String schema) {
      return new AssertThat(
          connectionFactory.with(new DefaultSchemaPostProcessor(schema)));
    }

    public AssertThat with(ConnectionPostProcessor postProcessor) {
      return new AssertThat(connectionFactory.with(postProcessor));
    }

    public final AssertThat withModel(String model) {
      return with(CalciteConnectionProperty.MODEL, "inline:" + model);
    }

    public final AssertThat withModel(URL model) {
      return with(CalciteConnectionProperty.MODEL,
          Sources.of(model).file().getAbsolutePath());
    }

    public final AssertThat withMaterializations(String model,
         final String... materializations) {
      return withMaterializations(model, false, materializations);
    }

    /** Adds materializations to the schema. */
    public final AssertThat withMaterializations(String model, final boolean existing,
        final String... materializations) {
      return withMaterializations(model, builder -> {
        assert materializations.length % 2 == 0;
        final List<Object> list = builder.list();
        for (int i = 0; i < materializations.length; i++) {
          String table = materializations[i++];
          final Map<String, Object> map = builder.map();
          map.put("table", table);
          if (!existing) {
            map.put("view", table + "v");
          }
          String sql = materializations[i];
          final String sql2 = sql.replaceAll("`", "\"");
          map.put("sql", sql2);
          list.add(map);
        }
        return list;
      });
    }

    /** Adds materializations to the schema. */
    public final AssertThat withMaterializations(String model,
        Function<JsonBuilder, List<Object>> materializations) {
      final JsonBuilder builder = new JsonBuilder();
      final List<Object> list = materializations.apply(builder);
      final String buf =
          "materializations: " + builder.toJsonString(list);
      final String model2;
      if (model.contains("defaultSchema: 'foodmart'")) {
        int endIndex = model.lastIndexOf(']');
        model2 = model.substring(0, endIndex)
            + ", \n{ name: 'mat', "
            + buf
            + "}\n"
            + "]"
            + model.substring(endIndex + 1);
      } else if (model.contains("type: ")) {
        model2 = model.replaceFirst("type: ",
            java.util.regex.Matcher.quoteReplacement(buf + ",\n"
            + "type: "));
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
    public AssertThat connectThrows(Consumer<Throwable> exceptionChecker) {
      Throwable throwable;
      try (Connection x = connectionFactory.createConnection()) {
        try {
          x.close();
        } catch (SQLException e) {
          // ignore
        }
        throwable = null;
      } catch (Throwable e) {
        throwable = e;
      }
      exceptionChecker.accept(throwable);
      return this;
    }

    /** Creates a {@link org.apache.calcite.jdbc.CalciteConnection}
     * and executes a callback. */
    public <T> AssertThat doWithConnection(Function<CalciteConnection, T> fn)
        throws Exception {
      try (Connection connection = connectionFactory.createConnection()) {
        T t = fn.apply((CalciteConnection) connection);
        Util.discard(t);
        return AssertThat.this;
      }
    }

    /** Creates a {@link org.apache.calcite.jdbc.CalciteConnection}
     * and executes a callback that returns no result. */
    public final AssertThat doWithConnection(Consumer<CalciteConnection> fn)
        throws Exception {
      return doWithConnection(c -> {
        fn.accept(c);
        return null;
      });
    }

    /** Creates a {@link DataContext} and executes a callback. */
    public <T> AssertThat doWithDataContext(Function<DataContext, T> fn)
        throws Exception {
      try (CalciteConnection connection =
               (CalciteConnection) connectionFactory.createConnection()) {
        final DataContext dataContext =
            CalciteMetaImpl.createDataContext(connection);
        T t = fn.apply(dataContext);
        Util.discard(t);
        return AssertThat.this;
      }
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

    public ConnectionFactory with(String property, Object value) {
      throw new UnsupportedOperationException();
    }

    public ConnectionFactory with(ConnectionProperty property, Object value) {
      throw new UnsupportedOperationException();
    }

    public ConnectionFactory with(ConnectionPostProcessor postProcessor) {
      throw new UnsupportedOperationException();
    }
  }

  /** Connection post processor */
  @FunctionalInterface
  public interface ConnectionPostProcessor {
    Connection apply(Connection connection) throws SQLException;
  }

  /** Adds {@link Schema} and sets it as default. */
  public static class AddSchemaPostProcessor
      implements ConnectionPostProcessor {
    private final String name;
    private final Schema schema;

    public AddSchemaPostProcessor(String name, Schema schema) {
      this.name = Objects.requireNonNull(name);
      this.schema = Objects.requireNonNull(schema);
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

  /** Sets a default schema name. */
  public static class DefaultSchemaPostProcessor
      implements ConnectionPostProcessor {
    private final String name;

    public DefaultSchemaPostProcessor(String name) {
      this.name = name;
    }

    public Connection apply(Connection connection) throws SQLException {
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
      con.setSchema(schemaSpec.schemaName);
      return connection;
    }
  }

  /** Connection factory that uses the same instance of connections. */
  private static class PoolingConnectionFactory
      extends ConnectionFactory {
    private final PoolingDataSource dataSource;

    PoolingConnectionFactory(final ConnectionFactory factory) {
      final PoolableConnectionFactory connectionFactory =
          new PoolableConnectionFactory(factory::createConnection, null);
      connectionFactory.setRollbackOnReturn(false);
      this.dataSource = new PoolingDataSource<>(
          new GenericObjectPool<>(connectionFactory));
    }

    public Connection createConnection() throws SQLException {
      return dataSource.getConnection();
    }
  }

  /** Connection factory that uses a given map of (name, value) pairs and
   * optionally an initial schema. */
  private static class MapConnectionFactory extends ConnectionFactory {
    private final ImmutableMap<String, String> map;
    private final ImmutableList<ConnectionPostProcessor> postProcessors;

    private MapConnectionFactory(ImmutableMap<String, String> map,
        ImmutableList<ConnectionPostProcessor> postProcessors) {
      this.map = Objects.requireNonNull(map);
      this.postProcessors = Objects.requireNonNull(postProcessors);
    }

    @Override public boolean equals(Object obj) {
      return this == obj
          || obj.getClass() == MapConnectionFactory.class
          && ((MapConnectionFactory) obj).map.equals(map)
          && ((MapConnectionFactory) obj).postProcessors.equals(postProcessors);
    }

    @Override public int hashCode() {
      return Objects.hash(map, postProcessors);
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

    public ConnectionFactory with(String property, Object value) {
      return new MapConnectionFactory(
          FlatLists.append(this.map, property, value.toString()),
          postProcessors);
    }

    public ConnectionFactory with(ConnectionProperty property, Object value) {
      if (!property.type().valid(value, property.valueClass())) {
        throw new IllegalArgumentException();
      }
      return with(property.camelName(), value.toString());
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
    private final List<Pair<Hook, Consumer>> hooks = new ArrayList<>();
    private PreparedStatementConsumer consumer;

    private AssertQuery(ConnectionFactory connectionFactory, String sql) {
      this.sql = sql;
      this.connectionFactory = connectionFactory;
    }

    protected Connection createConnection() {
      try {
        return connectionFactory.createConnection();
      } catch (SQLException e) {
        throw new IllegalStateException(
            "Unable to create connection: connectionFactory = " + connectionFactory, e);
      }
    }

    /** Performs an action using a connection, and closes the connection
     * afterwards. */
    public final AssertQuery withConnection(Consumer<Connection> f) {
      try (Connection c = createConnection()) {
        f.accept(c);
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
      return this;
    }

    public AssertQuery enable(boolean enabled) {
      return enabled ? this : NopAssertQuery.of(sql);
    }

    public AssertQuery returns(String expected) {
      return returns(checkResult(expected));
    }

    /** Similar to {@link #returns}, but trims a few values before
     * comparing. */
    public AssertQuery returns2(final String expected) {
      return returns(
          checkResult(expected,
              new ResultSetFormatter() {
                @Override protected String adjustValue(String s) {
                  if (s != null) {
                    if (s.contains(".")) {
                      while (s.endsWith("0")) {
                        s = s.substring(0, s.length() - 1);
                      }
                      if (s.endsWith(".")) {
                        s = s.substring(0, s.length() - 1);
                      }
                    }
                    if (s.endsWith(" 00:00:00")) {
                      s = s.substring(0, s.length() - " 00:00:00".length());
                    }
                  }
                  return s;
                }
              }));
    }

    public AssertQuery returnsValue(String expected) {
      return returns(checkResultValue(expected));
    }

    public AssertQuery returnsCount(int expectedCount) {
      return returns(checkResultCount(is(expectedCount)));
    }

    public final AssertQuery returns(Consumer<ResultSet> checker) {
      return returns(sql, checker);
    }

    public final AssertQuery updates(int count) {
      return withConnection(connection ->
          assertQuery(connection, sql, limit, materializationsEnabled,
              hooks, null, checkUpdateCount(count), null));
    }

    @SuppressWarnings("Guava")
    @Deprecated // to be removed in 2.0
    public final AssertQuery returns(
        com.google.common.base.Function<ResultSet, Void> checker) {
      return returns(sql, checker::apply);
    }

    protected AssertQuery returns(String sql, Consumer<ResultSet> checker) {
      return withConnection(connection -> {
        if (consumer == null) {
          assertQuery(connection, sql, limit, materializationsEnabled,
              hooks, checker, null, null);
        } else {
          assertPrepare(connection, sql, limit, materializationsEnabled,
              hooks, checker, null, null, consumer);
        }
      });
    }

    public AssertQuery returnsUnordered(String... lines) {
      return returns(checkResult(true, false, lines));
    }

    public AssertQuery returnsOrdered(String... lines) {
      return returns(checkResult(false, false, lines));
    }

    public AssertQuery returnsStartingWith(String... lines) {
      return returns(checkResult(false, true, lines));
    }

    public AssertQuery throws_(String message) {
      return withConnection(connection ->
        assertQuery(connection, sql, limit, materializationsEnabled,
            hooks, null, null, checkException(message)));
    }

    /**
     * Used to check whether a sql statement fails at the SQL Validation phase. More formally,
     * it checks if a {@link SqlValidatorException} or {@link CalciteException} was thrown.
     *
     * @param optionalMessage An optional message to check for in the output stacktrace
     * */
    public AssertQuery failsAtValidation(String optionalMessage) {
      return withConnection(connection ->
        assertQuery(connection, sql, limit, materializationsEnabled,
            hooks, null, null, checkValidationException(optionalMessage)));
    }

    /**
     * Utility method so that one doesn't have to call
     * {@link #failsAtValidation} with {@code null}
     * */
    public AssertQuery failsAtValidation() {
      return failsAtValidation(null);
    }

    public AssertQuery runs() {
      return withConnection(connection -> {
        if (consumer == null) {
          assertQuery(connection, sql, limit, materializationsEnabled,
              hooks, null, null, null);
        } else {
          assertPrepare(connection, sql, limit, materializationsEnabled,
              hooks, null, null, null, consumer);
        }
      });
    }

    public AssertQuery typeIs(String expected) {
      return withConnection(connection ->
        assertQuery(connection, sql, limit, false,
            hooks, checkResultType(expected), null, null));
    }

    /** Checks that when the query (which was set using
     * {@link AssertThat#query(String)}) is converted to a relational algebra
     * expression matching the given string. */
    public final AssertQuery convertContains(final String expected) {
      return convertMatches(checkRel(expected, null));
    }

    public final AssertQuery consumesPreparedStatement(PreparedStatementConsumer consumer) {
      this.consumer =  consumer;
      return this;
    }

    public AssertQuery convertMatches(final Function<RelNode, Void> checker) {
      return withConnection(connection ->
        assertPrepare(connection, sql, this.materializationsEnabled,
            checker, null));
    }

    public AssertQuery substitutionMatches(
        final Function<RelNode, Void> checker) {
      return withConnection(connection ->
        assertPrepare(connection, sql, materializationsEnabled, null, checker));
    }

    public AssertQuery explainContains(String expected) {
      return explainMatches("", checkResultContains(expected));
    }

    public final AssertQuery explainMatches(String extra,
        Consumer<ResultSet> checker) {
      return returns("explain plan " + extra + "for " + sql, checker);
    }

    public AssertQuery planContains(String expected) {
      ensurePlan(null);
      assertTrue(
          "Plan [" + plan + "] contains [" + expected + "]",
          Util.toLinux(plan)
              .replaceAll("\\\\r\\\\n", "\\\\n")
              .contains(expected));
      return this;
    }

    public AssertQuery planUpdateHasSql(String expected, int count) {
      ensurePlan(checkUpdateCount(count));
      expected = ".unwrap(javax.sql.DataSource.class), \""
          + expected.replace("\\", "\\\\")
          .replace("\"", "\\\"")
          .replaceAll("\n", "\\\\n")
          + "\"";
      assertTrue(
          "Plan [" + plan + "] contains [" + expected + "]",
          Util.toLinux(plan)
              .replaceAll("\\\\r\\\\n", "\\\\n")
              .contains(expected));
      return this;
    }

    public AssertQuery planHasSql(String expected) {
      return planContains(
          ".unwrap(javax.sql.DataSource.class), \""
              + expected.replace("\\", "\\\\")
              .replace("\"", "\\\"")
              .replaceAll("\n", "\\\\n")
              + "\"");
    }

    private void ensurePlan(Consumer<Integer> checkUpdate) {
      if (plan != null) {
        return;
      }
      addHook(Hook.JAVA_PLAN, this::setPlan);
      withConnection(connection -> {
        assertQuery(connection, sql, limit, materializationsEnabled,
            hooks, null, checkUpdate, null);
        assertNotNull(plan);
      });
    }

    private void setPlan(String plan) {
      this.plan = plan;
    }

    /** Runs the query and applies a checker to the generated third-party
     * queries. The checker should throw to fail the test if it does not see
     * what it wants. This method can be used to check whether a particular
     * MongoDB or SQL query is generated, for instance. */
    public AssertQuery queryContains(Consumer<List> predicate1) {
      final List<Object> list = new ArrayList<>();
      addHook(Hook.QUERY_PLAN, list::add);
      return withConnection(connection -> {
        assertQuery(connection, sql, limit, materializationsEnabled,
            hooks, null, null, null);
        predicate1.accept(list);
      });
    }

    /** @deprecated Use {@link #queryContains(Consumer)}. */
    @SuppressWarnings("Guava")
    @Deprecated // to be removed before 2.0
    public final AssertQuery queryContains(
        com.google.common.base.Function<List, Void> predicate1) {
      return queryContains((Consumer<List>) predicate1::apply);
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
        final boolean ordered =
            sql.toUpperCase(Locale.ROOT).contains("ORDER BY");
        final Consumer<ResultSet> checker = consistentResult(ordered);
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

    @SuppressWarnings("Guava")
    @Deprecated // to be removed in 2.0
    public <T> AssertQuery withHook(Hook hook, Function<T, Void> handler) {
      return withHook(hook, (Consumer<T>) handler::apply);
    }

    /** Adds a hook and a handler for that hook. Calcite will create a thread
     * hook (by calling {@link Hook#addThread(Consumer)})
     * just before running the query, and remove the hook afterwards. */
    public <T> AssertQuery withHook(Hook hook, Consumer<T> handler) {
      addHook(hook, handler);
      return this;
    }

    private <T> void addHook(Hook hook, Consumer<T> handler) {
      hooks.add(Pair.of(hook, handler));
    }

    /** Adds a property hook. */
    public <V> AssertQuery withProperty(Hook hook, V value) {
      return withHook(hook, Hook.propertyJ(value));
    }

    /** Adds a factory to create a {@link RelNode} query. This {@code RelNode}
     * will be used instead of the SQL string. */
    public AssertQuery withRel(final Function<RelBuilder, RelNode> relFn) {
      return withHook(Hook.STRING_TO_QUERY,
          (Consumer<Pair<FrameworkConfig, Holder<CalcitePrepare.Query>>>)
          pair -> {
            final FrameworkConfig config = forceDecorrelate(pair.left);
            final RelBuilder b = RelBuilder.create(config);
            pair.right.set(CalcitePrepare.Query.of(relFn.apply(b)));
          });
    }

    /** Creates a {@link FrameworkConfig} that does not decorrelate. */
    private FrameworkConfig forceDecorrelate(FrameworkConfig config) {
      return Frameworks.newConfigBuilder(config)
          .context(
              Contexts.of(new CalciteConnectionConfigImpl(new Properties())
                  .set(CalciteConnectionProperty.FORCE_DECORRELATE,
                      Boolean.toString(false))))
          .build();
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

    public final AssertMetaData returns(Consumer<ResultSet> checker) {
      try (Connection c = connectionFactory.createConnection()) {
        final ResultSet resultSet = function.apply(c);
        checker.accept(resultSet);
        resultSet.close();
        c.close();
        return this;
      } catch (Throwable e) {
        throw TestUtil.rethrow(e);
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

    /**
     * Configuration that creates a connection to hsqldb containing the
     * Scott schema via the JDBC adapter.
     */
    JDBC_SCOTT,

    /** Configuration that contains an in-memory clone of the FoodMart
     * database. */
    FOODMART_CLONE,

    /** Configuration that contains geo-spatial functions. */
    GEO,

    /** Configuration that contains an in-memory clone of the FoodMart
     * database, plus a lattice to enable on-the-fly materializations. */
    JDBC_FOODMART_WITH_LATTICE,

    /** Configuration that includes the metadata schema. */
    REGULAR_PLUS_METADATA,

    /** Configuration that loads the "scott/tiger" database. */
    SCOTT,

    /** Configuration that loads Spark. */
    SPARK,

    /** Configuration that loads AUX schema for tests involving view expansions
     * and lateral joins tests. */
    AUX
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

    @Override protected Connection createConnection() {
      throw new AssertionError("disabled");
    }

    @Override public AssertQuery returns(String sql,
        Consumer<ResultSet> checker) {
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

    @Override public AssertQuery planUpdateHasSql(String expected, int count) {
      return this;
    }

    @Override public AssertQuery queryContains(Consumer<List> predicate1) {
      return this;
    }
  }

  /** Information necessary to create a JDBC connection. Specify one to run
   * tests against a different database. (hsqldb is the default.) */
  public enum DatabaseInstance {
    HSQLDB(
        new ConnectionSpec(FoodmartHsqldb.URI, "FOODMART", "FOODMART",
            "org.hsqldb.jdbcDriver", "foodmart"),
        new ConnectionSpec(ScottHsqldb.URI, ScottHsqldb.USER,
            ScottHsqldb.PASSWORD, "org.hsqldb.jdbcDriver", "SCOTT")),
    H2(
        new ConnectionSpec("jdbc:h2:" + CalciteSystemProperty.TEST_DATASET_PATH.value()
            + "/h2/target/foodmart;user=foodmart;password=foodmart",
            "foodmart", "foodmart", "org.h2.Driver", "foodmart"), null),
    MYSQL(
        new ConnectionSpec("jdbc:mysql://localhost/foodmart", "foodmart",
            "foodmart", "com.mysql.jdbc.Driver", "foodmart"), null),
    ORACLE(
        new ConnectionSpec("jdbc:oracle:thin:@localhost:1521:XE", "foodmart",
            "foodmart", "oracle.jdbc.OracleDriver", "FOODMART"), null),
    POSTGRESQL(
        new ConnectionSpec(
            "jdbc:postgresql://localhost/foodmart?user=foodmart&password=foodmart&searchpath=foodmart",
            "foodmart", "foodmart", "org.postgresql.Driver", "foodmart"), null);

    public final ConnectionSpec foodmart;
    public final ConnectionSpec scott;

    DatabaseInstance(ConnectionSpec foodmart, ConnectionSpec scott) {
      this.foodmart = foodmart;
      this.scott = scott;
    }
  }

  /** Specification for common test schemas. */
  public enum SchemaSpec {
    REFLECTIVE_FOODMART("foodmart"),
    JDBC_FOODMART("foodmart"),
    CLONE_FOODMART("foodmart2"),
    JDBC_FOODMART_WITH_LATTICE("lattice"),
    GEO("GEO"),
    HR("hr"),
    JDBC_SCOTT("JDBC_SCOTT"),
    SCOTT("scott"),
    SCOTT_WITH_TEMPORAL("scott_temporal"),
    BLANK("BLANK"),
    LINGUAL("SALES"),
    POST("POST"),
    ORINOCO("ORINOCO"),
    AUX("AUX"),
    BOOKSTORE("bookstore");

    /** The name of the schema that is usually created from this specification.
     * (Names are not unique, and you can use another name if you wish.) */
    public final String schemaName;

    SchemaSpec(String schemaName) {
      this.schemaName = schemaName;
    }
  }

  /** Converts a {@link ResultSet} to string. */
  static class ResultSetFormatter {
    final StringBuilder buf = new StringBuilder();

    public ResultSetFormatter resultSet(ResultSet resultSet)
        throws SQLException {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      while (resultSet.next()) {
        rowToString(resultSet, metaData);
        buf.append("\n");
      }
      return this;
    }

    /** Converts one row to a string. */
    ResultSetFormatter rowToString(ResultSet resultSet,
        ResultSetMetaData metaData) throws SQLException {
      int n = metaData.getColumnCount();
      if (n > 0) {
        for (int i = 1;; i++) {
          buf.append(metaData.getColumnLabel(i))
              .append("=")
              .append(adjustValue(resultSet.getString(i)));
          if (i == n) {
            break;
          }
          buf.append("; ");
        }
      }
      return this;
    }

    protected String adjustValue(String string) {
      return string;
    }

    public Collection<String> toStringList(ResultSet resultSet,
        Collection<String> list) throws SQLException {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      while (resultSet.next()) {
        rowToString(resultSet, metaData);
        list.add(buf.toString());
        buf.setLength(0);
      }
      return list;
    }

    /** Flushes the buffer and returns its previous contents. */
    public String string() {
      String s = buf.toString();
      buf.setLength(0);
      return s;
    }
  }

  /** Builds a {@link java.util.Properties} containing connection property
   * settings. */
  static class PropBuilder {
    final Properties properties = new Properties();

    PropBuilder set(CalciteConnectionProperty p, String v) {
      properties.setProperty(p.camelName(), v);
      return this;
    }

    Properties build() {
      return properties;
    }
  }

  /**
   * We want a consumer which can throw SqlException
   */
  public interface PreparedStatementConsumer {
    void accept(PreparedStatement statement) throws SQLException;
  }
}

// End CalciteAssert.java
