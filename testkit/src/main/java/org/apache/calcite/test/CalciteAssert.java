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
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteMetaImpl;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.runtime.AccumOperation;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.CollectOperation;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.SpatialTypeFunctions;
import org.apache.calcite.runtime.UnionOperation;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlSpatialTypeFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.test.schemata.bookstore.BookstoreSchema;
import org.apache.calcite.test.schemata.countries.CountriesTableFunction;
import org.apache.calcite.test.schemata.countries.StatesTableFunction;
import org.apache.calcite.test.schemata.foodmart.FoodmartSchema;
import org.apache.calcite.test.schemata.hr.HrSchema;
import org.apache.calcite.test.schemata.lingual.LingualSchema;
import org.apache.calcite.test.schemata.orderstream.OrdersHistoryTable;
import org.apache.calcite.test.schemata.orderstream.OrdersStreamTableFactory;
import org.apache.calcite.test.schemata.orderstream.ProductsTemporalTable;
import org.apache.calcite.test.schemata.tpch.TpchSchema;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import net.hydromatic.foodmart.data.hsqldb.FoodmartHsqldb;
import net.hydromatic.scott.data.hsqldb.ScottHsqldb;
import net.hydromatic.steelwheels.data.hsqldb.SteelwheelsHsqldb;

import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matcher;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.sql.DataSource;

import static org.apache.calcite.test.Matchers.compose;
import static org.apache.calcite.test.Matchers.containsStringLinux;
import static org.apache.calcite.test.Matchers.isLinux;

import static org.apache.commons.lang3.StringUtils.countMatches;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import static java.util.Objects.requireNonNull;

/**
 * Fluid DSL for testing Calcite connections and queries.
 */
@SuppressWarnings("rawtypes")
public class CalciteAssert {
  private CalciteAssert() {}

  /**
   * Which database to use for tests that require a JDBC data source.
   *
   * @see CalciteSystemProperty#TEST_DB
   */
  public static final DatabaseInstance DB =
      DatabaseInstance.valueOf(CalciteSystemProperty.TEST_DB.value());

  private static String testMysqlUrl = "jdbc:mysql://localhost/foodmart";

  private static String testMysqlDriver = "com.mysql.jdbc.Driver";

  /** Implementation of {@link AssertThat} that does nothing. */
  private static final AssertThat DISABLED =
      new AssertThat(ConnectionFactories.empty(), ImmutableList.of()) {
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
            Function<CalciteConnection, T> fn) {
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

        @Override public AssertThat with(SqlConformanceEnum conformance) {
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
   * {@code CalciteAssert.that().with(Config.EMPTY).withModel(model)}. */
  public static AssertThat model(String model) {
    return that().withModel(model);
  }

  /** Short-hand for {@code CalciteAssert.that().with(Config.REGULAR)}. */
  public static AssertThat hr() {
    return that(Config.REGULAR);
  }

  /** Adds a Pair to a List. */
  private static <K, V> ImmutableList<Pair<K, V>> addPair(List<Pair<K, V>> list,
      K k, V v) {
    return ImmutableList.<Pair<K, V>>builder()
        .addAll(list)
        .add(Pair.of(k, v))
        .build();
  }

  static Consumer<RelNode> checkRel(final String expected,
      final @Nullable AtomicInteger counter) {
    return relNode -> {
      if (counter != null) {
        counter.incrementAndGet();
      }
      String s = RelOptUtil.toString(relNode);
      assertThat(s, containsStringLinux(expected));
    };
  }

  static Consumer<Throwable> checkException(final String expected) {
    return p0 -> {
      assertNotNull(p0, "expected exception but none was thrown");
      String stack = TestUtil.printStackTrace(p0);
      assertThat(stack, containsString(expected));
    };
  }

  static Consumer<Throwable> checkValidationException(
      final @Nullable String expected) {
    return new Consumer<Throwable>() {
      @Override public void accept(@Nullable Throwable throwable) {
        assertNotNull(throwable, "Nothing was thrown");

        Exception exception = containsCorrectException(throwable);

        assertNotNull(exception, "Expected to fail at validation, but did not");
        if (expected != null) {
          String stack = TestUtil.printStackTrace(exception);
          assertThat(stack, containsString(expected));
        }
      }

      private boolean isCorrectException(Throwable throwable) {
        return throwable instanceof SqlValidatorException
            || throwable instanceof CalciteException;
      }

      private @Nullable Exception containsCorrectException(Throwable root) {
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
    return updateCount -> assertThat(updateCount, is(expected));
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

      @Override public void accept(ResultSet resultSet) {
        ++executeCount;
        try {
          final Collection result =
              CalciteAssert.toStringList(resultSet,
                  ordered ? new ArrayList<>() : new TreeSet<>());
          if (executeCount == 1) {
            expected = result;
          } else {
            @SuppressWarnings("UndefinedEquals")
            boolean matches = expected.equals(result);
            if (!matches) {
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

  /** Checks that the {@link ResultSet} returns the given set of lines, in no
   * particular order.
   *
   * @see Matchers#returnsUnordered(String...) */
  static Consumer<ResultSet> checkResultUnordered(final String... lines) {
    return checkResult(true, false, lines);
  }

  /** Checks that the {@link ResultSet} returns the given set of lines,
   * optionally sorting.
   *
   * <p>The lines must not contain line breaks. If you have written
   *
   * <pre>{@code
   * checkUnordered("line1\n"
   *     + "line2")
   * }</pre>
   *
   * <p>you should instead write
   *
   * <pre>{@code
   * checkUnordered("line1",
   *     "line2")
   * }</pre>
   *
   * <p>so that result-checking is order-independent.
   *
   * @see Matchers#returnsUnordered(String...) */
  static Consumer<ResultSet> checkResult(final boolean sort,
      final boolean head, final String... lines) {
    // Check that none of the lines contains a line break.
    for (String line : lines) {
      if (line.contains("\n")) {
        throw new AssertionError("expected line has line breaks: " + line);
      }
    }

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
        assertThat(actual + " should have " + count + " occurrence of "
                + expected,
            countMatches(actual, expected), is(count));
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
        final String maskedActual = Matchers.trimNodeIds(actual);
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
        assertThat(actual, is(expected));
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
              ? RelDataTypeImpl.NON_NULLABLE_SUFFIX
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
      @Nullable Consumer<ResultSet> resultChecker,
      @Nullable Consumer<Integer> updateChecker,
      @Nullable Consumer<Throwable> exceptionChecker) {
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
      statement.setMaxRows(Math.max(limit, 0));
      ResultSet resultSet = null;
      Integer updateCount = null;
      try {
        if (updateChecker == null) {
          resultSet = statement.executeQuery(sql);
          if (resultChecker == null && exceptionChecker != null) {
            // Pull data from result set, otherwise exceptions that happen during evaluation
            // won't be triggered
            while (resultSet.next()) {
              // no need to do anything with the data
            }
          }
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
      @Nullable Consumer<ResultSet> resultChecker,
      @Nullable Consumer<Integer> updateChecker,
      @Nullable Consumer<Throwable> exceptionChecker,
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
      statement.setMaxRows(Math.max(limit, 0));
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
      final Consumer<RelNode> convertChecker,
      final Consumer<RelNode> substitutionChecker) {
    assertPrepare(connection, sql, materializationsEnabled, ImmutableList.of(),
        convertChecker, substitutionChecker);
  }

  static void assertPrepare(
      Connection connection,
      String sql,
      boolean materializationsEnabled,
      List<Pair<Hook, Consumer>> hooks,
      final @Nullable Consumer<RelNode> convertChecker,
      final @Nullable Consumer<RelNode> substitutionChecker) {
    try (Closer closer = new Closer()) {
      if (convertChecker != null) {
        closer.add(
            Hook.TRIMMED.addThread(convertChecker));
      }
      if (substitutionChecker != null) {
        closer.add(
            Hook.SUB.addThread(substitutionChecker));
      }
      for (Pair<Hook, Consumer> hook : hooks) {
        closer.add(hook.left.addThread(hook.right));
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
  public static String toString(ResultSet resultSet) throws SQLException {
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
      throws InvocationTargetException, IllegalAccessException {
    return method(o, methodName, args).invoke(o, args);
  }

  /** Finds a non-static method based on its target, name and arguments.
   * Throws if not found. */
  static Method method(Object o, String methodName, Object[] args) {
    for (Class<?> aClass = o.getClass();;) {
    loop:
      for (Method method1 : aClass.getMethods()) {
        if (method1.getName().equals(methodName)
            && method1.getParameterCount() == args.length
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

  /** Adds a schema specification (or specifications) to the root schema,
   * returning the last one created. */
  public static SchemaPlus addSchema(SchemaPlus rootSchema,
      SchemaSpec... schemas) {
    SchemaPlus s = rootSchema;
    for (SchemaSpec schema : schemas) {
      s = addSchema_(rootSchema, schema);
    }
    return s;
  }

  static SchemaPlus addSchema_(SchemaPlus rootSchema, SchemaSpec schema) {
    final SchemaPlus foodmart;
    final SchemaPlus jdbcScott;
    final SchemaPlus jdbcSteelwheels;
    final SchemaPlus scott;
    final ConnectionSpec cs;
    final DataSource dataSource;
    final ImmutableList<String> emptyPath = ImmutableList.of();
    switch (schema) {
    case REFLECTIVE_FOODMART:
      return rootSchema.add(schema.schemaName,
          new ReflectiveSchema(new FoodmartSchema()));
    case JDBC_SCOTT:
      cs = requireNonNull(DatabaseInstance.HSQLDB.scott);
      dataSource =
          JdbcSchema.dataSource(cs.url, cs.driver, cs.username, cs.password);
      return rootSchema.add(schema.schemaName,
          JdbcSchema.create(rootSchema, schema.schemaName, dataSource,
              cs.catalog, cs.schema));
    case JDBC_STEELWHEELS:
      cs = requireNonNull(DatabaseInstance.HSQLDB.steelwheels);
      dataSource =
          JdbcSchema.dataSource(cs.url, cs.driver, cs.username, cs.password);
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
      final CalciteSchema foodmartSchema =
          requireNonNull(foodmart.unwrap(CalciteSchema.class));
      foodmart.add(schema.schemaName,
          Lattice.create(foodmartSchema,
              "select 1 from \"foodmart\".\"sales_fact_1997\" as s\n"
                  + "join \"foodmart\".\"time_by_day\" as t using (\"time_id\")\n"
                  + "join \"foodmart\".\"customer\" as c using (\"customer_id\")\n"
                  + "join \"foodmart\".\"product\" as p using (\"product_id\")\n"
                  + "join \"foodmart\".\"product_class\" as pc on p.\"product_class_id\" = pc.\"product_class_id\"",
              true));
      return foodmart;

    case MY_DB:
      return rootSchema.add(schema.schemaName, MY_DB_SCHEMA);
    case UNSIGNED_TYPE:
      return rootSchema.add(schema.schemaName, UNSIGNED_TYPE_SCHEMA);
    case SCOTT:
      jdbcScott = addSchemaIfNotExists(rootSchema, SchemaSpec.JDBC_SCOTT);
      return rootSchema.add(schema.schemaName, new CloneSchema(jdbcScott));
    case SCOTT_WITH_TEMPORAL:
      scott = addSchemaIfNotExists(rootSchema, SchemaSpec.SCOTT);
      scott.add("products_temporal", new ProductsTemporalTable());
      scott.add("orders",
          new OrdersHistoryTable(
              OrdersStreamTableFactory.getRowList()));
      return scott;

    case STEELWHEELS:
      jdbcSteelwheels = addSchemaIfNotExists(rootSchema, SchemaSpec.JDBC_STEELWHEELS);
      return rootSchema.add(schema.schemaName, new CloneSchema(jdbcSteelwheels));

    case TPCH:
      return rootSchema.add(schema.schemaName,
          new ReflectiveSchema(new TpchSchema()));

    case CLONE_FOODMART:
      foodmart = addSchemaIfNotExists(rootSchema, SchemaSpec.JDBC_FOODMART);
      return rootSchema.add("foodmart2", new CloneSchema(foodmart));
    case GEO:
      ModelHandler.addFunctions(rootSchema, null, emptyPath,
          SpatialTypeFunctions.class.getName(), "*", true);
      ModelHandler.addFunctions(rootSchema, null, emptyPath,
          SqlSpatialTypeFunctions.class.getName(), "*", true);
      rootSchema.add("ST_UNION",
          requireNonNull(AggregateFunctionImpl.create(UnionOperation.class)));
      rootSchema.add("ST_ACCUM",
          requireNonNull(AggregateFunctionImpl.create(AccumOperation.class)));
      rootSchema.add("ST_COLLECT",
          requireNonNull(AggregateFunctionImpl.create(CollectOperation.class)));
      final SchemaPlus s =
          rootSchema.add(schema.schemaName, new AbstractSchema());
      ModelHandler.addFunctions(s, "countries", emptyPath,
          CountriesTableFunction.class.getName(), null, false);
      final String sql = "select * from table(\"countries\"(true))";
      final ViewTableMacro viewMacro =
          ViewTable.viewMacro(rootSchema, sql,
              ImmutableList.of("GEO"), emptyPath, false);
      s.add("countries", viewMacro);
      ModelHandler.addFunctions(s, "states", emptyPath,
          StatesTableFunction.class.getName(), "states", false);
      final String sql2 = "select \"name\",\n"
          + " ST_PolyFromText(\"geom\") as \"geom\"\n"
          + "from table(\"states\"(true))";
      final ViewTableMacro viewMacro2 =
          ViewTable.viewMacro(rootSchema, sql2,
              ImmutableList.of("GEO"), emptyPath, false);
      s.add("states", viewMacro2);

      ModelHandler.addFunctions(s, "parks", emptyPath,
          StatesTableFunction.class.getName(), "parks", false);
      final String sql3 = "select \"name\",\n"
          + " ST_PolyFromText(\"geom\") as \"geom\"\n"
          + "from table(\"parks\"(true))";
      final ViewTableMacro viewMacro3 =
          ViewTable.viewMacro(rootSchema, sql3,
              ImmutableList.of("GEO"), emptyPath, false);
      s.add("parks", viewMacro3);

      return s;
    case HR:
      return rootSchema.add(schema.schemaName,
          new ReflectiveSchemaWithoutRowCount(new HrSchema()));
    case LINGUAL:
      return rootSchema.add(schema.schemaName,
          new ReflectiveSchema(new LingualSchema()));
    case BLANK:
      return rootSchema.add(schema.schemaName, new AbstractSchema());
    case ORINOCO:
      final SchemaPlus orinoco =
          rootSchema.add(schema.schemaName, new AbstractSchema());
      orinoco.add("ORDERS",
          new OrdersHistoryTable(
              OrdersStreamTableFactory.getRowList()));
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
              emptyPath, ImmutableList.of("POST", "EMP"),
              null));
      post.add("DEPT",
          ViewTable.viewMacro(post,
              "select * from (values\n"
                  + "    (10, 'Sales'),\n"
                  + "    (20, 'Marketing'),\n"
                  + "    (30, 'Engineering'),\n"
                  + "    (40, 'Empty')) as t(deptno, dname)",
              emptyPath, ImmutableList.of("POST", "DEPT"),
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
              emptyPath, ImmutableList.of("POST", "EMPS"),
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
            ImmutableList.of(), ImmutableList.of("POST", "TICKER"),
            null));
      post.add("EMPS_DATE_TIME",
          ViewTable.viewMacro(post,
              "select * from (values\n"
                  + "    (100, 'Fred',  10, CAST(NULL AS CHAR(1)), CAST(NULL AS VARCHAR(20)), 40,               25, TRUE,    FALSE, DATE '1996-08-03', TIME '16:22:34', TIMESTAMP '1996-08-03 16:22:34'),\n"
                  + "    (110, 'Eric',  20, 'M',                   'San Francisco',           3,                80, UNKNOWN, FALSE, DATE '2001-01-01', TIME '12:20:00', TIMESTAMP '2001-01-01 12:20:00'),\n"
                  + "    (110, 'John',  40, 'M',                   'Vancouver',               2, CAST(NULL AS INT), FALSE,   TRUE,  DATE '2002-05-03', TIME '13:12:14', TIMESTAMP '2002-05-03 13:12:14'),\n"
                  + "    (120, 'Wilma', 20, 'F',                   CAST(NULL AS VARCHAR(20)), 1,                 5, UNKNOWN, TRUE,  DATE '2005-09-07', TIME '06:02:04', TIMESTAMP '2005-09-07 06:02:04'),\n"
                  + "    (130, 'Alice', 40, 'F',                   'Vancouver',               2, CAST(NULL AS INT), FALSE,   TRUE,  DATE '2007-01-01', TIME '23:09:59', TIMESTAMP '2007-01-01 23:09:59'))\n"
                  + " as t(empno, name, deptno, gender, city, empid, age, slacker, manager, joinedat, joinetime, joinetimestamp)",
              emptyPath, ImmutableList.of("POST", "EMPS_DATE_TIME"),
              null));
      return post;
    case FAKE_FOODMART:
      // Similar to FOODMART, but not based on JdbcSchema.
      // Contains 2 tables that do not extend JdbcTable.
      // They redirect requests for SqlDialect and DataSource to the real JDBC
      // FOODMART, and this allows statistics queries to be executed.
      foodmart = addSchemaIfNotExists(rootSchema, SchemaSpec.JDBC_FOODMART);
      final Wrapper salesTable =
          requireNonNull((Wrapper) foodmart.tables().get("sales_fact_1997"));
      SchemaPlus fake =
          rootSchema.add(schema.schemaName, new AbstractSchema());
      fake.add("time_by_day", new AbstractTable() {
        @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return typeFactory.builder()
              .add("time_id", SqlTypeName.INTEGER)
              .add("the_year", SqlTypeName.INTEGER)
              .build();
        }

        @Override public <C> C unwrap(Class<C> aClass) {
          if (aClass.isAssignableFrom(SqlDialect.class)
              || aClass.isAssignableFrom(DataSource.class)) {
            return salesTable.unwrap(aClass);
          }
          return super.unwrap(aClass);
        }
      });
      fake.add("sales_fact_1997", new AbstractTable() {
        @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return typeFactory.builder()
              .add("time_id", SqlTypeName.INTEGER)
              .add("customer_id", SqlTypeName.INTEGER)
              .build();
        }

        @Override public <C> C unwrap(Class<C> aClass) {
          if (aClass.isAssignableFrom(SqlDialect.class)
              || aClass.isAssignableFrom(DataSource.class)) {
            return salesTable.unwrap(aClass);
          }
          return super.unwrap(aClass);
        }
      });
      return fake;
    case AUX:
      SchemaPlus aux =
          rootSchema.add(schema.schemaName, new AbstractSchema());
      aux.add("TBLFUN",
          requireNonNull(
              TableFunctionImpl.create(Smalls.SimpleTableFunction.class,
                  "eval")));
      aux.add("TBLFUN_IDENTITY",
          requireNonNull(
              TableFunctionImpl.create(Smalls.IdentityTableFunction.class,
                  "eval")));
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
    final SchemaPlus schema = rootSchema.subSchemas().get(schemaSpec.schemaName);
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
   * {@link org.junit.jupiter.api.Assertions#assertArrayEquals(Object[], Object[], String)}
   *
   * @param message the identifying message for the {@link AssertionError} (<code>null</code>
   * okay)
   * @param expected expected value
   * @param actual actual value
   */
  public static void assertArrayEqual(
      String message, Object[] expected, Object[] actual) {
    assertThat(message, str(actual), is(str(expected)));
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
    private final ImmutableList<Pair<Hook, Consumer>> hooks;

    private static final AssertThat EMPTY =
        new AssertThat(ConnectionFactories.empty(), ImmutableList.of());

    private AssertThat(ConnectionFactory connectionFactory,
        ImmutableList<Pair<Hook, Consumer>> hooks) {
      this.connectionFactory =
          requireNonNull(connectionFactory, "connectionFactory");
      this.hooks = requireNonNull(hooks, "hooks");
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

    /** Creates a copy of this AssertThat, adding more schemas. */
    public AssertThat with(SchemaSpec... specs) {
      AssertThat next = this;
      for (SchemaSpec spec : specs) {
        next = next.with(ConnectionFactories.add(spec));
      }
      return next;
    }

    /** Creates a copy of this AssertThat, overriding the connection factory. */
    public AssertThat with(ConnectionFactory connectionFactory) {
      return new AssertThat(connectionFactory, hooks);
    }

    /** Adds a hook and a handler for that hook. Calcite will create a thread
     * hook (by calling {@link Hook#addThread(Consumer)})
     * just before running the query, and remove the hook afterwards. */
    public <T> AssertThat withHook(Hook hook, Consumer<T> handler) {
      return new AssertThat(connectionFactory,
          addPair(this.hooks, hook, handler));
    }

    public final AssertThat with(final Map<String, String> map) {
      AssertThat x = this;
      for (Map.Entry<String, String> entry : map.entrySet()) {
        x = with(entry.getKey(), entry.getValue());
      }
      return x;
    }

    public AssertThat with(String property, Object value) {
      return with(connectionFactory.with(property, value));
    }

    public AssertThat with(ConnectionProperty property, Object value) {
      if (!property.type().valid(value, property.valueClass())) {
        throw new IllegalArgumentException();
      }
      return with(connectionFactory.with(property, value));
    }

    /** Sets the Lex property. */
    public AssertThat with(Lex lex) {
      return with(CalciteConnectionProperty.LEX, lex);
    }

    /** Sets the conformance property. */
    public AssertThat with(SqlConformanceEnum conformance) {
      return with(CalciteConnectionProperty.CONFORMANCE, conformance);
    }

    /** Sets the default schema to a given schema. */
    public AssertThat withSchema(String name, Schema schema) {
      return with(ConnectionFactories.add(name, schema));
    }

    /** Sets the default schema of the connection. Schema name may be null. */
    public AssertThat withDefaultSchema(String schema) {
      return with(ConnectionFactories.setDefault(schema));
    }

    public AssertThat with(ConnectionPostProcessor postProcessor) {
      return with(connectionFactory.with(postProcessor));
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
          final String sql2 = sql.replace("`", "\"");
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
            + ",\n{ name: 'mat', "
            + buf
            + "}\n"
            + "]"
            + model.substring(endIndex + 1);
      } else if (model.contains("type: ")) {
        model2 =
            model.replaceFirst("type: ",
                java.util.regex.Matcher.quoteReplacement(buf + ",\n"
                    + "type: "));
      } else {
        throw new AssertionError("do not know where to splice");
      }
      return withModel(model2);
    }

    public AssertQuery query(String sql) {
      return new AssertQuery(connectionFactory, sql, hooks, -1, false, null);
    }

    /** Adds a factory to create a {@link RelNode} query. This {@code RelNode}
     * will be used instead of the SQL string.
     *
     * <p>Note: if you want to assert the optimized plan, consider using
     * {@code explainHook...} methods such as
     * {@link AssertQuery#explainHookMatches(String)}
     *
     * @param relFn a custom factory that creates a RelNode instead of regular sql to rel
     * @return updated AssertQuery
     * @see AssertQuery#explainHookContains(String)
     * @see AssertQuery#explainHookMatches(String)
     */
    @SuppressWarnings("DanglingJavadoc")
    public AssertQuery withRel(final Function<RelBuilder, RelNode> relFn) {
      /** Method-local handler for the hook. */
      class Handler {
        void accept(Pair<FrameworkConfig, Holder<CalcitePrepare.Query>> pair) {
          FrameworkConfig frameworkConfig = pair.left;
          Holder<CalcitePrepare.Query> queryHolder = pair.right;
          final FrameworkConfig config =
              Frameworks.newConfigBuilder(frameworkConfig)
                  .context(
                      Contexts.of(CalciteConnectionConfig.DEFAULT
                          .set(CalciteConnectionProperty.FORCE_DECORRELATE,
                              Boolean.toString(false))))
                  .build();
          final RelBuilder b = RelBuilder.create(config);
          queryHolder.set(CalcitePrepare.Query.of(relFn.apply(b)));
        }
      }

      return withHook(Hook.STRING_TO_QUERY, new Handler()::accept)
          .query("?");
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
      return with(ConnectionFactories.pool(connectionFactory));
    }

    public AssertMetaData metaData(Function<Connection, ResultSet> function) {
      return new AssertMetaData(connectionFactory, function);
    }
  }

  /** Connection post-processor. */
  @FunctionalInterface
  public interface ConnectionPostProcessor {
    Connection apply(Connection connection) throws SQLException;
  }

  /** Fluent interface for building a query to be tested. */
  public static class AssertQuery {
    private final String sql;
    private final ConnectionFactory connectionFactory;
    private final int limit;
    private final boolean materializationsEnabled;
    private final ImmutableList<Pair<Hook, Consumer>> hooks;
    private final @Nullable PreparedStatementConsumer consumer;

    private @Nullable String plan;

    private AssertQuery(ConnectionFactory connectionFactory, String sql,
        ImmutableList<Pair<Hook, Consumer>> hooks, int limit,
        boolean materializationsEnabled,
        @Nullable PreparedStatementConsumer consumer) {
      this.sql = requireNonNull(sql, "sql");
      this.connectionFactory =
          requireNonNull(connectionFactory, "connectionFactory");
      this.hooks = requireNonNull(hooks, "hooks");
      this.limit = limit;
      this.materializationsEnabled = materializationsEnabled;
      this.consumer = consumer;
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
     * afterward. */
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
                  return super.adjustValue(s);
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
    public AssertQuery failsAtValidation(@Nullable String optionalMessage) {
      return withConnection(connection ->
        assertQuery(connection, sql, limit, materializationsEnabled,
            hooks, null, null, checkValidationException(optionalMessage)));
    }

    /** Utility method so that one doesn't have to call
     * {@link #failsAtValidation} with {@code null}. */
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

    public AssertQuery consumesPreparedStatement(
        PreparedStatementConsumer consumer) {
      if (consumer == this.consumer) {
        return this;
      }
      return new AssertQuery(connectionFactory, sql, hooks, limit,
          materializationsEnabled, consumer);
    }

    public AssertQuery convertMatches(final Consumer<RelNode> checker) {
      return withConnection(connection ->
        assertPrepare(connection, sql, this.materializationsEnabled, hooks,
            checker, null));
    }

    public AssertQuery substitutionMatches(
        final Consumer<RelNode> checker) {
      return withConnection(connection ->
        assertPrepare(connection, sql, materializationsEnabled, hooks, null,
            checker));
    }

    public AssertQuery explainContains(String expected) {
      return explainMatches("", checkResultContains(expected));
    }

    /**
     * This enables to assert the optimized plan without issuing a separate {@code explain ...}
     * command. This is especially useful when {@code RelNode} is provided via
     * {@link Hook#STRING_TO_QUERY} or {@link AssertThat#withRel(Function)}.
     *
     * <p>Note: this API does NOT trigger the query, so you need to use something like
     * {@link #returns(String)}, or {@link #returnsUnordered(String...)} to trigger query
     * execution
     *
     * <p>Note: prefer using {@link #explainHookMatches(String)} if you assert
     * the full plan tree as it produces slightly cleaner messages
     *
     * @param expectedPlan expected execution plan. The plan is normalized to LF line endings
     * @return updated assert query
     */
    @API(since = "1.22", status = API.Status.EXPERIMENTAL)
    public AssertQuery explainHookContains(String expectedPlan) {
      return explainHookContains(SqlExplainLevel.EXPPLAN_ATTRIBUTES, expectedPlan);
    }

    /**
     * This enables to assert the optimized plan without issuing a separate {@code explain ...}
     * command. This is especially useful when {@code RelNode} is provided via
     * {@link Hook#STRING_TO_QUERY} or {@link AssertThat#withRel(Function)}.
     *
     * <p>Note: this API does NOT trigger the query, so you need to use something like
     * {@link #returns(String)}, or {@link #returnsUnordered(String...)} to trigger query
     * execution
     *
     * <p>Note: prefer using {@link #explainHookMatches(SqlExplainLevel, Matcher)} if you assert
     * the full plan tree as it produces slightly cleaner messages
     *
     * @param sqlExplainLevel the level of explain plan
     * @param expectedPlan expected execution plan. The plan is normalized to LF line endings
     * @return updated assert query
     */
    @API(since = "1.22", status = API.Status.EXPERIMENTAL)
    public AssertQuery explainHookContains(SqlExplainLevel sqlExplainLevel, String expectedPlan) {
      return explainHookMatches(sqlExplainLevel, containsString(expectedPlan));
    }

    /**
     * This enables to assert the optimized plan without issuing a separate {@code explain ...}
     * command. This is especially useful when {@code RelNode} is provided via
     * {@link Hook#STRING_TO_QUERY} or {@link AssertThat#withRel(Function)}.
     *
     * <p>Note: this API does NOT trigger the query, so you need to use something like
     * {@link #returns(String)}, or {@link #returnsUnordered(String...)} to trigger query
     * execution.
     *
     * @param expectedPlan expected execution plan. The plan is normalized to LF line endings
     * @return updated assert query
     */
    @API(since = "1.22", status = API.Status.EXPERIMENTAL)
    public AssertQuery explainHookMatches(String expectedPlan) {
      return explainHookMatches(SqlExplainLevel.EXPPLAN_ATTRIBUTES, is(expectedPlan));
    }

    /**
     * This enables to assert the optimized plan without issuing a separate {@code explain ...}
     * command. This is especially useful when {@code RelNode} is provided via
     * {@link Hook#STRING_TO_QUERY} or {@link AssertThat#withRel(Function)}.
     *
     * <p>Note: this API does NOT trigger the query, so you need to use something like
     * {@link #returns(String)}, or {@link #returnsUnordered(String...)} to trigger query
     * execution.
     *
     * @param planMatcher execution plan matcher. The plan is normalized to LF line endings
     * @return updated assert query
     */
    @API(since = "1.22", status = API.Status.EXPERIMENTAL)
    public AssertQuery explainHookMatches(Matcher<String> planMatcher) {
      return explainHookMatches(SqlExplainLevel.EXPPLAN_ATTRIBUTES, planMatcher);
    }

    /**
     * This enables to assert the optimized plan without issuing a separate {@code explain ...}
     * command. This is especially useful when {@code RelNode} is provided via
     * {@link Hook#STRING_TO_QUERY} or {@link AssertThat#withRel(Function)}.
     *
     * <p>Note: this API does NOT trigger the query, so you need to use something like
     * {@link #returns(String)}, or {@link #returnsUnordered(String...)} to trigger query
     * execution.
     *
     * @param sqlExplainLevel the level of explain plan
     * @param planMatcher execution plan matcher. The plan is normalized to LF line endings
     * @return updated assert query
     */
    @API(since = "1.22", status = API.Status.EXPERIMENTAL)
    public AssertQuery explainHookMatches(SqlExplainLevel sqlExplainLevel,
        Matcher<String> planMatcher) {
      return withHook(Hook.PLAN_BEFORE_IMPLEMENTATION,
          (RelRoot root) ->
              assertThat(
                  "Execution plan for sql " + sql,
                  RelOptUtil.toString(root.rel, sqlExplainLevel),
                  compose(planMatcher, Util::toLinux)));
    }

    public final AssertQuery explainMatches(String extra,
        Consumer<ResultSet> checker) {
      return returns("explain plan " + requireNonNull(extra, "extra")
          + "for " + sql, checker);
    }

    public AssertQuery planContains(String expected) {
      return planContains(null, JavaSql.fromJava(expected));
    }

    public AssertQuery planUpdateHasSql(String expected, int count) {
      return planContains(checkUpdateCount(count), JavaSql.fromSql(expected));
    }

    private AssertQuery planContains(@Nullable Consumer<Integer> checkUpdate,
        JavaSql expected) {
      ensurePlan(checkUpdate);
      requireNonNull(plan, "plan");
      if (expected.sql != null) {
        final List<String> planSqls = JavaSql.fromJava(plan).extractSql();
        final String planSql;
        if (planSqls.size() == 1) {
          planSql = planSqls.get(0);
          assertThat("Execution plan for sql " + sql, planSql, is(expected.sql));
        } else {
          assertThat("Execution plan for sql " + sql, planSqls, hasItem(expected.sql));
        }
      } else {
        assertThat("Execution plan for sql " + sql, plan, containsStringLinux(expected.java));
      }
      return this;
    }

    public AssertQuery planHasSql(String expected) {
      return planContains(null, JavaSql.fromSql(expected));
    }

    private void ensurePlan(@Nullable Consumer<Integer> checkUpdate) {
      if (plan != null) {
        return;
      }
      final List<Pair<Hook, Consumer>> newHooks =
          addPair(hooks, Hook.JAVA_PLAN, (Consumer<String>) this::setPlan);
      withConnection(connection -> {
        assertQuery(connection, sql, limit, materializationsEnabled,
            newHooks, null, checkUpdate, null);
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
      final List<Pair<Hook, Consumer>> newHooks =
          addPair(hooks, Hook.QUERY_PLAN, list::add);
      return withConnection(connection -> {
        assertQuery(connection, sql, limit, materializationsEnabled,
            newHooks, null, null, null);
        predicate1.accept(list);
      });
    }

    // CHECKSTYLE: IGNORE 1
    /** @deprecated Use {@link #queryContains(Consumer)}. */
    @SuppressWarnings("Guava")
    @Deprecated // to be removed before 2.0
    public final AssertQuery queryContains(
        com.google.common.base.Function<List, Void> predicate1) {
      return queryContains(functionConsumer(predicate1));
    }

    /** Converts a Guava function into a JDK consumer. */
    @SuppressWarnings("Guava")
    private static <T, R> Consumer<T> functionConsumer(
        com.google.common.base.Function<T, R> handler) {
      return t -> {
        // Squash ErrorProne warnings that the return of the function is not
        // used.
        R r = handler.apply(t);
        Util.discard(r);
      };
    }

    /** Sets a limit on the number of rows returned. -1 means no limit. */
    public AssertQuery limit(int limit) {
      if (limit == this.limit) {
        return this;
      }
      return new AssertQuery(connectionFactory, sql, hooks, limit,
          materializationsEnabled, consumer);
    }

    public void sameResultWithMaterializationsDisabled() {
      final boolean ordered =
          sql.toUpperCase(Locale.ROOT).contains("ORDER BY");
      final Consumer<ResultSet> checker = consistentResult(ordered);
      enableMaterializations(false).returns(checker);
      returns(checker);
    }

    public AssertQuery enableMaterializations(boolean materializationsEnabled) {
      if (materializationsEnabled == this.materializationsEnabled) {
        return this;
      }
      return new AssertQuery(connectionFactory, sql, hooks, limit,
          materializationsEnabled, consumer);
    }

    /** Adds a hook and a handler for that hook. Calcite will create a thread
     * hook (by calling {@link Hook#addThread(Consumer)})
     * just before running the query, and remove the hook afterwards. */
    public <T> AssertQuery withHook(Hook hook, Consumer<T> handler) {
      final ImmutableList<Pair<Hook, Consumer>> hooks =
          addPair(this.hooks, hook, handler);
      return new AssertQuery(connectionFactory, sql, hooks, limit,
          materializationsEnabled, consumer);
    }

    /** Adds a property hook. */
    public <V> AssertQuery withProperty(Hook hook, V value) {
      return withHook(hook, Hook.propertyJ(value));
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
     * {@link HrSchema} and
     * {@link FoodmartSchema}.
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
      super(new ConnectionFactory() {
        @Override public Connection createConnection() {
          throw new UnsupportedOperationException();
        }
      }, sql, ImmutableList.of(), 0, false, null);
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
        Consumer<RelNode> checker) {
      return this;
    }

    @Override public AssertQuery substitutionMatches(
        Consumer<RelNode> checker) {
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
        new ConnectionSpec(FoodmartHsqldb.URI,
            FoodmartHsqldb.USER,
            FoodmartHsqldb.PASSWORD,
            "org.hsqldb.jdbcDriver",
            "foodmart"),
        new ConnectionSpec(ScottHsqldb.URI,
            ScottHsqldb.USER,
            ScottHsqldb.PASSWORD,
            "org.hsqldb.jdbcDriver",
            "SCOTT"),
        new ConnectionSpec(SteelwheelsHsqldb.URI,
            SteelwheelsHsqldb.USER,
            SteelwheelsHsqldb.PASSWORD,
            "org.hsqldb.jdbcDriver",
            "steelwheels")),
    H2(
        new ConnectionSpec("jdbc:h2:" + CalciteSystemProperty.TEST_DATASET_PATH.value()
            + "/h2/target/foodmart;user=foodmart;password=foodmart",
            "foodmart", "foodmart", "org.h2.Driver", "foodmart"), null, null),
    MYSQL(
        new ConnectionSpec(testMysqlUrl, "foodmart",
            "foodmart", testMysqlDriver, "foodmart"), null, null),
    STARROCKS(
        new ConnectionSpec(testMysqlUrl, "foodmart",
            "foodmart", testMysqlDriver, "foodmart"), null, null),
    DORIS(
        new ConnectionSpec(testMysqlUrl, "foodmart",
            "foodmart", testMysqlDriver, "foodmart"), null, null),
    ORACLE(
        new ConnectionSpec("jdbc:oracle:thin:@localhost:1521:XE", "foodmart",
            "foodmart", "oracle.jdbc.OracleDriver", "FOODMART"), null, null),
    POSTGRESQL(
        new ConnectionSpec(
            "jdbc:postgresql://localhost/foodmart?user=foodmart&password=foodmart&searchpath=foodmart",
            "foodmart", "foodmart", "org.postgresql.Driver", "foodmart"), null, null);

    public final ConnectionSpec foodmart;
    public final @Nullable ConnectionSpec scott;
    public final @Nullable ConnectionSpec steelwheels;

    DatabaseInstance(ConnectionSpec foodmart, @Nullable ConnectionSpec scott,
        @Nullable ConnectionSpec steelwheels) {
      this.foodmart = foodmart;
      this.scott = scott;
      this.steelwheels = steelwheels;
    }
  }

  /** Specification for common test schemas. */
  public enum SchemaSpec {
    REFLECTIVE_FOODMART("foodmart"),
    FAKE_FOODMART("foodmart"),
    JDBC_FOODMART("foodmart"),
    CLONE_FOODMART("foodmart2"),
    JDBC_FOODMART_WITH_LATTICE("lattice"),
    GEO("GEO"),
    HR("hr"),
    MY_DB("myDb"),
    UNSIGNED_TYPE("UNSIGNED_TYPE"),
    JDBC_SCOTT("JDBC_SCOTT"),
    SCOTT("scott"),
    SCOTT_WITH_TEMPORAL("scott_temporal"),
    JDBC_STEELWHEELS("JDBC_STEELWHEELS"),
    STEELWHEELS("steelwheels"),
    TPCH("tpch"),
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
      if (string != null) {
        string = TestUtil.correctRoundedFloat(string);
      }
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

  /** We want a consumer that can throw SqlException. */
  public interface PreparedStatementConsumer {
    void accept(PreparedStatement statement) throws SQLException;
  }

  /** An expected string that may contain either Java or a SQL string embedded
   * in the Java. */
  private static class JavaSql {
    private static final String START =
        ".unwrap(javax.sql.DataSource.class), \"";
    private static final String END = "\"";

    private final String java;
    private final @Nullable String sql;

    JavaSql(String java, @Nullable String sql) {
      this.java = requireNonNull(java, "java");
      this.sql = sql;
    }

    static JavaSql fromJava(String java) {
      return new JavaSql(java, null);
    }

    static JavaSql fromSql(String sql) {
      return new JavaSql(wrap(sql), sql);
    }

    private static String wrap(String sql) {
      return START
          + sql.replace("\\", "\\\\")
              .replace("\"", "\\\"")
              .replace("\n", "\\\\n")
          + END;
    }

    /** Extracts the SQL statement(s) from within a Java plan. */
    public List<String> extractSql() {
      return unwrap(java);
    }

    static List<String> unwrap(String java) {
      final List<String> sqlList = new ArrayList<>();
      final StringBuilder b = new StringBuilder();
      hLoop:
      for (int h = 0;;) {
        final int i = java.indexOf(START, h);
        if (i < 0) {
          return sqlList;
        }
        for (int j = i + START.length(); j < java.length();) {
          char c = java.charAt(j++);
          switch (c) {
          case '"':
            sqlList.add(b.toString());
            b.setLength(0);
            h = j;
            continue hLoop;
          case '\\':
            c = java.charAt(j++);
            if (c == 'n') {
              b.append('\n');
              break;
            }
            if (c == 'r') {
              // Ignore CR, thus converting Windows strings to Unix.
              break;
            }
            // fall through for '\\' and '\"'
          default:
            b.append(c);
          }
        }
        return sqlList; // last SQL literal was incomplete
      }
    }
  }

  /** Schema instance for {@link SchemaSpec#MY_DB}. */
  private static final Schema MY_DB_SCHEMA = new Schema() {

    final Table table = new Table() {
      /**
       * {@inheritDoc}
       *
       * <p>Table schema is as follows:
       *
       * <pre>{@code
       * myTable(
       *      a: BIGINT,
       *      n1: STRUCT<
       *            n11: STRUCT<b: BIGINT>,
       *            n12: STRUCT<c: BIGINT>
       *          >,
       *      n2: STRUCT<d: BIGINT>,
       *      xs: ARRAY<BIGINT>,
       *      e: BIGINT)
       * }</pre>
       */
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        return typeFactory.builder()
            .add("a", bigint)
            .add("n1",
                typeFactory.builder()
                    .add("n11", typeFactory.builder().add("b", bigint).build())
                    .add("n12", typeFactory.builder().add("c", bigint).build())
                    .build())
            .add("n2", typeFactory.builder().add("d", bigint).build())
            .add("xs", typeFactory.createArrayType(bigint, -1))
            .add("e", bigint)
            .build();
      }

      @Override public Statistic getStatistic() {
        return new Statistic() {
          @Override public Double getRowCount() {
            return 0D;
          }
        };
      }

      @Override public Schema.TableType getJdbcTableType() {
        return TableType.TABLE;
      }

      @Override public boolean isRolledUp(String column) {
        return false;
      }

      @Override public boolean rolledUpColumnValidInsideAgg(String column,
          SqlCall call, @Nullable SqlNode parent,
          @Nullable CalciteConnectionConfig config) {
        return false;
      }
    };

    @Deprecated @Override public Table getTable(String name) {
      return table;
    }

    @Deprecated @Override public Set<String> getTableNames() {
      return ImmutableSet.of("myTable");
    }

    @Override public @Nullable RelProtoDataType getType(String name) {
      return null;
    }

    @Override public Set<String> getTypeNames() {
      return ImmutableSet.of();
    }

    @Override public Collection<org.apache.calcite.schema.Function>
      getFunctions(String name) {
      return ImmutableList.of();
    }

    @Override public Set<String> getFunctionNames() {
      return ImmutableSet.of();
    }

    @Deprecated @Override public @Nullable Schema getSubSchema(String name) {
      return null;
    }

    @Deprecated @Override public Set<String> getSubSchemaNames() {
      return ImmutableSet.of();
    }

    @Override public Expression getExpression(@Nullable SchemaPlus parentSchema,
        String name) {
      throw new UnsupportedOperationException("getExpression");
    }

    @Override public boolean isMutable() {
      return false;
    }

    @Override public Schema snapshot(SchemaVersion version) {
      throw new UnsupportedOperationException("snapshot");
    }
  };

  /** Schema instance for {@link SchemaSpec#UNSIGNED_TYPE}. */
  private static final Schema UNSIGNED_TYPE_SCHEMA = new AbstractSchema() {

    @Override protected Map<String, Table> getTableMap() {
      return ImmutableMap.of("test_unsigned", new UnsingedScannableTable());
    }
  };

  /** Scannable table for unsigned types test. */
  private static class UnsingedScannableTable extends AbstractTable implements ScannableTable {

    /**
     * {@inheritDoc}
     *
     * <p>Table schema is as follows:
     *
     * <pre>{@code
     * test_unsigned(
     *      utiny_value: TINYINT UNSIGNED,
     *      usmall_value: SMALLINT UNSIGNED,
     *      uint_value: INT UNSIGNED,
     *      ubig_value: BIGINT UNSIGNED)
     * }</pre>
     */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("utiny_value", typeFactory.createSqlType(SqlTypeName.UTINYINT))
          .add("usmall_value", typeFactory.createSqlType(SqlTypeName.USMALLINT))
          .add("uint_value", typeFactory.createSqlType(SqlTypeName.UINTEGER))
          .add("ubig_value", typeFactory.createSqlType(SqlTypeName.UBIGINT))
          .build();
    }

    @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(new Object[][] {
          {255, 65535, 4294967295L, new BigDecimal("18446744073709551615")}
      });
    }
  }
}
