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
package net.hydromatic.optiq.test;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.config.OptiqConnectionProperty;
import net.hydromatic.optiq.impl.AbstractSchema;
import net.hydromatic.optiq.impl.ViewTable;
import net.hydromatic.optiq.impl.clone.CloneSchema;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.impl.jdbc.JdbcSchema;
import net.hydromatic.optiq.jdbc.MetaImpl;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.jdbc.OptiqSchema;
import net.hydromatic.optiq.materialize.Lattice;
import net.hydromatic.optiq.runtime.Hook;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.util.*;

import com.google.common.base.*;
import com.google.common.base.Function;
import com.google.common.cache.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Lists;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.*;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * Fluid DSL for testing Optiq connections and queries.
 */
public class OptiqAssert {
  private OptiqAssert() {}

  /** Which database to use for tests that require a JDBC data source. By
   * default the test suite runs against the embedded hsqldb database.
   *
   * <p>We recommend that casual users use hsqldb, and frequent Optiq developers
   * use MySQL. The test suite runs faster against the MySQL database (mainly
   * because of the 0.1s versus 6s startup time). You have to populate MySQL
   * manually with the foodmart data set, otherwise there will be test failures.
   * To run against MySQL, specify '-Doptiq.test.db=mysql' on the java command
   * line.</p> */
  public static final ConnectionSpec CONNECTION_SPEC =
      Util.first(System.getProperty("optiq.test.db"), "hsqldb").equals("mysql")
          ? ConnectionSpec.MYSQL
          : ConnectionSpec.HSQLDB;

  /** Whether to enable slow tests. Default is false. */
  public static final boolean ENABLE_SLOW =
      Util.first(Boolean.getBoolean("optiq.test.slow"), false);

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
        public AssertThat with(Map<String, String> map) {
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
          return NopAssertQuery.of(sql);
        }

        @Override public AssertThat connectThrows(
            Function<Throwable, Void> exceptionChecker) {
          return this;
        }

        @Override
        public <T> AssertThat doWithConnection(Function<OptiqConnection, T> fn)
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

        @Override
        public AssertThat pooled() {
          return this;
        }
      };

  /** Creates an instance of {@code OptiqAssert} with the regular
   * configuration. */
  public static AssertThat that() {
    return new AssertThat(Config.REGULAR);
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
          final String resultString = OptiqAssert.toString(resultSet);
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
          final int count = OptiqAssert.countRows(resultSet);
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
              OptiqAssert.toStringList(resultSet,
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
          OptiqAssert.toStringList(resultSet, actualList);
          Collections.sort(actualList);

          // Use assertArrayEquals since it implements fine-grained comparison.
          assertArrayEquals(expectedList.toArray(), actualList.toArray());
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
          final String actual = Util.toLinux(OptiqAssert.toString(s));
          if (!actual.contains(expected)) {
            assertEquals("contains", expected, actual);
          }
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
          final String actual = Util.toLinux(OptiqAssert.toString(s));
          final String maskedActual =
              actual.replaceAll(", id = [0-9]+", "");
          if (!maskedActual.contains(expected)) {
            assertEquals("contains", expected, maskedActual);
          }
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
      ((OptiqConnection) connection).getProperties().setProperty(
          OptiqConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
          Boolean.toString(materializationsEnabled));
      ((OptiqConnection) connection).getProperties().setProperty(
          OptiqConnectionProperty.CREATE_MATERIALIZATIONS.camelName(),
          Boolean.toString(materializationsEnabled));
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
      ((OptiqConnection) connection).getProperties().setProperty(
          OptiqConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
          Boolean.toString(materializationsEnabled));
      ((OptiqConnection) connection).getProperties().setProperty(
          OptiqConnectionProperty.CREATE_MATERIALIZATIONS.camelName(),
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
      buf.append("\n");
    }
    return buf.toString();
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
      int n = resultSet.getMetaData().getColumnCount();
      if (n > 0) {
        for (int i = 1;; i++) {
          buf.append(resultSet.getMetaData().getColumnLabel(i))
              .append("=")
              .append(resultSet.getString(i));
          if (i == n) {
            break;
          }
          buf.append("; ");
        }
      }
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
    switch (schema) {
    case REFLECTIVE_FOODMART:
      return rootSchema.add("foodmart",
          new ReflectiveSchema(new JdbcTest.FoodmartSchema()));
    case JDBC_FOODMART:
      final DataSource dataSource =
          JdbcSchema.dataSource(
              CONNECTION_SPEC.url,
              CONNECTION_SPEC.driver,
              CONNECTION_SPEC.username,
              CONNECTION_SPEC.password);
      return rootSchema.add("foodmart",
          JdbcSchema.create(rootSchema, "foodmart", dataSource, null,
              "foodmart"));
    case JDBC_FOODMART_WITH_LATTICE:
      foodmart = rootSchema.getSubSchema("foodmart");
      if (foodmart == null) {
        foodmart = OptiqAssert.addSchema(rootSchema, SchemaSpec.JDBC_FOODMART);
      }
      foodmart.add("lattice",
          Lattice.create(foodmart.unwrap(OptiqSchema.class),
              "select 1 from \"foodmart\".\"sales_fact_1997\" as s\n"
              + "join \"foodmart\".\"time_by_day\" as t using (\"time_id\")\n"
              + "join \"foodmart\".\"customer\" as c using (\"customer_id\")\n"
              + "join \"foodmart\".\"product\" as p using (\"product_id\")\n"
              + "join \"foodmart\".\"product_class\" as pc on p.\"product_class_id\" = pc.\"product_class_id\"",
              true));
      return foodmart;
    case CLONE_FOODMART:
      foodmart = rootSchema.getSubSchema("foodmart");
      if (foodmart == null) {
        foodmart = OptiqAssert.addSchema(rootSchema, SchemaSpec.JDBC_FOODMART);
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
              ImmutableList.<String>of()));
      post.add("DEPT",
          ViewTable.viewMacro(post,
              "select * from (values\n"
              + "    (10, 'Sales'),\n"
              + "    (20, 'Marketing'),\n"
              + "    (30, 'Engineering'),\n"
              + "    (40, 'Empty')) as t(deptno, dname)",
              ImmutableList.<String>of()));
      post.add("EMPS",
          ViewTable.viewMacro(post,
              "select * from (values\n"
              + "    (100, 'Fred',  10, CAST(NULL AS CHAR(1)), CAST(NULL AS VARCHAR(20)), 40,               25, TRUE,    FALSE, DATE '1996-08-03'),\n"
              + "    (110, 'Eric',  20, 'M',                   'San Francisco',           3,                80, UNKNOWN, FALSE, DATE '2001-01-01'),\n"
              + "    (110, 'John',  40, 'M',                   'Vancouver',               2, CAST(NULL AS INT), FALSE,   TRUE,  DATE '2002-05-03'),\n"
              + "    (120, 'Wilma', 20, 'F',                   CAST(NULL AS VARCHAR(20)), 1,                 5, UNKNOWN, TRUE,  DATE '2005-09-07'),\n"
              + "    (130, 'Alice', 40, 'F',                   'Vancouver',               2, CAST(NULL AS INT), FALSE,   TRUE,  DATE '2007-01-01'))\n"
              + " as t(empno, name, deptno, gender, city, empid, age, slacker, manager, joinedat)",
              ImmutableList.<String>of()));
      return post;
    default:
      throw new AssertionError("unknown schema " + schema);
    }
  }

  static OptiqConnection getConnection(String... schema)
      throws ClassNotFoundException, SQLException {
    final List<String> schemaList = Arrays.asList(schema);
    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    String suffix = schemaList.contains("spark") ? "spark=true" : "";
    Connection connection =
        DriverManager.getConnection("jdbc:optiq:" + suffix);
    OptiqConnection optiqConnection =
        connection.unwrap(OptiqConnection.class);
    SchemaPlus rootSchema = optiqConnection.getRootSchema();
    if (schemaList.contains("hr")) {
      addSchema(rootSchema, SchemaSpec.HR);
    }
    if (schemaList.contains("foodmart")) {
      addSchema(rootSchema, SchemaSpec.REFLECTIVE_FOODMART);
    }
    if (schemaList.contains("lingual")) {
      addSchema(rootSchema, SchemaSpec.LINGUAL);
    }
    if (schemaList.contains("post")) {
      addSchema(rootSchema, SchemaSpec.POST);
    }
    if (schemaList.contains("metadata")) {
      // always present
      Util.discard(0);
    }
    return optiqConnection;
  }

  /**
   * Creates a connection with a given query provider. If provider is null,
   * uses the connection as its own provider. The connection contains a
   * schema called "foodmart" backed by a JDBC connection to MySQL.
   *
   * @param schemaSpec Schema specification; whether to create a "foodmart2"
   *     schema as in-memory clone
   * @return Connection
   * @throws ClassNotFoundException
   * @throws java.sql.SQLException
   */
  static OptiqConnection getConnection(SchemaSpec schemaSpec)
      throws ClassNotFoundException, SQLException {
    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    Connection connection = DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection =
        connection.unwrap(OptiqConnection.class);
    final SchemaPlus rootSchema = optiqConnection.getRootSchema();
    switch (schemaSpec) {
    case JDBC_FOODMART:
      addSchema(rootSchema, schemaSpec);
      break;
    case CLONE_FOODMART:
    case JDBC_FOODMART_WITH_LATTICE:
      addSchema(rootSchema, SchemaSpec.JDBC_FOODMART);
      addSchema(rootSchema, schemaSpec);
      break;
    default:
      throw new AssertionError("unknown schema " + schemaSpec);
    }
    optiqConnection.setSchema("foodmart2");
    return optiqConnection;
  }

  static <F, T> Function<F, T> constantNull() {
    //noinspection unchecked
    return (Function<F, T>) (Function) Functions.<T>constant(null);
  }

  /**
   * Result of calling {@link OptiqAssert#that}.
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

    public AssertThat with(final Map<String, String> map) {
      return new AssertThat(
          new ConnectionFactory() {
            public OptiqConnection createConnection() throws Exception {
              Class.forName("net.hydromatic.optiq.jdbc.Driver");
              final Properties info = new Properties();
              for (Map.Entry<String, String> entry : map.entrySet()) {
                info.setProperty(entry.getKey(), entry.getValue());
              }
              return (OptiqConnection) DriverManager.getConnection(
                  "jdbc:optiq:", info);
            }
          });
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
              SchemaPlus rootSchema =
                  optiqConnection.getRootSchema();
              rootSchema.add(name, new ReflectiveSchema(schema));
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

    /** Adds materializations to the schema. */
    public AssertThat withMaterializations(
        String model, String... materializations) {
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

    /** Creates a {@link OptiqConnection} and executes a callback. */
    public <T> AssertThat doWithConnection(Function<OptiqConnection, T> fn)
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

    /** Creates a {@link DataContext} and executes a callback. */
    public <T> AssertThat doWithDataContext(Function<DataContext, T> fn)
        throws Exception {
      OptiqConnection connection = connectionFactory.createConnection();
      final DataContext dataContext = MetaImpl.createDataContext(connection);
      try {
        T t = fn.apply(dataContext);
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

    /** Use sparingly. Does not close the connection. */
    public Connection connect() throws Exception {
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
  }

  public interface ConnectionFactory {
    OptiqConnection createConnection() throws Exception;
  }

  private static class PoolingConnectionFactory implements ConnectionFactory {
    private final ConnectionFactory factory;

    public PoolingConnectionFactory(final ConnectionFactory factory) {
      this.factory = factory;
    }

    public OptiqConnection createConnection() throws Exception {
      return Pool.INSTANCE.cache.get(factory);
    }
  }

  private static class Pool {
    private static final Pool INSTANCE = new Pool();

    private final LoadingCache<ConnectionFactory, OptiqConnection> cache =
        CacheBuilder.newBuilder().build(
            new CacheLoader<ConnectionFactory, OptiqConnection>() {
              public OptiqConnection load(ConnectionFactory key)
                  throws Exception {
                return key.createConnection();
              }
            });
  }

  private static class ConfigConnectionFactory implements ConnectionFactory {
    private final Config config;

    public ConfigConnectionFactory(Config config) {
      this.config = config;
    }

    @Override public int hashCode() {
      return config.hashCode();
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof ConfigConnectionFactory
          && config == ((ConfigConnectionFactory) obj).config;
    }

    public OptiqConnection createConnection() throws Exception {
      switch (config) {
      case REGULAR:
        return getConnection("hr", "foodmart", "post");
      case REGULAR_PLUS_METADATA:
        return getConnection("hr", "foodmart", "metadata");
      case LINGUAL:
        return getConnection("lingual");
      case JDBC_FOODMART:
        return getConnection(OptiqAssert.SchemaSpec.JDBC_FOODMART);
      case FOODMART_CLONE:
        return getConnection(SchemaSpec.CLONE_FOODMART);
      case JDBC_FOODMART_WITH_LATTICE:
        return getConnection(SchemaSpec.JDBC_FOODMART_WITH_LATTICE);
      case SPARK:
        return getConnection("spark");
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

    /** Adds a hook and a handler for that hook. Optiq will create a thread
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

  public enum Config {
    /**
     * Configuration that creates a connection with two in-memory data sets:
     * {@link net.hydromatic.optiq.test.JdbcTest.HrSchema} and
     * {@link net.hydromatic.optiq.test.JdbcTest.FoodmartSchema}.
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
     * {@link net.hydromatic.linq4j.Enumerable#where(net.hydromatic.linq4j.function.Predicate1)}.
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

    @Override
    protected Connection createConnection() throws Exception {
      throw new AssertionError("disabled");
    }

    @Override
    public AssertQuery returns(String sql, Function<ResultSet, Void> checker) {
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

    @Override public AssertQuery convertMatches(
        Function<RelNode, Void> checker) {
      return this;
    }

    @Override public AssertQuery substitutionMatches(
        Function<RelNode, Void> checker) {
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

    @Override
    public AssertQuery queryContains(Function<List, Void> predicate1) {
      return this;
    }
  }

  /** Information necessary to create a JDBC connection. Specify one to run
   * tests against a different database. (hsqldb is the default.) */
  public enum ConnectionSpec {
    HSQLDB("jdbc:hsqldb:res:foodmart", "FOODMART", "FOODMART",
        "org.hsqldb.jdbcDriver"),
    MYSQL("jdbc:mysql://localhost/foodmart", "foodmart", "foodmart",
        "com.mysql.jdbc.Driver");

    public final String url;
    public final String username;
    public final String password;
    public final String driver;

    ConnectionSpec(String url, String username, String password,
        String driver) {
      this.url = url;
      this.username = username;
      this.password = password;
      this.driver = driver;
    }
  }

  public enum SchemaSpec {
    REFLECTIVE_FOODMART,
    JDBC_FOODMART,
    CLONE_FOODMART,
    JDBC_FOODMART_WITH_LATTICE,
    HR,
    LINGUAL,
    POST
  }
}

// End OptiqAssert.java
