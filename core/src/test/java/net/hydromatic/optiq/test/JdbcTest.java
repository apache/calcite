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

import net.hydromatic.avatica.*;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Types;
import net.hydromatic.linq4j.function.Function1;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.*;
import net.hydromatic.optiq.impl.clone.CloneSchema;
import net.hydromatic.optiq.impl.generate.RangeTable;
import net.hydromatic.optiq.impl.java.AbstractQueryableTable;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.impl.jdbc.JdbcConvention;
import net.hydromatic.optiq.impl.jdbc.JdbcSchema;
import net.hydromatic.optiq.jdbc.*;
import net.hydromatic.optiq.jdbc.Driver;
import net.hydromatic.optiq.prepare.Prepare;
import net.hydromatic.optiq.runtime.Hook;
import net.hydromatic.optiq.runtime.SqlFunctions;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelProtoDataType;
import org.eigenbase.resource.EigenbaseNewResource;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Bug;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

import org.hsqldb.jdbcDriver;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.sql.Statement;
import java.util.*;
import javax.sql.DataSource;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * Tests for using Optiq via JDBC.
 */
public class JdbcTest {
  public static final Method GENERATE_STRINGS_METHOD =
      Types.lookupMethod(JdbcTest.class, "generateStrings", Integer.TYPE);

  public static final Method VIEW_METHOD =
      Types.lookupMethod(JdbcTest.class, "view", String.class);

  public static final Method STRING_UNION_METHOD =
      Types.lookupMethod(
          JdbcTest.class, "stringUnion", Queryable.class, Queryable.class);

  public static final String FOODMART_SCHEMA =
      "     {\n"
      + "       type: 'jdbc',\n"
      + "       name: 'foodmart',\n"
      + "       jdbcDriver: '" + OptiqAssert.CONNECTION_SPEC.driver + "',\n"
      + "       jdbcUser: '" + OptiqAssert.CONNECTION_SPEC.username + "',\n"
      + "       jdbcPassword: '" + OptiqAssert.CONNECTION_SPEC.password + "',\n"
      + "       jdbcUrl: '" + OptiqAssert.CONNECTION_SPEC.url + "',\n"
      + "       jdbcCatalog: null,\n"
      + "       jdbcSchema: 'foodmart'\n"
      + "     }\n";

  public static final String FOODMART_MODEL =
      "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'foodmart',\n"
      + "   schemas: [\n"
      + FOODMART_SCHEMA
      + "   ]\n"
      + "}";

  public static final String HR_SCHEMA =
      "     {\n"
      + "       type: 'custom',\n"
      + "       name: 'hr',\n"
      + "       factory: '"
      + ReflectiveSchema.Factory.class.getName()
      + "',\n"
      + "       operand: {\n"
      + "         class: '" + HrSchema.class.getName() + "'\n"
      + "       }\n"
      + "     }\n";

  public static final String HR_MODEL =
      "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'hr',\n"
      + "   schemas: [\n"
      + HR_SCHEMA
      + "   ]\n"
      + "}";

  public static List<Pair<String, String>> getFoodmartQueries() {
    return FOODMART_QUERIES;
  }

  /**
   * Tests a relation that is accessed via method syntax.
   *
   * <p>The function ({@link #generateStrings(int)} has a return type
   * {@link Table} and the actual returned value implements
   * {@link net.hydromatic.optiq.QueryableTable} but not
   * {@link net.hydromatic.optiq.TranslatableTable}.
   */
  @Ignore
  @Test public void testTableFunction()
    throws SQLException, ClassNotFoundException {
    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    Connection connection =
        DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection =
        connection.unwrap(OptiqConnection.class);
    SchemaPlus rootSchema = optiqConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    final TableMacro tableMacro =
        TableMacroImpl.create(GENERATE_STRINGS_METHOD);
    schema.add("GenerateStrings", tableMacro);
    ResultSet resultSet = connection.createStatement().executeQuery(
        "select *\n"
        + "from table(\"s\".\"GenerateStrings\"(5)) as t(n, c)\n"
        + "where char_length(c) > 3");
    assertTrue(resultSet.next());
  }

  /**
   * Tests a relation that is accessed via method syntax.
   *
   * <p>The function ({@link #view(String)} has a return type
   * {@link Table} and the actual returned value implements
   * {@link net.hydromatic.optiq.TranslatableTable}.
   */
  @Test public void testTableMacro()
    throws SQLException, ClassNotFoundException {
    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    Connection connection =
        DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection =
        connection.unwrap(OptiqConnection.class);
    SchemaPlus rootSchema = optiqConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    final TableMacro tableMacro = TableMacroImpl.create(VIEW_METHOD);
    schema.add("View", tableMacro);
    ResultSet resultSet = connection.createStatement().executeQuery(
        "select *\n"
        + "from table(\"s\".\"View\"('(10), (20)')) as t(n)\n"
        + "where n < 15");
    // The call to "View('(10), (2)')" expands to 'values (1), (3), (10), (20)'.
    assertThat(OptiqAssert.toString(resultSet),
        equalTo("N=1\n"
            + "N=3\n"
            + "N=10\n"));
  }

  /** Tests a JDBC connection that provides a model that contains a table
   *  macro. */
  @Test public void testTableMacroInModel() throws Exception {
    checkTableMacroInModel(TableMacroFunction.class);
    checkTableMacroInModel(StaticTableMacroFunction.class);
  }

  private void checkTableMacroInModel(Class clazz) {
    OptiqAssert.that()
        .withModel(
            "{\n"
            + "  version: '1.0',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       name: 'adhoc',\n"
            + "       functions: [\n"
            + "         {\n"
            + "           name: 'View',\n"
            + "           className: '" + clazz.getName() + "'\n"
            + "         }\n"
            + "       ]\n"
            + "     }\n"
            + "   ]\n"
            + "}").query("select * from table(\"adhoc\".\"View\"('(30)'))")
        .returns(
            "c=1\n"
            + "c=3\n"
            + "c=30\n");
  }

  public static <T> Queryable<T> stringUnion(
      Queryable<T> q0, Queryable<T> q1) {
    return q0.concat(q1);
  }

  /** A function that generates a table that generates a sequence of
   * {@link net.hydromatic.optiq.test.JdbcTest.IntString} values. */
  public static Table generateStrings(final int count) {
    return new AbstractQueryableTable(IntString.class) {
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("n", SqlTypeName.INTEGER)
            .add("s", SqlTypeName.VARCHAR, -1)
            .build();
      }

      public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
          SchemaPlus schema, String tableName) {
        BaseQueryable<IntString> queryable =
            new BaseQueryable<IntString>(null, IntString.class, null) {
              public Enumerator<IntString> enumerator() {
                return new Enumerator<IntString>() {
                  static final String Z = "abcdefghijklm";

                  int i = -1;
                  IntString o;

                  public IntString current() {
                    return o;
                  }

                  public boolean moveNext() {
                    if (i < count - 1) {
                      o = new IntString(i, Z.substring(0, i % Z.length()));
                      ++i;
                      return true;
                    } else {
                      return false;
                    }
                  }

                  public void reset() {
                    i = -1;
                  }

                  public void close() {
                  }
                };
              }
            };
        //noinspection unchecked
        return (Queryable<T>) queryable;
      }
    };
  }

  /** Tests {@link net.hydromatic.avatica.Handler#onConnectionClose}
   * and  {@link net.hydromatic.avatica.Handler#onStatementClose}. */
  @Test public void testOnConnectionClose() throws Exception {
    final int[] closeCount = {0};
    final int[] statementCloseCount = {0};
    HandlerDriver.HANDLERS.set(
        new HandlerImpl() {
          @Override
          public void onConnectionClose(AvaticaConnection connection) {
            ++closeCount[0];
            throw new RuntimeException();
          }
          @Override
          public void onStatementClose(AvaticaStatement statement) {
            ++statementCloseCount[0];
            throw new RuntimeException();
          }
        });
    final HandlerDriver driver =
        new HandlerDriver();
    OptiqConnection connection = (OptiqConnection)
        driver.connect("jdbc:optiq:", new Properties());
    SchemaPlus rootSchema = connection.getRootSchema();
    rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
    connection.setSchema("hr");
    final Statement statement = connection.createStatement();
    final ResultSet resultSet =
        statement.executeQuery("select * from \"emps\"");
    assertEquals(0, closeCount[0]);
    assertEquals(0, statementCloseCount[0]);
    resultSet.close();
    assertEquals(0, closeCount[0]);
    assertEquals(0, statementCloseCount[0]);

    // Close statement. It throws SQLException, but statement is still closed.
    try {
      statement.close();
      fail("expecting error");
    } catch (SQLException e) {
      // ok
    }
    assertEquals(0, closeCount[0]);
    assertEquals(1, statementCloseCount[0]);

    // Close connection. It throws SQLException, but connection is still closed.
    try {
      connection.close();
      fail("expecting error");
    } catch (SQLException e) {
      // ok
    }
    assertEquals(1, closeCount[0]);
    assertEquals(1, statementCloseCount[0]);

    // Close a closed connection. Handler is not called again.
    connection.close();
    assertEquals(1, closeCount[0]);
    assertEquals(1, statementCloseCount[0]);

    HandlerDriver.HANDLERS.remove();
  }

  /** Tests {@link java.sql.Statement}.{@code closeOnCompletion()}. */
  @Test public void testStatementCloseOnCompletion() throws Exception {
    String javaVersion = System.getProperty("java.version");
    if (javaVersion.compareTo("1.7") < 0) {
      // Statement.closeOnCompletion was introduced in JDK 1.7.
      return;
    }
    final Driver driver = new Driver();
    OptiqConnection connection = (OptiqConnection)
        driver.connect("jdbc:optiq:", new Properties());
    SchemaPlus rootSchema = connection.getRootSchema();
    rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
    connection.setSchema("hr");
    final Statement statement = connection.createStatement();
    assertFalse((Boolean) OptiqAssert.call(statement, "isCloseOnCompletion"));
    OptiqAssert.call(statement, "closeOnCompletion");
    assertTrue((Boolean) OptiqAssert.call(statement, "isCloseOnCompletion"));
    final ResultSet resultSet =
        statement.executeQuery("select * from \"emps\"");

    assertFalse(resultSet.isClosed());
    assertFalse(statement.isClosed());
    assertFalse(connection.isClosed());

    // when result set is closed, statement is closed automatically
    resultSet.close();
    assertTrue(resultSet.isClosed());
    assertTrue(statement.isClosed());
    assertFalse(connection.isClosed());

    connection.close();
    assertTrue(resultSet.isClosed());
    assertTrue(statement.isClosed());
    assertTrue(connection.isClosed());
  }

  /**
   * The example in the README.
   */
  @Test public void testReadme() throws ClassNotFoundException, SQLException {
    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    Connection connection = DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection =
        connection.unwrap(OptiqConnection.class);
    final SchemaPlus rootSchema = optiqConnection.getRootSchema();
    rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
    Statement statement = optiqConnection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "select d.\"deptno\", min(e.\"empid\")\n"
            + "from \"hr\".\"emps\" as e\n"
            + "join \"hr\".\"depts\" as d\n"
            + "  on e.\"deptno\" = d.\"deptno\"\n"
            + "group by d.\"deptno\"\n"
            + "having count(*) > 1");
    final String s = OptiqAssert.toString(resultSet);
    assertThat(s, notNullValue());
    resultSet.close();
    statement.close();
    connection.close();
  }

  /** Test for {@link Driver#getPropertyInfo(String, Properties)}. */
  @Test public void testConnectionProperties() throws ClassNotFoundException,
      SQLException {
    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    java.sql.Driver driver = DriverManager.getDriver("jdbc:optiq:");
    final DriverPropertyInfo[] propertyInfo =
        driver.getPropertyInfo("jdbc:optiq:", new Properties());
    final HashSet<String> names = new HashSet<String>();
    for (DriverPropertyInfo info : propertyInfo) {
      names.add(info.name);
    }
    assertTrue(names.contains("SCHEMA"));
    assertTrue(names.contains("TIMEZONE"));
    assertTrue(names.contains("MATERIALIZATIONS_ENABLED"));
  }

  /**
   * Make sure that the properties look sane.
   */
  @Test public void testVersion() throws ClassNotFoundException, SQLException {
    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    Connection connection = DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection =
        connection.unwrap(OptiqConnection.class);
    final DatabaseMetaData metaData = optiqConnection.getMetaData();
    assertEquals("Optiq JDBC Driver", metaData.getDriverName());

    final String driverVersion = metaData.getDriverVersion();
    final int driverMajorVersion = metaData.getDriverMajorVersion();
    final int driverMinorVersion = metaData.getDriverMinorVersion();
    assertEquals(0, driverMajorVersion);
    assertTrue(driverMinorVersion >= 5); // will work for the next few releases

    assertEquals("Optiq", metaData.getDatabaseProductName());
    final String databaseProductVersion =
        metaData.getDatabaseProductVersion();
    final int databaseMajorVersion = metaData.getDatabaseMajorVersion();
    assertEquals(driverMajorVersion, databaseMajorVersion);
    final int databaseMinorVersion = metaData.getDatabaseMinorVersion();
    assertEquals(driverMinorVersion, databaseMinorVersion);

    // Check how version is composed of major and minor version. Note that
    // version is stored in pom.xml; major and minor version are
    // stored in net-hydromatic-optiq-jdbc.properties, but derived from
    // version.major and version.minor in pom.xml.
    if (!driverVersion.endsWith("-SNAPSHOT")) {
      assertTrue(driverVersion.startsWith("0."));
      String[] split = driverVersion.split("\\.");
      assertTrue(split.length >= 2);
      final String majorMinor = driverMajorVersion + "." + driverMinorVersion;
      assertTrue(driverVersion.equals(majorMinor)
          || driverVersion.startsWith(majorMinor + "."));
    }
    if (!databaseProductVersion.endsWith("-SNAPSHOT")) {
      assertTrue(databaseProductVersion.startsWith("0."));
      String[] split = databaseProductVersion.split("\\.");
      assertTrue(split.length >= 2);
      final String majorMinor =
          databaseMajorVersion + "." + databaseMinorVersion;
      assertTrue(databaseProductVersion.equals(majorMinor)
          || databaseProductVersion.startsWith(majorMinor + "."));
    }

    connection.close();
  }

  /** Tests driver's implementation of {@link DatabaseMetaData#getColumns}. */
  @Test public void testMetaDataColumns()
    throws ClassNotFoundException, SQLException {
    Connection connection = OptiqAssert.getConnection("hr", "foodmart");
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getColumns(null, null, null, null);
    assertTrue(resultSet.next()); // there's something
    String name = resultSet.getString(4);
    int type = resultSet.getInt(5);
    String typeName = resultSet.getString(6);
    int columnSize = resultSet.getInt(7);
    int decimalDigits = resultSet.getInt(9);
    int numPrecRadix = resultSet.getInt(10);
    int charOctetLength = resultSet.getInt(16);
    String isNullable = resultSet.getString(18);
    resultSet.close();
    connection.close();
  }

  /** Tests driver's implementation of {@link DatabaseMetaData#getPrimaryKeys}.
   * It is empty but it should still have column definitions. */
  @Test public void testMetaDataPrimaryKeys()
    throws ClassNotFoundException, SQLException {
    Connection connection = OptiqAssert.getConnection("hr", "foodmart");
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getPrimaryKeys(null, null, null);
    assertFalse(resultSet.next()); // catalog never contains primary keys
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertEquals(6, resultSetMetaData.getColumnCount());
    assertEquals("TABLE_CAT", resultSetMetaData.getColumnName(1));
    assertEquals(java.sql.Types.VARCHAR, resultSetMetaData.getColumnType(1));
    assertEquals("PK_NAME", resultSetMetaData.getColumnName(6));
    resultSet.close();
    connection.close();
  }

  /** Unit test for
   * {@link net.hydromatic.optiq.jdbc.MetaImpl#likeToRegex(net.hydromatic.avatica.Meta.Pat)}. */
  @Test public void testLikeToRegex() {
    checkLikeToRegex(true, "%", "abc");
    checkLikeToRegex(true, "abc", "abc");
    checkLikeToRegex(false, "abc", "abcd"); // trailing char fails match
    checkLikeToRegex(false, "abc", "0abc"); // leading char fails match
    checkLikeToRegex(false, "abc", "aBc"); // case-sensitive match
    checkLikeToRegex(true, "a[b]c", "a[b]c"); // nothing special about brackets
    checkLikeToRegex(true, "a$c", "a$c"); // nothing special about dollar
    checkLikeToRegex(false, "a$", "a"); // nothing special about dollar
    checkLikeToRegex(true, "a%c", "ac");
    checkLikeToRegex(true, "a%c", "abbbc");
    checkLikeToRegex(false, "a%c", "acccd");

    // escape using back-slash
    checkLikeToRegex(true, "a\\%c", "a%c");
    checkLikeToRegex(false, "a\\%c", "abc");
    checkLikeToRegex(false, "a\\%c", "a\\%c");

    // multiple wild-cards
    checkLikeToRegex(true, "a%c%d", "abcdaaad");
    checkLikeToRegex(false, "a%c%d", "abcdc");
  }

  private void checkLikeToRegex(boolean b, String pattern, String abc) {
    assertTrue(
        b == MetaImpl.likeToRegex(Meta.Pat.of(pattern)).matcher(abc).matches());
  }

  /** Tests driver's implementation of {@link DatabaseMetaData#getColumns}. */
  @Test public void testResultSetMetaData()
    throws ClassNotFoundException, SQLException {
    Connection connection = OptiqAssert.getConnection("hr", "foodmart");
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "select \"empid\", \"deptno\" as x, 1 as y\n"
            + "from \"hr\".\"emps\"");
    ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(3, metaData.getColumnCount());
    assertEquals("empid", metaData.getColumnLabel(1));
    assertEquals("empid", metaData.getColumnName(1));
    assertEquals("emps", metaData.getTableName(1));
    assertEquals("X", metaData.getColumnLabel(2));
    assertEquals("deptno", metaData.getColumnName(2));
    assertEquals("emps", metaData.getTableName(2));
    assertEquals("Y", metaData.getColumnLabel(3));
    assertEquals("Y", metaData.getColumnName(3));
    assertEquals(null, metaData.getTableName(3));
    resultSet.close();
    connection.close();
  }

  /** Tests some queries that have expedited processing because connection pools
   * like to use them to check whether the connection is alive.
   */
  @Test public void testSimple() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query("SELECT 1")
        .returns("EXPR$0=1\n");
  }

  /** Tests accessing columns by name. */
  @Test public void testGetByName() throws Exception {
    // JDBC 3.0 specification: "Column names supplied to getter methods are case
    // insensitive. If a select list contains the same column more than once,
    // the first instance of the column will be returned."
    OptiqAssert.that()
        .doWithConnection(
            new Function1<OptiqConnection, Object>() {
              public Object apply(OptiqConnection c) {
                try {
                  Statement s = c.createStatement();
                  ResultSet rs =
                      s.executeQuery(
                          "SELECT 1 as \"a\", 2 as \"b\", 3 as \"a\", 4 as \"B\"\n"
                          + "FROM (VALUES (0))");
                  assertTrue(rs.next());
                  assertEquals(1, rs.getInt("a"));
                  assertEquals(1, rs.getInt("A"));
                  assertEquals(2, rs.getInt("b"));
                  assertEquals(2, rs.getInt("B"));
                  assertEquals(1, rs.getInt(1));
                  assertEquals(2, rs.getInt(2));
                  assertEquals(3, rs.getInt(3));
                  assertEquals(4, rs.getInt(4));
                  try {
                    int x = rs.getInt("z");
                    fail("expected error, got " + x);
                  } catch (SQLException e) {
                    // ok
                  }
                  assertEquals(1, rs.findColumn("a"));
                  assertEquals(1, rs.findColumn("A"));
                  assertEquals(2, rs.findColumn("b"));
                  assertEquals(2, rs.findColumn("B"));
                  try {
                    int x = rs.findColumn("z");
                    fail("expected error, got " + x);
                  } catch (SQLException e) {
                    assertThat(e.getMessage(), equalTo("column 'z' not found"));
                  }
                  try {
                    int x = rs.getInt(0);
                    fail("expected error, got " + x);
                  } catch (SQLException e) {
                    assertThat(e.getMessage(),
                        equalTo("invalid column ordinal: 0"));
                  }
                  try {
                    int x = rs.getInt(5);
                    fail("expected error, got " + x);
                  } catch (SQLException e) {
                    assertThat(e.getMessage(),
                        equalTo("invalid column ordinal: 5"));
                  }
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }

  @Test public void testCloneSchema()
    throws ClassNotFoundException, SQLException {
    final OptiqConnection connection = OptiqAssert.getConnection(false);
    final SchemaPlus rootSchema = connection.getRootSchema();
    final SchemaPlus foodmart = rootSchema.getSubSchema("foodmart");
    rootSchema.add("foodmart2", new CloneSchema(foodmart));
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "select count(*) from \"foodmart2\".\"time_by_day\"");
    assertTrue(resultSet.next());
    assertEquals(730, resultSet.getInt(1));
    resultSet.close();
    connection.close();
  }

  @Test public void testCloneGroupBy() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"the_year\", count(*) as c, min(\"the_month\") as m\n"
            + "from \"foodmart2\".\"time_by_day\"\n"
            + "group by \"the_year\"\n"
            + "order by 1, 2")
        .returns(
            "the_year=1997; C=365; M=April\n"
            + "the_year=1998; C=365; M=April\n");
  }

  @Ignore
  @Test public void testCloneGroupBy2() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"time_by_day\".\"the_year\" as \"c0\", \"time_by_day\".\"quarter\" as \"c1\", \"product_class\".\"product_family\" as \"c2\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"product_class\" as \"product_class\", \"product\" as \"product\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" group by \"time_by_day\".\"the_year\", \"time_by_day\".\"quarter\", \"product_class\".\"product_family\"")
        .returns(
            "c0=1997; c1=Q2; c2=Drink; m0=5895.0000\n"
            + "c0=1997; c1=Q1; c2=Food; m0=47809.0000\n"
            + "c0=1997; c1=Q3; c2=Drink; m0=6065.0000\n"
            + "c0=1997; c1=Q4; c2=Drink; m0=6661.0000\n"
            + "c0=1997; c1=Q4; c2=Food; m0=51866.0000\n"
            + "c0=1997; c1=Q1; c2=Drink; m0=5976.0000\n"
            + "c0=1997; c1=Q3; c2=Non-Consumable; m0=12343.0000\n"
            + "c0=1997; c1=Q4; c2=Non-Consumable; m0=13497.0000\n"
            + "c0=1997; c1=Q2; c2=Non-Consumable; m0=11890.0000\n"
            + "c0=1997; c1=Q2; c2=Food; m0=44825.0000\n"
            + "c0=1997; c1=Q3; c2=Food; m0=47440.0000\n"
            + "c0=1997; c1=Q1; c2=Non-Consumable; m0=12506.0000\n");
  }

  /** Tests plan for a query with 4 tables, 3 joins. */
  @Ignore
  @Test public void testCloneGroupBy2Plan() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "explain plan for select \"time_by_day\".\"the_year\" as \"c0\", \"time_by_day\".\"quarter\" as \"c1\", \"product_class\".\"product_family\" as \"c2\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"product_class\" as \"product_class\", \"product\" as \"product\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" group by \"time_by_day\".\"the_year\", \"time_by_day\".\"quarter\", \"product_class\".\"product_family\"")
        .returns(
            "PLAN=EnumerableAggregateRel(group=[{0, 1, 2}], m0=[SUM($3)])\n"
            + "  EnumerableCalcRel(expr#0..37=[{inputs}], c0=[$t9], c1=[$t13], c2=[$t4], unit_sales=[$t22])\n"
            + "    EnumerableJoinRel(condition=[=($23, $0)], joinType=[inner])\n"
            + "      EnumerableTableAccessRel(table=[[foodmart2, product_class]])\n"
            + "      EnumerableJoinRel(condition=[=($10, $19)], joinType=[inner])\n"
            + "        EnumerableJoinRel(condition=[=($11, $0)], joinType=[inner])\n"
            + "          EnumerableCalcRel(expr#0..9=[{inputs}], expr#10=[CAST($t4):INTEGER], expr#11=[1997], expr#12=[=($t10, $t11)], proj#0..9=[{exprs}], $condition=[$t12])\n"
            + "            EnumerableTableAccessRel(table=[[foodmart2, time_by_day]])\n"
            + "          EnumerableTableAccessRel(table=[[foodmart2, sales_fact_1997]])\n"
            + "        EnumerableTableAccessRel(table=[[foodmart2, product]])\n"
            + "\n");
  }

  @Test public void testOrderByCase() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"time_by_day\".\"the_year\" as \"c0\" from \"time_by_day\" as \"time_by_day\" group by \"time_by_day\".\"the_year\" order by CASE WHEN \"time_by_day\".\"the_year\" IS NULL THEN 1 ELSE 0 END, \"time_by_day\".\"the_year\" ASC")
        .returns(
            "c0=1997\n"
            + "c0=1998\n");
  }

  private static final String[] QUERIES = {
    "select count(*) from (select 1 as \"c0\" from \"salary\" as \"salary\") as \"init\"",
    "EXPR$0=21252\n",
    "select count(*) from (select 1 as \"c0\" from \"salary\" as \"salary2\") as \"init\"",
    "EXPR$0=21252\n",
    "select count(*) from (select 1 as \"c0\" from \"department\" as \"department\") as \"init\"",
    "EXPR$0=12\n",
    "select count(*) from (select 1 as \"c0\" from \"employee\" as \"employee\") as \"init\"",
    "EXPR$0=1155\n",
    "select count(*) from (select 1 as \"c0\" from \"employee_closure\" as \"employee_closure\") as \"init\"",
    "EXPR$0=7179\n",
    "select count(*) from (select 1 as \"c0\" from \"position\" as \"position\") as \"init\"",
    "EXPR$0=18\n",
    "select count(*) from (select 1 as \"c0\" from \"promotion\" as \"promotion\") as \"init\"",
    "EXPR$0=1864\n",
    "select count(*) from (select 1 as \"c0\" from \"store\" as \"store\") as \"init\"",
    "EXPR$0=25\n",
    "select count(*) from (select 1 as \"c0\" from \"product\" as \"product\") as \"init\"",
    "EXPR$0=1560\n",
    "select count(*) from (select 1 as \"c0\" from \"product_class\" as \"product_class\") as \"init\"",
    "EXPR$0=110\n",
    "select count(*) from (select 1 as \"c0\" from \"time_by_day\" as \"time_by_day\") as \"init\"",
    "EXPR$0=730\n",
    "select count(*) from (select 1 as \"c0\" from \"customer\" as \"customer\") as \"init\"",
    "EXPR$0=10281\n",
    "select count(*) from (select 1 as \"c0\" from \"sales_fact_1997\" as \"sales_fact_1997\") as \"init\"",
    "EXPR$0=86837\n",
    "select count(*) from (select 1 as \"c0\" from \"inventory_fact_1997\" as \"inventory_fact_1997\") as \"init\"",
    "EXPR$0=4070\n",
    "select count(*) from (select 1 as \"c0\" from \"warehouse\" as \"warehouse\") as \"init\"",
    "EXPR$0=24\n",
    "select count(*) from (select 1 as \"c0\" from \"agg_c_special_sales_fact_1997\" as \"agg_c_special_sales_fact_1997\") as \"init\"",
    "EXPR$0=86805\n",
    "select count(*) from (select 1 as \"c0\" from \"agg_pl_01_sales_fact_1997\" as \"agg_pl_01_sales_fact_1997\") as \"init\"",
    "EXPR$0=86829\n",
    "select count(*) from (select 1 as \"c0\" from \"agg_l_05_sales_fact_1997\" as \"agg_l_05_sales_fact_1997\") as \"init\"",
    "EXPR$0=86154\n",
    "select count(*) from (select 1 as \"c0\" from \"agg_g_ms_pcat_sales_fact_1997\" as \"agg_g_ms_pcat_sales_fact_1997\") as \"init\"",
    "EXPR$0=2637\n",
    "select count(*) from (select 1 as \"c0\" from \"agg_c_14_sales_fact_1997\" as \"agg_c_14_sales_fact_1997\") as \"init\"",
    "EXPR$0=86805\n",
    "select \"time_by_day\".\"the_year\" as \"c0\" from \"time_by_day\" as \"time_by_day\" group by \"time_by_day\".\"the_year\" order by \"time_by_day\".\"the_year\" ASC",
    "c0=1997\n"
      + "c0=1998\n",
    "select \"store\".\"store_country\" as \"c0\" from \"store\" as \"store\" where UPPER(\"store\".\"store_country\") = UPPER('USA') group by \"store\".\"store_country\" order by \"store\".\"store_country\" ASC",
    "c0=USA\n",
    "select \"store\".\"store_state\" as \"c0\" from \"store\" as \"store\" where (\"store\".\"store_country\" = 'USA') and UPPER(\"store\".\"store_state\") = UPPER('CA') group by \"store\".\"store_state\" order by \"store\".\"store_state\" ASC",
    "c0=CA\n",
    "select \"store\".\"store_city\" as \"c0\", \"store\".\"store_state\" as \"c1\" from \"store\" as \"store\" where (\"store\".\"store_state\" = 'CA' and \"store\".\"store_country\" = 'USA') and UPPER(\"store\".\"store_city\") = UPPER('Los Angeles') group by \"store\".\"store_city\", \"store\".\"store_state\" order by \"store\".\"store_city\" ASC",
    "c0=Los Angeles; c1=CA\n",
    "select \"customer\".\"country\" as \"c0\" from \"customer\" as \"customer\" where UPPER(\"customer\".\"country\") = UPPER('USA') group by \"customer\".\"country\" order by \"customer\".\"country\" ASC",
    "c0=USA\n",
    "select \"customer\".\"state_province\" as \"c0\", \"customer\".\"country\" as \"c1\" from \"customer\" as \"customer\" where (\"customer\".\"country\" = 'USA') and UPPER(\"customer\".\"state_province\") = UPPER('CA') group by \"customer\".\"state_province\", \"customer\".\"country\" order by \"customer\".\"state_province\" ASC",
    "c0=CA; c1=USA\n",
    "select \"customer\".\"city\" as \"c0\", \"customer\".\"country\" as \"c1\", \"customer\".\"state_province\" as \"c2\" from \"customer\" as \"customer\" where (\"customer\".\"country\" = 'USA' and \"customer\".\"state_province\" = 'CA' and \"customer\".\"country\" = 'USA' and \"customer\".\"state_province\" = 'CA' and \"customer\".\"country\" = 'USA') and UPPER(\"customer\".\"city\") = UPPER('Los Angeles') group by \"customer\".\"city\", \"customer\".\"country\", \"customer\".\"state_province\" order by \"customer\".\"city\" ASC",
    "c0=Los Angeles; c1=USA; c2=CA\n",
    "select \"store\".\"store_country\" as \"c0\" from \"store\" as \"store\" where UPPER(\"store\".\"store_country\") = UPPER('Gender') group by \"store\".\"store_country\" order by \"store\".\"store_country\" ASC",
    "",
    "select \"store\".\"store_type\" as \"c0\" from \"store\" as \"store\" where UPPER(\"store\".\"store_type\") = UPPER('Gender') group by \"store\".\"store_type\" order by \"store\".\"store_type\" ASC",
    "",
    "select \"product_class\".\"product_family\" as \"c0\" from \"product\" as \"product\", \"product_class\" as \"product_class\" where \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" and UPPER(\"product_class\".\"product_family\") = UPPER('Gender') group by \"product_class\".\"product_family\" order by \"product_class\".\"product_family\" ASC",
    "",
    "select \"promotion\".\"media_type\" as \"c0\" from \"promotion\" as \"promotion\" where UPPER(\"promotion\".\"media_type\") = UPPER('Gender') group by \"promotion\".\"media_type\" order by \"promotion\".\"media_type\" ASC",
    "",
    "select \"promotion\".\"promotion_name\" as \"c0\" from \"promotion\" as \"promotion\" where UPPER(\"promotion\".\"promotion_name\") = UPPER('Gender') group by \"promotion\".\"promotion_name\" order by \"promotion\".\"promotion_name\" ASC",
    "",
    "select \"promotion\".\"media_type\" as \"c0\" from \"promotion\" as \"promotion\" where UPPER(\"promotion\".\"media_type\") = UPPER('No Media') group by \"promotion\".\"media_type\" order by \"promotion\".\"media_type\" ASC",
    "c0=No Media\n",
    "select \"promotion\".\"media_type\" as \"c0\" from \"promotion\" as \"promotion\" group by \"promotion\".\"media_type\" order by \"promotion\".\"media_type\" ASC",
    "c0=Bulk Mail\n"
      + "c0=Cash Register Handout\n"
      + "c0=Daily Paper\n"
      + "c0=Daily Paper, Radio\n"
      + "c0=Daily Paper, Radio, TV\n"
      + "c0=In-Store Coupon\n"
      + "c0=No Media\n"
      + "c0=Product Attachment\n"
      + "c0=Radio\n"
      + "c0=Street Handout\n"
      + "c0=Sunday Paper\n"
      + "c0=Sunday Paper, Radio\n"
      + "c0=Sunday Paper, Radio, TV\n"
      + "c0=TV\n",
    "select count(distinct \"the_year\") from \"time_by_day\"",
    "EXPR$0=2\n",
    "select \"time_by_day\".\"the_year\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 group by \"time_by_day\".\"the_year\"",
    "c0=1997; m0=266773.0000\n",
    "select \"time_by_day\".\"the_year\" as \"c0\", \"promotion\".\"media_type\" as \"c1\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"promotion\" as \"promotion\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\" group by \"time_by_day\".\"the_year\", \"promotion\".\"media_type\"",
    "c0=1997; c1=Bulk Mail; m0=4320.0000\n"
      + "c0=1997; c1=Radio; m0=2454.0000\n"
      + "c0=1997; c1=Street Handout; m0=5753.0000\n"
      + "c0=1997; c1=TV; m0=3607.0000\n"
      + "c0=1997; c1=No Media; m0=195448.0000\n"
      + "c0=1997; c1=In-Store Coupon; m0=3798.0000\n"
      + "c0=1997; c1=Sunday Paper, Radio, TV; m0=2726.0000\n"
      + "c0=1997; c1=Product Attachment; m0=7544.0000\n"
      + "c0=1997; c1=Daily Paper; m0=7738.0000\n"
      + "c0=1997; c1=Cash Register Handout; m0=6697.0000\n"
      + "c0=1997; c1=Daily Paper, Radio; m0=6891.0000\n"
      + "c0=1997; c1=Daily Paper, Radio, TV; m0=9513.0000\n"
      + "c0=1997; c1=Sunday Paper, Radio; m0=5945.0000\n"
      + "c0=1997; c1=Sunday Paper; m0=4339.0000\n",
    "select \"store\".\"store_country\" as \"c0\", sum(\"inventory_fact_1997\".\"supply_time\") as \"m0\" from \"store\" as \"store\", \"inventory_fact_1997\" as \"inventory_fact_1997\" where \"inventory_fact_1997\".\"store_id\" = \"store\".\"store_id\" group by \"store\".\"store_country\"",
    "c0=USA; m0=10425\n",
    "select \"sn\".\"desc\" as \"c0\" from (SELECT * FROM (VALUES (1, 'SameName')) AS \"t\" (\"id\", \"desc\")) as \"sn\" group by \"sn\".\"desc\" order by \"sn\".\"desc\" ASC NULLS LAST",
    "c0=SameName\n",
    "select \"the_year\", count(*) as c, min(\"the_month\") as m\n"
      + "from \"foodmart2\".\"time_by_day\"\n"
      + "group by \"the_year\"\n"
      + "order by 1, 2",
    "the_year=1997; C=365; M=April\n"
      + "the_year=1998; C=365; M=April\n",
    "select\n"
      + " \"store\".\"store_state\" as \"c0\",\n"
      + " \"time_by_day\".\"the_year\" as \"c1\",\n"
      + " sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\",\n"
      + " sum(\"sales_fact_1997\".\"store_sales\") as \"m1\"\n"
      + "from \"store\" as \"store\",\n"
      + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
      + " \"time_by_day\" as \"time_by_day\"\n"
      + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
      + "and \"store\".\"store_state\" in ('DF', 'WA')\n"
      + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
      + "and \"time_by_day\".\"the_year\" = 1997\n"
      + "group by \"store\".\"store_state\", \"time_by_day\".\"the_year\"",
    "c0=WA; c1=1997; m0=124366.0000; m1=263793.2200\n",
    "select count(distinct \"product_id\") from \"product\"",
    "EXPR$0=1560\n",
    "select \"store\".\"store_name\" as \"c0\",\n"
      + " \"time_by_day\".\"the_year\" as \"c1\",\n"
      + " sum(\"sales_fact_1997\".\"store_sales\") as \"m0\"\n"
      + "from \"store\" as \"store\",\n"
      + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
      + " \"time_by_day\" as \"time_by_day\"\n"
      + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
      + "and \"store\".\"store_name\" in ('Store 1', 'Store 10', 'Store 11', 'Store 15', 'Store 16', 'Store 24', 'Store 3', 'Store 7')\n"
      + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
      + "and \"time_by_day\".\"the_year\" = 1997\n"
      + "group by \"store\".\"store_name\",\n"
      + " \"time_by_day\".\"the_year\"\n",
    "c0=Store 7; c1=1997; m0=54545.2800\n"
      + "c0=Store 24; c1=1997; m0=54431.1400\n"
      + "c0=Store 16; c1=1997; m0=49634.4600\n"
      + "c0=Store 3; c1=1997; m0=52896.3000\n"
      + "c0=Store 15; c1=1997; m0=52644.0700\n"
      + "c0=Store 11; c1=1997; m0=55058.7900\n",
    "select \"customer\".\"yearly_income\" as \"c0\","
      + " \"customer\".\"education\" as \"c1\" \n"
      + "from \"customer\" as \"customer\",\n"
      + " \"sales_fact_1997\" as \"sales_fact_1997\"\n"
      + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
      + " and ((not (\"customer\".\"yearly_income\" in ('$10K - $30K', '$50K - $70K'))\n"
      + " or (\"customer\".\"yearly_income\" is null)))\n"
      + "group by \"customer\".\"yearly_income\",\n"
      + " \"customer\".\"education\"\n"
      + "order by \"customer\".\"yearly_income\" ASC NULLS LAST,\n"
      + " \"customer\".\"education\" ASC NULLS LAST",
    "c0=$110K - $130K; c1=Bachelors Degree\n"
      + "c0=$110K - $130K; c1=Graduate Degree\n"
      + "c0=$110K - $130K; c1=High School Degree\n"
      + "c0=$110K - $130K; c1=Partial College\n"
      + "c0=$110K - $130K; c1=Partial High School\n"
      + "c0=$130K - $150K; c1=Bachelors Degree\n"
      + "c0=$130K - $150K; c1=Graduate Degree\n"
      + "c0=$130K - $150K; c1=High School Degree\n"
      + "c0=$130K - $150K; c1=Partial College\n"
      + "c0=$130K - $150K; c1=Partial High School\n"
      + "c0=$150K +; c1=Bachelors Degree\n"
      + "c0=$150K +; c1=Graduate Degree\n"
      + "c0=$150K +; c1=High School Degree\n"
      + "c0=$150K +; c1=Partial College\n"
      + "c0=$150K +; c1=Partial High School\n"
      + "c0=$30K - $50K; c1=Bachelors Degree\n"
      + "c0=$30K - $50K; c1=Graduate Degree\n"
      + "c0=$30K - $50K; c1=High School Degree\n"
      + "c0=$30K - $50K; c1=Partial College\n"
      + "c0=$30K - $50K; c1=Partial High School\n"
      + "c0=$70K - $90K; c1=Bachelors Degree\n"
      + "c0=$70K - $90K; c1=Graduate Degree\n"
      + "c0=$70K - $90K; c1=High School Degree\n"
      + "c0=$70K - $90K; c1=Partial College\n"
      + "c0=$70K - $90K; c1=Partial High School\n"
      + "c0=$90K - $110K; c1=Bachelors Degree\n"
      + "c0=$90K - $110K; c1=Graduate Degree\n"
      + "c0=$90K - $110K; c1=High School Degree\n"
      + "c0=$90K - $110K; c1=Partial College\n"
      + "c0=$90K - $110K; c1=Partial High School\n",
    "ignore:select \"time_by_day\".\"the_year\" as \"c0\", \"product_class\".\"product_family\" as \"c1\", \"customer\".\"state_province\" as \"c2\", \"customer\".\"city\" as \"c3\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"product_class\" as \"product_class\", \"product\" as \"product\", \"customer\" as \"customer\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" and \"product_class\".\"product_family\" = 'Drink' and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and \"customer\".\"state_province\" = 'WA' and \"customer\".\"city\" in ('Anacortes', 'Ballard', 'Bellingham', 'Bremerton', 'Burien', 'Edmonds', 'Everett', 'Issaquah', 'Kirkland', 'Lynnwood', 'Marysville', 'Olympia', 'Port Orchard', 'Puyallup', 'Redmond', 'Renton', 'Seattle', 'Sedro Woolley', 'Spokane', 'Tacoma', 'Walla Walla', 'Yakima') group by \"time_by_day\".\"the_year\", \"product_class\".\"product_family\", \"customer\".\"state_province\", \"customer\".\"city\"",
    "c0=1997; c1=Drink; c2=WA; c3=Sedro Woolley; m0=58.0000\n",
    "select \"store\".\"store_country\" as \"c0\",\n"
      + " \"time_by_day\".\"the_year\" as \"c1\",\n"
      + " sum(\"sales_fact_1997\".\"store_cost\") as \"m0\",\n"
      + " count(\"sales_fact_1997\".\"product_id\") as \"m1\",\n"
      + " count(distinct \"sales_fact_1997\".\"customer_id\") as \"m2\",\n"
      + " sum((case when \"sales_fact_1997\".\"promotion_id\" = 0 then 0\n"
      + "     else \"sales_fact_1997\".\"store_sales\" end)) as \"m3\"\n"
      + "from \"store\" as \"store\",\n"
      + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
      + " \"time_by_day\" as \"time_by_day\"\n"
      + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
      + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
      + "and \"time_by_day\".\"the_year\" = 1997\n"
      + "group by \"store\".\"store_country\", \"time_by_day\".\"the_year\"",
    "c0=USA; c1=1997; m0=225627.2336; m1=86837; m2=5581; m3=151211.2100\n",
      // query 6077
      // disabled (runs out of memory)
    "ignore:select \"time_by_day\".\"the_year\" as \"c0\",\n"
      + " count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\"\n"
      + "from \"time_by_day\" as \"time_by_day\",\n"
      + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
      + " \"product_class\" as \"product_class\",\n"
      + " \"product\" as \"product\"\n"
      + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
      + "and \"time_by_day\".\"the_year\" = 1997\n"
      + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
      + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
      + "and (((\"product\".\"brand_name\" = 'Cormorant'\n"
      + "   and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers'\n"
      + "   and \"product_class\".\"product_category\" = 'Kitchen Products'\n"
      + "   and \"product_class\".\"product_department\" = 'Household'\n"
      + "   and \"product_class\".\"product_family\" = 'Non-Consumable')\n"
      + " or (\"product\".\"brand_name\" = 'Denny'\n"
      + "   and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers'\n"
      + "   and \"product_class\".\"product_category\" = 'Kitchen Products'\n"
      + "   and \"product_class\".\"product_department\" = 'Household'\n"
      + "   and \"product_class\".\"product_family\" = 'Non-Consumable')\n"
      + " or (\"product\".\"brand_name\" = 'High Quality'\n"
      + "   and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers'\n"
      + "   and \"product_class\".\"product_category\" = 'Kitchen Products'\n"
      + "   and \"product_class\".\"product_department\" = 'Household'\n"
      + "   and \"product_class\".\"product_family\" = 'Non-Consumable')\n"
      + " or (\"product\".\"brand_name\" = 'Red Wing'\n"
      + "   and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers'\n"
      + "   and \"product_class\".\"product_category\" = 'Kitchen Products'\n"
      + "   and \"product_class\".\"product_department\" = 'Household'\n"
      + "   and \"product_class\".\"product_family\" = 'Non-Consumable'))\n"
      + " or (\"product_class\".\"product_subcategory\" = 'Pots and Pans'\n"
      + "   and \"product_class\".\"product_category\" = 'Kitchen Products'\n"
      + "   and \"product_class\".\"product_department\" = 'Household'\n"
      + "   and \"product_class\".\"product_family\" = 'Non-Consumable'))\n"
      + "group by \"time_by_day\".\"the_year\"\n",
    "xxtodo",
      // query 6077, simplified
      // disabled (slow)
    "ignore:select count(\"sales_fact_1997\".\"customer_id\") as \"m0\"\n"
      + "from \"sales_fact_1997\" as \"sales_fact_1997\",\n"
      + " \"product_class\" as \"product_class\",\n"
      + " \"product\" as \"product\"\n"
      + "where \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
      + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
      + "and ((\"product\".\"brand_name\" = 'Cormorant'\n"
      + "   and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers')\n"
      + " or (\"product_class\".\"product_subcategory\" = 'Pots and Pans'))\n",
    "xxxx",
      // query 6077, simplified further
    "select count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\"\n"
      + "from \"sales_fact_1997\" as \"sales_fact_1997\",\n"
      + " \"product_class\" as \"product_class\",\n"
      + " \"product\" as \"product\"\n"
      + "where \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
      + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
      + "and \"product\".\"brand_name\" = 'Cormorant'\n",
    "m0=1298",
      // query 193
    "select \"store\".\"store_country\" as \"c0\",\n"
      + " \"time_by_day\".\"the_year\" as \"c1\",\n"
      + " \"time_by_day\".\"quarter\" as \"c2\",\n"
      + " \"product_class\".\"product_family\" as \"c3\",\n"
      + " count(\"sales_fact_1997\".\"product_id\") as \"m0\",\n"
      + " count(distinct \"sales_fact_1997\".\"customer_id\") as \"m1\"\n"
      + "from \"store\" as \"store\",\n"
      + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
      + " \"time_by_day\" as \"time_by_day\",\n"
      + " \"product_class\" as \"product_class\",\n"
      + " \"product\" as \"product\"\n"
      + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
      + "and \"store\".\"store_country\" = 'USA'\n"
      + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
      + "and \"time_by_day\".\"the_year\" = 1997\n"
      + "and \"time_by_day\".\"quarter\" = 'Q3'\n"
      + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
      + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
      + "and \"product_class\".\"product_family\" = 'Food'\n"
      + "group by \"store\".\"store_country\",\n"
      + " \"time_by_day\".\"the_year\",\n"
      + " \"time_by_day\".\"quarter\",\n"
      + " \"product_class\".\"product_family\"",
    "c0=USA; c1=1997; c2=Q3; c3=Food; m0=15449; m1=2939",
  };

  public static final List<Pair<String, String>> FOODMART_QUERIES =
      querify(QUERIES);

  /** Janino bug
   * <a href="https://jira.codehaus.org/browse/JANINO-169">JANINO-169</a>
   * running queries against the JDBC adapter. As of janino-2.7.3 bug is
   * open but we have a workaround in EnumerableRelImplementor. */
  @Test public void testJanino169() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.JDBC_FOODMART)
        .query(
            "select \"time_id\" from \"foodmart\".\"time_by_day\" as \"t\"\n")
        .returnsCount(730);
  }

  /** Tests 3-way AND.
   *
   * <p>With <a href="https://github.com/julianhyde/optiq/issues/127">optiq-127,
   * "EnumerableCalcRel can't support 3+ AND conditions"</a>, the last condition
   * is ignored and rows with deptno=10 are wrongly returned.</p>
   */
  @Test public void testAnd3() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select \"deptno\" from \"hr\".\"emps\"\n"
            + "where \"emps\".\"empid\" < 240\n"
                + "and \"salary\" > 7500.0"
                + "and \"emps\".\"deptno\" > 10\n")
        .returnsUnordered("deptno=20");
  }

  /** Tests a date literal against a JDBC data source. */
  @Test public void testJdbcDate() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select count(*) as c from (\n"
            + "  select 1 from \"foodmart\".\"employee\" as e1\n"
            + "  where \"position_title\" = 'VP Country Manager'\n"
            + "  and \"birth_date\" < DATE '1950-01-01'\n"
            + "  and \"gender\" = 'F')")
        .returns("C=1\n");
  }

  /** Tests a timestamp literal against JDBC data source. */
  @Test public void testJdbcTimestamp() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.JDBC_FOODMART)
        .query(
            "select count(*) as c from (\n"
                + "  select 1 from \"foodmart\".\"employee\" as e1\n"
                + "  where \"hire_date\" < TIMESTAMP '1996-06-05 00:00:00'\n"
                + "  and \"gender\" = 'F')")
        .returns("C=287\n");
  }

  /** Unit test for self-join. Left and right children of the join are the same
   * relational expression. */
  @Test public void testSelfJoin() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.JDBC_FOODMART)
        .query(
            "select count(*) as c from (\n"
            + "  select 1 from \"foodmart\".\"employee\" as e1\n"
            + "  join \"foodmart\".\"employee\" as e2 using (\"position_title\"))")
        .returns("C=247149\n");
  }

  /** Self-join on different columns, select a different column, and sort and
   * limit on yet another column. */
  @Test public void testSelfJoinDifferentColumns() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.JDBC_FOODMART)
        .query(
            "select e1.\"full_name\"\n"
            + "  from \"foodmart\".\"employee\" as e1\n"
            + "  join \"foodmart\".\"employee\" as e2 on e1.\"first_name\" = e2.\"last_name\"\n"
            + "order by e1.\"last_name\" limit 3")
        .returns(
            "full_name=James Aguilar\n"
            + "full_name=Carol Amyotte\n"
            + "full_name=Terry Anderson\n");
  }

  /** Test case for
   * <a href="https://github.com/julianhyde/optiq/issues/35">issue #35</a>. */
  @Ignore
  @Test public void testJoinJoin() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select\n"
            + "   \"product_class\".\"product_family\" as \"c0\",\n"
            + "   \"product_class\".\"product_department\" as \"c1\",\n"
            + "   \"customer\".\"country\" as \"c2\",\n"
            + "   \"customer\".\"state_province\" as \"c3\",\n"
            + "   \"customer\".\"city\" as \"c4\"\n"
            + "from\n"
            + "   \"sales_fact_1997\" as \"sales_fact_1997\"\n"
            + "join (\"product\" as \"product\"\n"
            + "     join \"product_class\" as \"product_class\"\n"
            + "     on \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\")\n"
            + "on  \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
            + "join \"customer\" as \"customer\"\n"
            + "on  \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
            + "join \"promotion\" as \"promotion\"\n"
            + "on \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
            + "where (\"promotion\".\"media_type\" = 'Radio'\n"
            + " or \"promotion\".\"media_type\" = 'TV'\n"
            + " or \"promotion\".\"media_type\" = 'Sunday Paper'\n"
            + " or \"promotion\".\"media_type\" = 'Street Handout')\n"
            + " and (\"product_class\".\"product_family\" = 'Drink')\n"
            + " and (\"customer\".\"country\" = 'USA' and \"customer\".\"state_province\""
            + " = 'WA' and \"customer\".\"city\" = 'Bellingham')\n"
            + "group by \"product_class\".\"product_family\",\n"
            + "   \"product_class\".\"product_department\",\n"
            + "   \"customer\".\"country\",\n"
            + "   \"customer\".\"state_province\",\n"
            + "   \"customer\".\"city\"\n"
            + "order by ISNULL(\"product_class\".\"product_family\") ASC,   \"product_class\".\"product_family\" ASC,\n"
            + "   ISNULL(\"product_class\".\"product_department\") ASC,   \"product_class\".\"product_department\" ASC,\n"
            + "   ISNULL(\"customer\".\"country\") ASC,   \"customer\".\"country\" ASC,\n"
            + "   ISNULL(\"customer\".\"state_province\") ASC,   \"customer\".\"state_province\" ASC,\n"
            + "   ISNULL(\"customer\".\"city\") ASC,   \"customer\".\"city\" ASC")
        .returns(
            "+-------+---------------------+-----+------+------------+\n"
            + "| c0    | c1                  | c2  | c3   | c4         |\n"
            + "+-------+---------------------+-----+------+------------+\n"
            + "| Drink | Alcoholic Beverages | USA | WA   | Bellingham |\n"
            + "| Drink | Dairy               | USA | WA   | Bellingham |\n"
            + "+-------+---------------------+-----+------+------------+");
  }

  /** Four-way join. Used to take 80 seconds. */
  @Ignore
  @Test public void testJoinFiveWay() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"store\".\"store_country\" as \"c0\",\n"
            + " \"time_by_day\".\"the_year\" as \"c1\",\n"
            + " \"product_class\".\"product_family\" as \"c2\",\n"
            + " count(\"sales_fact_1997\".\"product_id\") as \"m0\"\n"
            + "from \"store\" as \"store\",\n"
            + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
            + " \"time_by_day\" as \"time_by_day\",\n"
            + " \"product_class\" as \"product_class\",\n"
            + " \"product\" as \"product\"\n"
            + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
            + "and \"store\".\"store_country\" = 'USA'\n"
            + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
            + "and \"time_by_day\".\"the_year\" = 1997\n"
            + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
            + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
            + "group by \"store\".\"store_country\",\n"
            + " \"time_by_day\".\"the_year\",\n"
            + " \"product_class\".\"product_family\"")
        .explainContains(
            "EnumerableAggregateRel(group=[{0, 1, 2}], m0=[COUNT($3)])\n"
            + "  EnumerableCalcRel(expr#0..61=[{inputs}], c0=[$t19], c1=[$t4], c2=[$t46], product_id=[$t34])\n"
            + "    EnumerableJoinRel(condition=[=($35, $0)], joinType=[inner])\n"
            + "      EnumerableCalcRel(expr#0..9=[{inputs}], expr#10=[CAST($t4):INTEGER], expr#11=[1997], expr#12=[=($t10, $t11)], proj#0..9=[{exprs}], $condition=[$t12])\n"
            + "        EnumerableTableAccessRel(table=[[foodmart2, time_by_day]])\n"
            + "      EnumerableCalcRel(expr#0..51=[{inputs}], proj#0..23=[{exprs}], product_id=[$t44], time_id=[$t45], customer_id=[$t46], promotion_id=[$t47], store_id0=[$t48], store_sales=[$t49], store_cost=[$t50], unit_sales=[$t51], product_class_id=[$t24], product_subcategory=[$t25], product_category=[$t26], product_department=[$t27], product_family=[$t28], product_class_id0=[$t29], product_id0=[$t30], brand_name=[$t31], product_name=[$t32], SKU=[$t33], SRP=[$t34], gross_weight=[$t35], net_weight=[$t36], recyclable_package=[$t37], low_fat=[$t38], units_per_case=[$t39], cases_per_pallet=[$t40], shelf_width=[$t41], shelf_height=[$t42], shelf_depth=[$t43])\n"
            + "        EnumerableJoinRel(condition=[=($48, $0)], joinType=[inner])\n"
            + "          EnumerableCalcRel(expr#0..23=[{inputs}], expr#24=['USA'], expr#25=[=($t9, $t24)], proj#0..23=[{exprs}], $condition=[$t25])\n"
            + "            EnumerableTableAccessRel(table=[[foodmart2, store]])\n"
            + "          EnumerableCalcRel(expr#0..27=[{inputs}], proj#0..4=[{exprs}], product_class_id0=[$t13], product_id=[$t14], brand_name=[$t15], product_name=[$t16], SKU=[$t17], SRP=[$t18], gross_weight=[$t19], net_weight=[$t20], recyclable_package=[$t21], low_fat=[$t22], units_per_case=[$t23], cases_per_pallet=[$t24], shelf_width=[$t25], shelf_height=[$t26], shelf_depth=[$t27], product_id0=[$t5], time_id=[$t6], customer_id=[$t7], promotion_id=[$t8], store_id=[$t9], store_sales=[$t10], store_cost=[$t11], unit_sales=[$t12])\n"
            + "            EnumerableJoinRel(condition=[=($13, $0)], joinType=[inner])\n"
            + "              EnumerableTableAccessRel(table=[[foodmart2, product_class]])\n"
            + "              EnumerableJoinRel(condition=[=($0, $9)], joinType=[inner])\n"
            + "                EnumerableTableAccessRel(table=[[foodmart2, sales_fact_1997]])\n"
            + "                EnumerableTableAccessRel(table=[[foodmart2, product]])\n"
            + "\n"
            + "]>")
        .returns(
            "+-------+---------------------+-----+------+------------+\n"
            + "| c0    | c1                  | c2  | c3   | c4         |\n"
            + "+-------+---------------------+-----+------+------------+\n"
            + "| Drink | Alcoholic Beverages | USA | WA   | Bellingham |\n"
            + "| Drink | Dairy               | USA | WA   | Bellingham |\n"
            + "+-------+---------------------+-----+------+------------+");
  }

  /** Returns a list of (query, expected) pairs. The expected result is
   * sometimes null. */
  private static List<Pair<String, String>> querify(String[] queries1) {
    final List<Pair<String, String>> list =
        new ArrayList<Pair<String, String>>();
    for (int i = 0; i < queries1.length; i++) {
      String query = queries1[i];
      String expected = null;
      if (i + 1 < queries1.length
          && queries1[i + 1] != null
          && !queries1[i + 1].startsWith("select")) {
        expected = queries1[++i];
      }
      list.add(Pair.of(query, expected));
    }
    return list;
  }

  /** A selection of queries generated by Mondrian. */
  @Ignore
  @Test public void testCloneQueries() {
    OptiqAssert.AssertThat with =
        OptiqAssert.that()
            .with(OptiqAssert.Config.FOODMART_CLONE);
    for (Ord<Pair<String, String>> query : Ord.zip(FOODMART_QUERIES)) {
      try {
        // uncomment to run specific queries:
//      if (query.i != FOODMART_QUERIES.size() - 1) continue;
        final String sql = query.e.left;
        if (sql.startsWith("ignore:")) {
          continue;
        }
        final String expected = query.e.right;
        final OptiqAssert.AssertQuery query1 = with.query(sql);
        if (expected != null) {
          if (sql.contains("order by")) {
            query1.returns(expected);
          } else {
            query1.returnsUnordered(expected.split("\n"));
          }
        } else {
          query1.runs();
        }
      } catch (Throwable e) {
        throw new RuntimeException("while running query #" + query.i, e);
      }
    }
  }

  /** Tests accessing a column in a JDBC source whose type is ARRAY. */
  @Test public void testArray() throws Exception {
    String hsqldbMemUrl = "jdbc:hsqldb:mem:.";
    Connection baseConnection = DriverManager.getConnection(hsqldbMemUrl);
    Statement baseStmt = baseConnection.createStatement();
    baseStmt.execute("CREATE TABLE ARR_TABLE (\n"
      + "ID INTEGER,\n"
      + "VALS INTEGER ARRAY)");
    baseStmt.execute("INSERT INTO ARR_TABLE VALUES (1, ARRAY[1,2,3])");
    baseStmt.execute("CREATE TABLE ARR_TABLE2 (\n"
        + "ID INTEGER,\n"
        + "VALS INTEGER ARRAY,\n"
        + "VALVALS VARCHAR(10) ARRAY)");
    baseStmt.execute(
        "INSERT INTO ARR_TABLE2 VALUES (1, ARRAY[1,2,3], ARRAY['x','y'])");
    baseStmt.close();
    baseConnection.commit();

    Properties info = new Properties();
    info.put("model",
      "inline:"
        + "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'BASEJDBC',\n"
        + "  schemas: [\n"
        + "     {\n"
        + "       type: 'jdbc',\n"
        + "       name: 'BASEJDBC',\n"
        + "       jdbcDriver: '" + jdbcDriver.class.getName() + "',\n"
        + "       jdbcUrl: '" + hsqldbMemUrl + "',\n"
        + "       jdbcCatalog: null,\n"
        + "       jdbcSchema: null\n"
        + "     }\n"
        + "  ]\n"
        + "}");

    Connection optiqConnection = DriverManager.getConnection(
      "jdbc:optiq:", info);

    Statement optiqStatement = optiqConnection.createStatement();
    ResultSet rs = optiqStatement.executeQuery(
      "SELECT ID, VALS FROM ARR_TABLE");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    Array array = rs.getArray(2);
    assertNotNull(array);
    assertArrayEquals(new Object[]{1, 2, 3}, (Object[]) array.getArray());
    assertFalse(rs.next());
    rs.close();

    rs = optiqStatement.executeQuery(
        "SELECT ID, CARDINALITY(VALS), VALS[2] FROM ARR_TABLE");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(3, rs.getInt(2));
    assertEquals(2, rs.getInt(3));
    assertFalse(rs.next());
    rs.close();

    rs = optiqStatement.executeQuery(
        "SELECT * FROM ARR_TABLE2");
    final ResultSetMetaData metaData = rs.getMetaData();
    assertThat(metaData.getColumnTypeName(1), equalTo("INTEGER"));
    assertThat(metaData.getColumnTypeName(2), equalTo("INTEGER ARRAY"));
    assertThat(metaData.getColumnTypeName(3), equalTo("VARCHAR(10) ARRAY"));
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertThat(rs.getArray(2), notNullValue());
    assertThat(rs.getArray(3), notNullValue());
    assertFalse(rs.next());

    optiqConnection.close();
  }

  private OptiqAssert.AssertQuery withFoodMartQuery(int id) throws IOException {
    final FoodmartTest.FoodMartQuerySet set =
        FoodmartTest.FoodMartQuerySet.instance();
    return OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(set.queries.get(id).sql);
  }

  /** Makes sure that a projection introduced by a call to
   * {@link org.eigenbase.rel.rules.SwapJoinRule} does not manifest as an
   * {@link net.hydromatic.optiq.rules.java.JavaRules.EnumerableCalcRel} in the
   * plan.
   *
   * <p>Test case for (not yet fixed)
   * <a href="https://github.com/julianhyde/optiq/issues/92">#92</a>, "Project
   * should be optimized away, not converted to EnumerableCalcRel".</p>
   */
  @Ignore
  @Test public void testNoCalcBetweenJoins() throws IOException {
    final FoodmartTest.FoodMartQuerySet set =
        FoodmartTest.FoodMartQuerySet.instance();
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(set.queries.get(16).sql)
        .explainContains(
            "EnumerableSortRel(sort0=[$0], sort1=[$1], sort2=[$2], sort3=[$4], sort4=[$10], sort5=[$11], sort6=[$12], sort7=[$13], sort8=[$22], sort9=[$23], sort10=[$24], sort11=[$25], sort12=[$26], sort13=[$27], dir0=[Ascending-nulls-last], dir1=[Ascending-nulls-last], dir2=[Ascending-nulls-last], dir3=[Ascending-nulls-last], dir4=[Ascending-nulls-last], dir5=[Ascending-nulls-last], dir6=[Ascending-nulls-last], dir7=[Ascending-nulls-last], dir8=[Ascending-nulls-last], dir9=[Ascending-nulls-last], dir10=[Ascending-nulls-last], dir11=[Ascending-nulls-last], dir12=[Ascending-nulls-last], dir13=[Ascending-nulls-last])\n"
            + "  EnumerableCalcRel(expr#0..26=[{inputs}], proj#0..4=[{exprs}], c5=[$t4], c6=[$t5], c7=[$t6], c8=[$t7], c9=[$t8], c10=[$t9], c11=[$t10], c12=[$t11], c13=[$t12], c14=[$t13], c15=[$t14], c16=[$t15], c17=[$t16], c18=[$t17], c19=[$t18], c20=[$t19], c21=[$t20], c22=[$t21], c23=[$t22], c24=[$t23], c25=[$t24], c26=[$t25], c27=[$t26])\n"
            + "    EnumerableAggregateRel(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26}])\n"
            + "      EnumerableCalcRel(expr#0..80=[{inputs}], c0=[$t12], c1=[$t10], c2=[$t9], c3=[$t0], fullname=[$t28], c6=[$t19], c7=[$t17], c8=[$t22], c9=[$t18], c10=[$t46], c11=[$t44], c12=[$t43], c13=[$t40], c14=[$t38], c15=[$t47], c16=[$t52], c17=[$t53], c18=[$t54], c19=[$t55], c20=[$t56], c21=[$t42], c22=[$t80], c23=[$t79], c24=[$t78], c25=[$t77], c26=[$t63], c27=[$t64])\n"
            + "        EnumerableJoinRel(condition=[=($61, $76)], joinType=[inner])\n"
            + "          EnumerableJoinRel(condition=[=($29, $62)], joinType=[inner])\n"
            + "            EnumerableJoinRel(condition=[=($33, $37)], joinType=[inner])\n"
            + "              EnumerableCalcRel(expr#0..36=[{inputs}], customer_id=[$t8], account_num=[$t9], lname=[$t10], fname=[$t11], mi=[$t12], address1=[$t13], address2=[$t14], address3=[$t15], address4=[$t16], city=[$t17], state_province=[$t18], postal_code=[$t19], country=[$t20], customer_region_id=[$t21], phone1=[$t22], phone2=[$t23], birthdate=[$t24], marital_status=[$t25], yearly_income=[$t26], gender=[$t27], total_children=[$t28], num_children_at_home=[$t29], education=[$t30], date_accnt_opened=[$t31], member_card=[$t32], occupation=[$t33], houseowner=[$t34], num_cars_owned=[$t35], fullname=[$t36], product_id=[$t0], time_id=[$t1], customer_id0=[$t2], promotion_id=[$t3], store_id=[$t4], store_sales=[$t5], store_cost=[$t6], unit_sales=[$t7])\n"
            + "                EnumerableJoinRel(condition=[=($2, $8)], joinType=[inner])\n"
            + "                  EnumerableTableAccessRel(table=[[foodmart2, sales_fact_1997]])\n"
            + "                  EnumerableTableAccessRel(table=[[foodmart2, customer]])\n"
            + "              EnumerableTableAccessRel(table=[[foodmart2, store]])\n"
            + "            EnumerableTableAccessRel(table=[[foodmart2, product]])\n"
            + "          EnumerableTableAccessRel(table=[[foodmart2, product_class]])\n");
  }

  /** Checks that a 3-way join is re-ordered so that join conditions can be
   * applied. The plan must not contain cartesian joins.
   * {@link org.eigenbase.rel.rules.PushJoinThroughJoinRule} makes this
   * possible. */
  @Ignore
  @Test public void testExplainJoin() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(FOODMART_QUERIES.get(48).left)
        .explainContains(
            "EnumerableAggregateRel(group=[{}], m0=[COUNT($0)])\n"
            + "  EnumerableAggregateRel(group=[{0}])\n"
            + "    EnumerableCalcRel(expr#0..27=[{inputs}], customer_id=[$t7])\n"
            + "      EnumerableJoinRel(condition=[=($13, $0)], joinType=[inner])\n"
            + "        EnumerableTableAccessRel(table=[[foodmart2, product_class]])\n"
            + "        EnumerableJoinRel(condition=[=($0, $9)], joinType=[inner])\n"
            + "          EnumerableTableAccessRel(table=[[foodmart2, sales_fact_1997]])\n"
            + "          EnumerableCalcRel(expr#0..14=[{inputs}], expr#15=['Cormorant'], expr#16=[=($t2, $t15)], proj#0..14=[{exprs}], $condition=[$t16])\n"
            + "            EnumerableTableAccessRel(table=[[foodmart2, product]]");
  }

  /** Checks that a 3-way join is re-ordered so that join conditions can be
   * applied. The plan is left-deep (agg_c_14_sales_fact_1997 the most
   * rows, then time_by_day, then store). This makes for efficient
   * hash-joins. */
  @Ignore
  @Test public void testExplainJoin2() throws IOException {
    withFoodMartQuery(2482)
        .explainContains(
            "EnumerableSortRel(sort0=[$0], sort1=[$1], dir0=[Ascending-nulls-last], dir1=[Ascending-nulls-last])\n"
            + "  EnumerableAggregateRel(group=[{0, 1}])\n"
            + "    EnumerableCalcRel(expr#0..5=[{inputs}], c0=[$t4], c1=[$t1])\n"
            + "      EnumerableJoinRel(condition=[=($3, $5)], joinType=[inner])\n"
            + "        EnumerableCalcRel(expr#0..3=[{inputs}], store_id=[$t2], store_country=[$t3], store_id0=[$t0], month_of_year=[$t1])\n"
            + "          EnumerableJoinRel(condition=[=($0, $2)], joinType=[inner])\n"
            + "            EnumerableCalcRel(expr#0..10=[{inputs}], store_id=[$t2], month_of_year=[$t4])\n"
            + "              EnumerableTableAccessRel(table=[[foodmart2, agg_c_14_sales_fact_1997]])\n"
            + "            EnumerableCalcRel(expr#0..23=[{inputs}], store_id=[$t0], store_country=[$t9])\n"
            + "              EnumerableTableAccessRel(table=[[foodmart2, store]])\n"
            + "        EnumerableCalcRel(expr#0..9=[{inputs}], the_year=[$t4], month_of_year=[$t7])\n"
            + "          EnumerableTableAccessRel(table=[[foodmart2, time_by_day]])\n")
        .runs();
  }

  /** One of the most expensive foodmart queries. */
  @Ignore // OOME on Travis; works on most other machines
  @Test public void testExplainJoin3() throws IOException {
    withFoodMartQuery(8)
        .explainContains(
            "EnumerableSortRel(sort0=[$0], sort1=[$1], sort2=[$2], sort3=[$4], dir0=[Ascending-nulls-last], dir1=[Ascending-nulls-last], dir2=[Ascending-nulls-last], dir3=[Ascending-nulls-last])\n"
            + "  EnumerableCalcRel(expr#0..8=[{inputs}], expr#9=['%Jeanne%'], expr#10=[LIKE($t4, $t9)], proj#0..4=[{exprs}], c5=[$t4], c6=[$t5], c7=[$t6], c8=[$t7], c9=[$t8], $condition=[$t10])\n"
            + "    EnumerableAggregateRel(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8}])\n"
            + "      EnumerableCalcRel(expr#0..46=[{inputs}], c0=[$t12], c1=[$t10], c2=[$t9], c3=[$t0], fullname=[$t28], c6=[$t19], c7=[$t17], c8=[$t22], c9=[$t18])\n"
            + "        EnumerableJoinRel(condition=[=($30, $37)], joinType=[inner])\n"
            + "          EnumerableCalcRel(expr#0..36=[{inputs}], customer_id=[$t8], account_num=[$t9], lname=[$t10], fname=[$t11], mi=[$t12], address1=[$t13], address2=[$t14], address3=[$t15], address4=[$t16], city=[$t17], state_province=[$t18], postal_code=[$t19], country=[$t20], customer_region_id=[$t21], phone1=[$t22], phone2=[$t23], birthdate=[$t24], marital_status=[$t25], yearly_income=[$t26], gender=[$t27], total_children=[$t28], num_children_at_home=[$t29], education=[$t30], date_accnt_opened=[$t31], member_card=[$t32], occupation=[$t33], houseowner=[$t34], num_cars_owned=[$t35], fullname=[$t36], product_id=[$t0], time_id=[$t1], customer_id0=[$t2], promotion_id=[$t3], store_id=[$t4], store_sales=[$t5], store_cost=[$t6], unit_sales=[$t7])\n"
            + "            EnumerableJoinRel(condition=[=($2, $8)], joinType=[inner])\n"
            + "              EnumerableTableAccessRel(table=[[foodmart2, sales_fact_1997]])\n"
            + "              EnumerableTableAccessRel(table=[[foodmart2, customer]])\n"
            + "          EnumerableCalcRel(expr#0..9=[{inputs}], expr#10=[CAST($t4):INTEGER], expr#11=[1997], expr#12=[=($t10, $t11)], proj#0..9=[{exprs}], $condition=[$t12])\n"
            + "            EnumerableTableAccessRel(table=[[foodmart2, time_by_day]])")
        .runs();
  }

  /** Test case for (not yet fixed)
   * <a href="https://github.com/julianhyde/optiq/issues/99">issue #99</a>,
   * "Recognize semi-join that has high selectivity and push it down". */
  @Ignore
  @Test public void testExplainJoin4() throws IOException {
    withFoodMartQuery(5217)
        .explainContains(
            "EnumerableAggregateRel(group=[{0, 1, 2, 3}], m0=[COUNT($4)])\n"
            + "  EnumerableCalcRel(expr#0..69=[{inputs}], c0=[$t4], c1=[$t27], c2=[$t61], c3=[$t66], $f11=[$t11])\n"
            + "    EnumerableJoinRel(condition=[=($68, $69)], joinType=[inner])\n"
            + "      EnumerableCalcRel(expr#0..67=[{inputs}], proj#0..67=[{exprs}], $f68=[$t66])\n"
            + "        EnumerableJoinRel(condition=[=($11, $65)], joinType=[inner])\n"
            + "          EnumerableJoinRel(condition=[=($46, $59)], joinType=[inner])\n"
            + "            EnumerableCalcRel(expr#0..58=[{inputs}], $f0=[$t49], $f1=[$t50], $f2=[$t51], $f3=[$t52], $f4=[$t53], $f5=[$t54], $f6=[$t55], $f7=[$t56], $f8=[$t57], $f9=[$t58], $f10=[$t41], $f11=[$t42], $f12=[$t43], $f13=[$t44], $f14=[$t45], $f15=[$t46], $f16=[$t47], $f17=[$t48], $f18=[$t0], $f19=[$t1], $f20=[$t2], $f21=[$t3], $f22=[$t4], $f23=[$t5], $f24=[$t6], $f25=[$t7], $f26=[$t8], $f27=[$t9], $f28=[$t10], $f29=[$t11], $f30=[$t12], $f31=[$t13], $f32=[$t14], $f33=[$t15], $f34=[$t16], $f35=[$t17], $f36=[$t18], $f37=[$t19], $f38=[$t20], $f39=[$t21], $f40=[$t22], $f41=[$t23], $f42=[$t24], $f43=[$t25], $f44=[$t26], $f45=[$t27], $f46=[$t28], $f47=[$t29], $f48=[$t30], $f49=[$t31], $f50=[$t32], $f51=[$t33], $f52=[$t34], $f53=[$t35], $f54=[$t36], $f55=[$t37], $f56=[$t38], $f57=[$t39], $f58=[$t40])\n"
            + "              EnumerableJoinRel(condition=[=($41, $50)], joinType=[inner])\n"
            + "                EnumerableCalcRel(expr#0..48=[{inputs}], $f0=[$t25], $f1=[$t26], $f2=[$t27], $f3=[$t28], $f4=[$t29], $f5=[$t30], $f6=[$t31], $f7=[$t32], $f8=[$t33], $f9=[$t34], $f10=[$t35], $f11=[$t36], $f12=[$t37], $f13=[$t38], $f14=[$t39], $f15=[$t40], $f16=[$t41], $f17=[$t42], $f18=[$t43], $f19=[$t44], $f20=[$t45], $f21=[$t46], $f22=[$t47], $f23=[$t48], $f24=[$t8], $f25=[$t9], $f26=[$t10], $f27=[$t11], $f28=[$t12], $f29=[$t13], $f30=[$t14], $f31=[$t15], $f32=[$t16], $f33=[$t17], $f34=[$t18], $f35=[$t19], $f36=[$t20], $f37=[$t21], $f38=[$t22], $f39=[$t23], $f40=[$t24], $f41=[$t0], $f42=[$t1], $f43=[$t2], $f44=[$t3], $f45=[$t4], $f46=[$t5], $f47=[$t6], $f48=[$t7])\n"
            + "                  EnumerableJoinRel(condition=[=($14, $25)], joinType=[inner])\n"
            + "                    EnumerableJoinRel(condition=[=($1, $8)], joinType=[inner])\n"
            + "                      EnumerableTableAccessRel(table=[[foodmart2, salary]])\n"
            + "                      EnumerableTableAccessRel(table=[[foodmart2, employee]])\n"
            + "                    EnumerableTableAccessRel(table=[[foodmart2, store]])\n"
            + "                EnumerableTableAccessRel(table=[[foodmart2, time_by_day]])\n"
            + "            EnumerableTableAccessRel(table=[[foodmart2, position]])\n"
            + "          EnumerableTableAccessRel(table=[[foodmart2, employee_closure]])\n"
            + "      EnumerableAggregateRel(group=[{0}])\n"
            + "        EnumerableValuesRel(tuples=[[{ 1 }, { 2 }, { 20 }, { 21 }, { 22 }, { 23 }, { 24 }, { 25 }, { 26 }, { 27 }, { 28 }, { 29 }, { 30 }, { 31 }, { 53 }, { 54 }, { 55 }, { 56 }, { 57 }, { 58 }, { 59 }, { 60 }, { 61 }, { 62 }, { 63 }, { 64 }, { 65 }, { 66 }, { 67 }, { 68 }, { 69 }, { 70 }, { 71 }, { 72 }, { 73 }, { 74 }, { 75 }, { 76 }, { 77 }, { 78 }, { 79 }, { 80 }, { 81 }, { 82 }, { 83 }, { 84 }, { 85 }, { 86 }, { 87 }, { 88 }, { 89 }, { 90 }, { 91 }, { 92 }, { 93 }, { 94 }, { 95 }, { 96 }, { 97 }, { 98 }, { 99 }, { 100 }, { 101 }, { 102 }, { 103 }, { 104 }, { 105 }, { 106 }, { 107 }, { 108 }, { 109 }, { 110 }, { 111 }, { 112 }, { 113 }, { 114 }, { 115 }, { 116 }, { 117 }, { 118 }, { 119 }, { 120 }, { 121 }, { 122 }, { 123 }, { 124 }, { 125 }, { 126 }, { 127 }, { 128 }, { 129 }, { 130 }, { 131 }, { 132 }, { 133 }, { 134 }, { 135 }, { 136 }, { 137 }, { 138 }, { 139 }, { 140 }, { 141 }, { 142 }, { 143 }, { 144 }, { 145 }, { 146 }, { 147 }, { 148 }, { 149 }, { 150 }, { 151 }, { 152 }, { 153 }, { 154 }, { 155 }, { 156 }, { 157 }, { 158 }, { 159 }, { 160 }, { 161 }, { 162 }, { 163 }, { 164 }, { 165 }, { 166 }, { 167 }, { 168 }, { 169 }, { 170 }, { 171 }, { 172 }, { 173 }, { 174 }, { 175 }, { 176 }, { 177 }, { 178 }, { 179 }, { 180 }, { 181 }, { 182 }, { 183 }, { 184 }, { 185 }, { 186 }, { 187 }, { 188 }, { 189 }, { 190 }, { 191 }, { 192 }, { 193 }, { 194 }, { 195 }, { 196 }, { 197 }, { 198 }, { 199 }, { 200 }, { 201 }, { 202 }, { 203 }, { 204 }, { 205 }, { 206 }, { 207 }, { 208 }, { 209 }, { 210 }, { 211 }, { 212 }, { 213 }, { 214 }, { 215 }, { 216 }, { 217 }, { 218 }, { 219 }, { 220 }, { 221 }, { 222 }, { 223 }, { 224 }, { 225 }, { 226 }, { 227 }, { 228 }, { 229 }, { 230 }, { 231 }, { 232 }, { 233 }, { 234 }, { 235 }, { 236 }, { 237 }, { 238 }, { 239 }, { 240 }, { 241 }, { 242 }, { 243 }, { 244 }, { 245 }, { 246 }, { 247 }, { 248 }, { 249 }, { 250 }, { 251 }, { 252 }, { 253 }, { 254 }, { 255 }, { 256 }, { 257 }, { 258 }, { 259 }, { 260 }, { 261 }, { 262 }, { 263 }, { 264 }, { 265 }, { 266 }, { 267 }, { 268 }, { 269 }, { 270 }, { 271 }, { 272 }, { 273 }, { 274 }, { 275 }, { 276 }, { 277 }, { 278 }, { 279 }, { 280 }, { 281 }, { 282 }, { 283 }, { 284 }, { 285 }, { 286 }, { 287 }, { 288 }, { 289 }, { 290 }, { 291 }, { 292 }, { 293 }, { 294 }, { 295 }, { 296 }, { 297 }, { 298 }, { 299 }, { 300 }, { 301 }, { 302 }, { 303 }, { 304 }, { 305 }, { 306 }, { 307 }, { 308 }, { 309 }, { 310 }, { 311 }, { 312 }, { 313 }, { 314 }, { 315 }, { 316 }, { 317 }, { 318 }, { 319 }, { 320 }, { 321 }, { 322 }, { 323 }, { 324 }, { 325 }, { 326 }, { 327 }, { 328 }, { 329 }, { 330 }, { 331 }, { 332 }, { 333 }, { 334 }, { 335 }, { 336 }, { 337 }, { 338 }, { 339 }, { 340 }, { 341 }, { 342 }, { 343 }, { 344 }, { 345 }, { 346 }, { 347 }, { 348 }, { 349 }, { 350 }, { 351 }, { 352 }, { 353 }, { 354 }, { 355 }, { 356 }, { 357 }, { 358 }, { 359 }, { 360 }, { 361 }, { 362 }, { 363 }, { 364 }, { 365 }, { 366 }, { 367 }, { 368 }, { 369 }, { 370 }, { 371 }, { 372 }, { 373 }, { 374 }, { 375 }, { 376 }, { 377 }, { 378 }, { 379 }, { 380 }, { 381 }, { 382 }, { 383 }, { 384 }, { 385 }, { 386 }, { 387 }, { 388 }, { 389 }, { 390 }, { 391 }, { 392 }, { 393 }, { 394 }, { 395 }, { 396 }, { 397 }, { 398 }, { 399 }, { 400 }, { 401 }, { 402 }, { 403 }, { 404 }, { 405 }, { 406 }, { 407 }, { 408 }, { 409 }, { 410 }, { 411 }, { 412 }, { 413 }, { 414 }, { 415 }, { 416 }, { 417 }, { 418 }, { 419 }, { 420 }, { 421 }, { 422 }, { 423 }, { 424 }, { 425 }, { 430 }, { 431 }, { 432 }, { 433 }, { 434 }, { 435 }, { 436 }, { 437 }, { 442 }, { 443 }, { 444 }, { 445 }, { 446 }, { 447 }, { 448 }, { 449 }, { 450 }, { 451 }, { 457 }, { 458 }, { 459 }, { 460 }, { 461 }, { 462 }, { 463 }, { 469 }, { 470 }, { 471 }, { 472 }, { 473 }]])\n")
        .runs();
  }

  /** Condition involving OR makes this more complex than
   * {@link #testExplainJoin()}. */
  @Ignore
  @Test public void testExplainJoinOrderingWithOr() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(FOODMART_QUERIES.get(47).left)
        .explainContains("xxx");
  }

  /** There was a bug representing a nullable timestamp using a {@link Long}
   * internally. */
  @Test public void testNullableTimestamp() {
    checkNullableTimestamp(OptiqAssert.Config.FOODMART_CLONE);
  }

  /** Similar to {@link #testNullableTimestamp} but directly off JDBC. */
  @Test public void testNullableTimestamp2() {
    checkNullableTimestamp(OptiqAssert.Config.JDBC_FOODMART);
  }

  private void checkNullableTimestamp(OptiqAssert.Config config) {
    OptiqAssert.that()
        .with(config)
        .query(
            "select \"hire_date\", \"end_date\", \"birth_date\" from \"foodmart\".\"employee\" where \"employee_id\" = 1")
        .returns(
            "hire_date=1994-12-01 00:00:00; end_date=null; birth_date=1961-08-26\n");
  }

  @Test public void testReuseExpressionWhenNullChecking() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select upper((case when \"empid\">\"deptno\"*10 then 'y' else null end)) T from \"hr\".\"emps\"")
        .planContains(
            "static final String $L4J$C$net_hydromatic_optiq_runtime_SqlFunctions_upper_y_ = "
            + "net.hydromatic.optiq.runtime.SqlFunctions.upper(\"y\");")
        .planContains(
            "return current.empid <= current.deptno * 10 "
            + "? (String) null "
            + ": $L4J$C$net_hydromatic_optiq_runtime_SqlFunctions_upper_y_;")
        .returns("T=null\n"
                 + "T=null\n"
                 + "T=Y\n"
                 + "T=Y\n");
  }

  @Test public void testReuseExpressionWhenNullChecking2() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select upper((case when \"empid\">\"deptno\"*10 then \"name\" end)) T from \"hr\".\"emps\"")
        .planContains(
            "final String inp2_ = current.name;")
        .planContains(
            "return current.empid <= current.deptno * 10 "
            + "|| inp2_ == null "
            + "? (String) null "
            + ": net.hydromatic.optiq.runtime.SqlFunctions.upper(inp2_);")
        .returns("T=null\n"
                 + "T=null\n"
                 + "T=SEBASTIAN\n"
                 + "T=THEODORE\n");
  }

  @Test public void testReuseExpressionWhenNullChecking3() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select substring(\"name\", \"deptno\"+case when user <> 'sa' then 1 end) from \"hr\".\"emps\"")
        .planContains(
            "final String inp2_ = current.name;")
        .planContains(
            "static final boolean $L4J$C$net_hydromatic_optiq_runtime_SqlFunctions_ne_sa_sa_ = "
            + "net.hydromatic.optiq.runtime.SqlFunctions.ne(\"sa\", \"sa\");")
        .planContains(
            "static final boolean $L4J$C$_net_hydromatic_optiq_runtime_SqlFunctions_ne_sa_sa_ = "
            + "!$L4J$C$net_hydromatic_optiq_runtime_SqlFunctions_ne_sa_sa_;")
        .planContains(
            "return inp2_ == null || $L4J$C$_net_hydromatic_optiq_runtime_SqlFunctions_ne_sa_sa_ ? (String) null"
            + " : net.hydromatic.optiq.runtime.SqlFunctions.substring(inp2_, "
            + "current.deptno + 1);");
  }

  @Test public void testReuseExpressionWhenNullChecking4() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select substring(trim(\n"
            + "substring(\"name\",\n"
            + "  \"deptno\"*0+case when user = 'sa' then 1 end)\n"
            + "), case when \"empid\">\"deptno\" then 4\n" /* diff from 5 */
            + "   else\n"
            + "     case when \"deptno\"*8>8 then 5 end\n"
            + "   end-2) T\n"
            + "from\n"
            + "\"hr\".\"emps\"")
        .planContains(
            "final String inp2_ = current.name;")
        .planContains(
            "final int inp1_ = current.deptno;")
        .planContains(
            "static final boolean $L4J$C$net_hydromatic_optiq_runtime_SqlFunctions_eq_sa_sa_ = "
            + "net.hydromatic.optiq.runtime.SqlFunctions.eq(\"sa\", \"sa\");")
        .planContains(
            "static final boolean $L4J$C$_net_hydromatic_optiq_runtime_SqlFunctions_eq_sa_sa_ = "
            + "!$L4J$C$net_hydromatic_optiq_runtime_SqlFunctions_eq_sa_sa_;")
        .planContains(
            "return inp2_ == null "
            + "|| $L4J$C$_net_hydromatic_optiq_runtime_SqlFunctions_eq_sa_sa_ "
            + "|| !v5 && inp1_ * 8 <= 8 "
            + "? (String) null "
            + ": net.hydromatic.optiq.runtime.SqlFunctions.substring("
            + "net.hydromatic.optiq.runtime.SqlFunctions.trim(true, true, \" \", "
            + "net.hydromatic.optiq.runtime.SqlFunctions.substring(inp2_, "
            + "inp1_ * 0 + 1)), (v5 ? 4 : 5) - 2);")
        .returns("T=ill\n"
                 + "T=ric\n"
                 + "T=ebastian\n"
                 + "T=heodore\n");
  }

  @Test public void testReuseExpressionWhenNullChecking5() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select substring(trim(\n"
            + "substring(\"name\",\n"
            + "  \"deptno\"*0+case when user = 'sa' then 1 end)\n"
            + "), case when \"empid\">\"deptno\" then 5\n" /* diff from 4 */
            + "   else\n"
            + "     case when \"deptno\"*8>8 then 5 end\n"
            + "   end-2) T\n"
            + "from\n"
            + "\"hr\".\"emps\"")
        .planContains(
            "final String inp2_ = current.name;")
        .planContains(
            "final int inp1_ = current.deptno;")
        .planContains(
            "static final int $L4J$C$5_2 = 5 - 2;")
        .planContains(
            "static final boolean $L4J$C$net_hydromatic_optiq_runtime_SqlFunctions_eq_sa_sa_ = "
            + "net.hydromatic.optiq.runtime.SqlFunctions.eq(\"sa\", \"sa\");")
        .planContains(
            "static final boolean $L4J$C$_net_hydromatic_optiq_runtime_SqlFunctions_eq_sa_sa_ = "
            + "!$L4J$C$net_hydromatic_optiq_runtime_SqlFunctions_eq_sa_sa_;")
        .planContains(
            "return inp2_ == null "
            + "|| $L4J$C$_net_hydromatic_optiq_runtime_SqlFunctions_eq_sa_sa_ "
            + "|| current.empid <= inp1_ && inp1_ * 8 <= 8 "
            + "? (String) null "
            + ": net.hydromatic.optiq.runtime.SqlFunctions.substring("
            + "net.hydromatic.optiq.runtime.SqlFunctions.trim(true, true, \" \", "
            + "net.hydromatic.optiq.runtime.SqlFunctions.substring(inp2_, "
            + "inp1_ * 0 + 1)), $L4J$C$5_2);")
        .returns("T=ll\n"
                 + "T=ic\n"
                 + "T=bastian\n"
                 + "T=eodore\n");
  }

  @Test public void testValues() {
    OptiqAssert.that()
        .query("values (1), (2)")
        .returns(
            "EXPR$0=1\n"
            + "EXPR$0=2\n");
  }

  @Test public void testValuesAlias() {
    OptiqAssert.that()
        .query(
            "select \"desc\" from (VALUES ROW(1, 'SameName')) AS \"t\" (\"id\", \"desc\")")
        .returns("desc=SameName\n");
  }

  @Test public void testValuesMinus() {
    OptiqAssert.that()
        .query("values (-2-1)")
        .returns("EXPR$0=-3\n");
  }

  /**
   * Conversions table, per JDBC 1.1 specification, table 6.
   *
   * <pre>
   *                    T S I B R F D D N B C V L B V L D T T
   *                    I M N I E L O E U I H A O I A O A I I
   *                    N A T G A O U C M T A R N N R N T M M
   *                    Y L E I L A B I E   R C G A B G E E E
   *                    I L G N     L M R     H V R I V E   S
   *                    N I E T     E A I     A A Y   A     T
   *                    T N R         L C     R R     R     A
   *                      T                     C     B     M
   *                                            H     I     P
   * Java type
   * ================== = = = = = = = = = = = = = = = = = = =
   * String             x x x x x x x x x x x x x x x x x x x
   * BigDecimal         x x x x x x x x x x x x . . . . . . .
   * Boolean            x x x x x x x x x x x x . . . . . . .
   * Integer            x x x x x x x x x x x x . . . . . . .
   * Long               x x x x x x x x x x x x . . . . . . .
   * Float              x x x x x x x x x x x x . . . . . . .
   * Double             x x x x x x x x x x x x . . . . . . .
   * byte[]             . . . . . . . . . . . . . x x x . . .
   * java.sql.Date      . . . . . . . . . . x x x . . . x . x
   * java.sql.Time      . . . . . . . . . . x x x . . . . x .
   * java.sql.Timestamp . . . . . . . . . . x x x . . . x x x
   * </pre>
   */
  public static final ImmutableMultimap<Class, Integer> CONVERSIONS = x();

  private static ImmutableMultimap<Class, Integer> x() {
    final ImmutableMultimap.Builder<Class, Integer> builder =
        ImmutableMultimap.builder();
    int[] allTypes = {
      java.sql.Types.TINYINT,
      java.sql.Types.SMALLINT,
      java.sql.Types.INTEGER,
      java.sql.Types.BIGINT,
      java.sql.Types.REAL,
      java.sql.Types.FLOAT,
      java.sql.Types.DOUBLE,
      java.sql.Types.DECIMAL,
      java.sql.Types.NUMERIC,
      java.sql.Types.BIT,
      java.sql.Types.CHAR,
      java.sql.Types.VARCHAR,
      java.sql.Types.LONGVARCHAR,
      java.sql.Types.BINARY,
      java.sql.Types.VARBINARY,
      java.sql.Types.LONGVARBINARY,
      java.sql.Types.DATE,
      java.sql.Types.TIME,
      java.sql.Types.TIMESTAMP
    };
    int[] numericTypes = {
      java.sql.Types.TINYINT,
      java.sql.Types.SMALLINT,
      java.sql.Types.INTEGER,
      java.sql.Types.BIGINT,
      java.sql.Types.REAL,
      java.sql.Types.FLOAT,
      java.sql.Types.DOUBLE,
      java.sql.Types.DECIMAL,
      java.sql.Types.NUMERIC,
      java.sql.Types.BIT
    };
    Class[] numericClasses = {
      BigDecimal.class, Boolean.class, Integer.class, Long.class, Float.class,
      Double.class
    };
    Class[] allClasses = {
      String.class, BigDecimal.class, Boolean.class, Integer.class,
      Long.class, Float.class, Double.class, byte[].class,
      java.sql.Date.class, java.sql.Time.class, java.sql.Timestamp.class,
    };
    int[] charTypes = {
      java.sql.Types.CHAR,
      java.sql.Types.VARCHAR,
      java.sql.Types.LONGVARCHAR
    };
    int[] binaryTypes = {
      java.sql.Types.BINARY,
      java.sql.Types.VARBINARY,
      java.sql.Types.LONGVARBINARY
    };
    for (int type : allTypes) {
      builder.put(String.class, type);
    }
    for (Class clazz : numericClasses) {
      for (int type : numericTypes) {
        builder.put(clazz, type);
      }
    }
    for (int type : charTypes) {
      for (Class clazz : allClasses) {
        builder.put(clazz, type);
      }
    }
    for (int type : binaryTypes) {
      builder.put(byte[].class, type);
    }
    builder.put(java.sql.Date.class, java.sql.Types.DATE);
    builder.put(java.sql.Date.class, java.sql.Types.TIMESTAMP);
    builder.put(java.sql.Time.class, java.sql.Types.TIME);
    builder.put(java.sql.Timestamp.class, java.sql.Types.DATE);
    builder.put(java.sql.Time.class, java.sql.Types.TIME);
    builder.put(java.sql.Timestamp.class, java.sql.Types.TIMESTAMP);
    return builder.build();
  }

  /** Tests a table constructor that has multiple rows and multiple columns.
   *
   * <p>Note that the character literals become CHAR(3) and that the first is
   * correctly rendered with trailing spaces: 'a  '. If we were inserting
   * into a VARCHAR column the behavior would be different; the literals
   * would be converted into VARCHAR(3) values and the implied cast from
   * CHAR(1) to CHAR(3) that appends trailing spaces does not occur. See
   * "contextually typed value specification" in the SQL spec.</p>
   */
  @Test public void testValuesComposite() {
    OptiqAssert.that()
        .query("values (1, 'a'), (2, 'abc')")
        .returns(
            "EXPR$0=1; EXPR$1=a  \n"
            + "EXPR$0=2; EXPR$1=abc\n");
  }

  /** Tests inner join to an inline table ({@code VALUES} clause). */
  @Test public void testInnerJoinValues() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.LINGUAL)
        .query(
            "select empno, desc from sales.emps,\n"
            + "  (SELECT * FROM (VALUES (10, 'SameName')) AS t (id, desc)) as sn\n"
            + "where emps.deptno = sn.id and sn.desc = 'SameName' group by empno, desc")
        .returns("EMPNO=1; DESC=SameName\n");
  }

  /** Tests a cartesian product aka cross join. */
  @Test public void testCartesianJoin() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select * from \"hr\".\"emps\", \"hr\".\"depts\" where \"emps\".\"empid\" < 140 and \"depts\".\"deptno\" > 20")
        .returnsUnordered(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000; deptno0=30; name0=Marketing; employees=[]",
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000; deptno0=40; name0=HR; employees=[Employee [empid: 200, deptno: 20, name: Eric]]",
            "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250; deptno0=30; name0=Marketing; employees=[]",
            "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250; deptno0=40; name0=HR; employees=[Employee [empid: 200, deptno: 20, name: Eric]]");
  }

  @Test public void testDistinctCount() {
    final String s =
        "select \"time_by_day\".\"the_year\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 group by \"time_by_day\".\"the_year\"";
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(s)
        .returns("c0=1997; m0=266773.0000\n");
  }

  /** A difficult query: an IN list so large that the planner promotes it
   * to a semi-join against a VALUES relation. */
  @Ignore
  @Test public void testIn() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"time_by_day\".\"the_year\" as \"c0\",\n"
            + " \"product_class\".\"product_family\" as \"c1\",\n"
            + " \"customer\".\"country\" as \"c2\",\n"
            + " \"customer\".\"state_province\" as \"c3\",\n"
            + " \"customer\".\"city\" as \"c4\",\n"
            + " sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\"\n"
            + "from \"time_by_day\" as \"time_by_day\",\n"
            + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
            + " \"product_class\" as \"product_class\",\n"
            + " \"product\" as \"product\", \"customer\" as \"customer\"\n"
            + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
            + "and \"time_by_day\".\"the_year\" = 1997\n"
            + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
            + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
            + "and \"product_class\".\"product_family\" = 'Drink'\n"
            + "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
            + "and \"customer\".\"country\" = 'USA'\n"
            + "and \"customer\".\"state_province\" = 'WA'\n"
            + "and \"customer\".\"city\" in ('Anacortes', 'Ballard', 'Bellingham', 'Bremerton', 'Burien', 'Edmonds', 'Everett', 'Issaquah', 'Kirkland', 'Lynnwood', 'Marysville', 'Olympia', 'Port Orchard', 'Puyallup', 'Redmond', 'Renton', 'Seattle', 'Sedro Woolley', 'Spokane', 'Tacoma', 'Walla Walla', 'Yakima')\n"
            + "group by \"time_by_day\".\"the_year\",\n"
            + " \"product_class\".\"product_family\",\n"
            + " \"customer\".\"country\",\n"
            + " \"customer\".\"state_province\",\n"
            + " \"customer\".\"city\"")
        .returns(
            "c0=1997; c1=Drink; c2=USA; c3=WA; c4=Sedro Woolley; m0=58.0000\n");
  }

  /** Query that uses parenthesized JOIN. */
  @Test public void testSql92JoinParenthesized() {
    if (!Bug.TODO_FIXED) {
      return;
    }
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select\n"
            + "   \"product_class\".\"product_family\" as \"c0\",\n"
            + "   \"product_class\".\"product_department\" as \"c1\",\n"
            + "   \"customer\".\"country\" as \"c2\",\n"
            + "   \"customer\".\"state_province\" as \"c3\",\n"
            + "   \"customer\".\"city\" as \"c4\"\n"
            + "from\n"
            + "   \"sales_fact_1997\" as \"sales_fact_1997\"\n"
            + "join (\"product\" as \"product\"\n"
            + "     join \"product_class\" as \"product_class\"\n"
            + "     on \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\")\n"
            + "on  \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
            + "join \"customer\" as \"customer\"\n"
            + "on  \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
            + "join \"promotion\" as \"promotion\"\n"
            + "on \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
            + "where (\"promotion\".\"media_type\" = 'Radio'\n"
            + " or \"promotion\".\"media_type\" = 'TV'\n"
            + " or \"promotion\".\"media_type\" = 'Sunday Paper'\n"
            + " or \"promotion\".\"media_type\" = 'Street Handout')\n"
            + " and (\"product_class\".\"product_family\" = 'Drink')\n"
            + " and (\"customer\".\"country\" = 'USA' and \"customer\".\"state_province\""
            + " = 'WA' and \"customer\".\"city\" = 'Bellingham')\n"
            + "group by \"product_class\".\"product_family\",\n"
            + "   \"product_class\".\"product_department\",\n"
            + "   \"customer\".\"country\",\n"
            + "   \"customer\".\"state_province\",\n"
            + "   \"customer\".\"city\"\n"
            + "order by \"product_class\".\"product_family\" ASC,\n"
            + "   \"product_class\".\"product_department\" ASC,\n"
            + "   \"customer\".\"country\" ASC,\n"
            + "   \"customer\".\"state_province\" ASC,\n"
            + "   \"customer\".\"city\" ASC")
        .returns(
            "+-------+---------------------+-----+------+------------+\n"
            + "| c0    | c1                  | c2  | c3   | c4         |\n"
            + "+-------+---------------------+-----+------+------------+\n"
            + "| Drink | Alcoholic Beverages | USA | WA   | Bellingham |\n"
            + "| Drink | Dairy               | USA | WA   | Bellingham |\n"
            + "+-------+---------------------+-----+------+------------+\n");
  }

  /** Tests ORDER BY with no options. Nulls come last.
   *
   * @see net.hydromatic.avatica.AvaticaDatabaseMetaData#nullsAreSortedAtEnd()
   */
  @Test public void testOrderBy() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"store_id\", \"grocery_sqft\" from \"store\"\n"
            + "where \"store_id\" < 3 order by 2")
        .returns(
            "store_id=1; grocery_sqft=17475\n"
            + "store_id=2; grocery_sqft=22271\n"
            + "store_id=0; grocery_sqft=null\n");
  }

  /** Tests ORDER BY ... DESC. Nulls come last, as for ASC. */
  @Test public void testOrderByDesc() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"store_id\", \"grocery_sqft\" from \"store\"\n"
            + "where \"store_id\" < 3 order by 2 desc")
        .returns(
            "store_id=2; grocery_sqft=22271\n"
            + "store_id=1; grocery_sqft=17475\n"
            + "store_id=0; grocery_sqft=null\n");
  }

  /** Tests sorting by an expression not in the select clause. */
  @Test public void testOrderByExpr() {
    OptiqAssert.that().with(OptiqAssert.Config.REGULAR)
        .query(
            "select \"name\", \"empid\" from \"hr\".\"emps\"\n"
            + "order by - \"empid\"")
        .returns(
            "name=Eric; empid=200\n"
            + "name=Sebastian; empid=150\n"
            + "name=Theodore; empid=110\n"
            + "name=Bill; empid=100\n");
  }

  /** Tests sorting by an expression not in the '*' select clause. Test case for
   * <a href="https://github.com/julianhyde/optiq/issues/176">issue #176</a>. */
  @Test public void testOrderStarByExpr() {
    OptiqAssert.that().with(OptiqAssert.Config.REGULAR)
        .query(
            "select * from \"hr\".\"emps\"\n"
            + "order by - \"empid\"")
        .explainContains(
            "EnumerableCalcRel(expr#0..5=[{inputs}], proj#0..4=[{exprs}])\n"
            + "  EnumerableSortRel(sort0=[$5], dir0=[ASC])\n"
            + "    EnumerableCalcRel(expr#0..4=[{inputs}], expr#5=[-($t0)], proj#0..5=[{exprs}])\n"
            + "      EnumerableTableAccessRel(table=[[hr, emps]])")
        .returns(
            "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n"
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n");
  }

  @Test public void testOrderUnionStarByExpr() {
    OptiqAssert.that().with(OptiqAssert.Config.REGULAR)
        .query(
            "select * from \"hr\".\"emps\" where \"empid\" < 150\n"
            + "union all\n"
            + "select * from \"hr\".\"emps\" where \"empid\" > 150\n"
            + "order by - \"empid\"")
        .returns(
            "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500\n"
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n"
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n");
  }

  /** Tests sorting by a CAST expression not in the select clause. */
  @Test public void testOrderByCast() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"customer_id\", \"postal_code\" from \"customer\"\n"
            + "where \"customer_id\" < 5\n"
            + "order by cast(substring(\"postal_code\" from 3) as integer) desc")
        // ordered by last 3 digits (980, 674, 172, 057)
        .returns(
            "customer_id=3; postal_code=73980\n"
            + "customer_id=4; postal_code=74674\n"
            + "customer_id=2; postal_code=17172\n"
            + "customer_id=1; postal_code=15057\n");
  }

  /** Tests ORDER BY ... DESC NULLS FIRST. */
  @Test public void testOrderByDescNullsFirst() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"store_id\", \"grocery_sqft\" from \"store\"\n"
            + "where \"store_id\" < 3 order by 2 desc nulls first")
        .returns(
            "store_id=0; grocery_sqft=null\n"
            + "store_id=2; grocery_sqft=22271\n"
            + "store_id=1; grocery_sqft=17475\n");
  }

  /** Tests ORDER BY ... NULLS FIRST. */
  @Test public void testOrderByNullsFirst() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"store_id\", \"grocery_sqft\" from \"store\"\n"
            + "where \"store_id\" < 3 order by 2 nulls first")
        .returns(
            "store_id=0; grocery_sqft=null\n"
            + "store_id=1; grocery_sqft=17475\n"
            + "store_id=2; grocery_sqft=22271\n");
  }

  /** Tests ORDER BY ... DESC NULLS LAST. */
  @Test public void testOrderByDescNullsLast() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"store_id\", \"grocery_sqft\" from \"store\"\n"
            + "where \"store_id\" < 3 order by 2 desc nulls last")
        .returns(
            "store_id=2; grocery_sqft=22271\n"
            + "store_id=1; grocery_sqft=17475\n"
            + "store_id=0; grocery_sqft=null\n");
  }

  /** Tests ORDER BY ... NULLS LAST. */
  @Test public void testOrderByNullsLast() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"store_id\", \"grocery_sqft\" from \"store\"\n"
            + "where \"store_id\" < 3 order by 2 nulls last")
        .returns(
            "store_id=1; grocery_sqft=17475\n"
            + "store_id=2; grocery_sqft=22271\n"
            + "store_id=0; grocery_sqft=null\n");
  }

  /** Tests ORDER BY ... FETCH. */
  @Test public void testOrderByFetch() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"store_id\", \"grocery_sqft\" from \"store\"\n"
            + "where \"store_id\" < 10\n"
            + "order by 1 fetch first 5 rows only")
        .explainContains(
            "PLAN=EnumerableLimitRel(fetch=[5])\n"
            + "  EnumerableSortRel(sort0=[$0], dir0=[ASC])\n"
            + "    EnumerableCalcRel(expr#0..23=[{inputs}], expr#24=[10], expr#25=[<($t0, $t24)], store_id=[$t0], grocery_sqft=[$t16], $condition=[$t25])\n"
            + "      EnumerableTableAccessRel(table=[[foodmart2, store]])\n")
        .returns(
            "store_id=0; grocery_sqft=null\n"
            + "store_id=1; grocery_sqft=17475\n"
            + "store_id=2; grocery_sqft=22271\n"
            + "store_id=3; grocery_sqft=24390\n"
            + "store_id=4; grocery_sqft=16844\n");
  }

  /** Tests ORDER BY ... OFFSET ... FETCH. */
  @Test public void testOrderByOffsetFetch() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"store_id\", \"grocery_sqft\" from \"store\"\n"
            + "where \"store_id\" < 10\n"
            + "order by 1 offset 2 rows fetch next 5 rows only")
        .returns(
            "store_id=2; grocery_sqft=22271\n"
            + "store_id=3; grocery_sqft=24390\n"
            + "store_id=4; grocery_sqft=16844\n"
            + "store_id=5; grocery_sqft=15012\n"
            + "store_id=6; grocery_sqft=15337\n");
  }

  /** Tests FETCH with no ORDER BY. */
  @Test public void testFetch() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select \"empid\" from \"hr\".\"emps\"\n"
            + "fetch first 2 rows only")
        .returns(
            "empid=100\n"
            + "empid=200\n");
  }

  @Test public void testFetchStar() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select * from \"hr\".\"emps\"\n"
            + "fetch first 2 rows only")
        .returns(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500\n");
  }

  /** "SELECT ... LIMIT 0" is executed differently. A planner rule converts the
   * whole query to an empty rel. */
  @Test public void testLimitZero() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select * from \"hr\".\"emps\"\n"
            + "limit 0")
        .returns("")
        .planContains(
            "return net.hydromatic.linq4j.Linq4j.asEnumerable(new Object[] {})");
  }

  /** Alternative formulation for {@link #testFetchStar()}. */
  @Test public void testLimitStar() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select * from \"hr\".\"emps\"\n"
            + "limit 2")
        .returns(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500\n");
  }

  /** Limit implemented using {@link Queryable#take}. Test case for
   * <a href="https://github.com/julianhyde/optiq/issues/96">issue #96</a>. */
  @Test public void testLimitOnQueryableTable() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select * from \"days\"\n"
            + "limit 2")
        .returns(
            "day=1; week_day=Sunday\n"
            + "day=2; week_day=Monday\n");
  }

  /** Limit implemented using {@link Queryable#take}. Test case for
   * <a href="https://github.com/julianhyde/optiq/issues/70">issue #70</a>. */
  @Test public void testSelfJoinCount() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.JDBC_FOODMART)
        .query(
            "select count(*) as c from \"foodmart\".\"sales_fact_1997\" as p1 join \"foodmart\".\"sales_fact_1997\" as p2 using (\"store_id\")")
        .returns(
            "C=749681031\n")
        .explainContains(
            "EnumerableAggregateRel(group=[{}], C=[COUNT()])\n"
            + "  EnumerableCalcRel(expr#0..1=[{inputs}], expr#2=[0], DUMMY=[$t2])\n"
            + "    EnumerableJoinRel(condition=[=($0, $1)], joinType=[inner])\n"
            + "      JdbcToEnumerableConverter\n"
            + "        JdbcProjectRel(store_id=[$4])\n"
            + "          JdbcTableScan(table=[[foodmart, sales_fact_1997]])\n"
            + "      JdbcToEnumerableConverter\n"
            + "        JdbcProjectRel(store_id=[$4])\n"
            + "          JdbcTableScan(table=[[foodmart, sales_fact_1997]])\n");
  }

  /** Tests composite GROUP BY where one of the columns has NULL values. */
  @Test public void testGroupByNull() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select \"deptno\", \"commission\", sum(\"salary\") s\n"
            + "from \"hr\".\"emps\"\n"
            + "group by \"deptno\", \"commission\"")
        .returnsUnordered(
            "deptno=10; commission=null; S=7000.0",
            "deptno=20; commission=500; S=8000.0",
            "deptno=10; commission=1000; S=10000.0",
            "deptno=10; commission=250; S=11500.0");
  }

  @Test public void testSelectDistinct() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select distinct \"deptno\"\n"
            + "from \"hr\".\"emps\"\n")
        .returnsUnordered(
            "deptno=10",
            "deptno=20")
        .planContains(".distinct(");
  }

  /** Select distinct on composite key, one column of which is boolean to
   * boot. */
  @Test public void testSelectDistinctComposite() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query("select distinct \"empid\" > 140 as c, \"deptno\"\n"
            + "from \"hr\".\"emps\"\n")
        .returnsUnordered(
            "C=false; deptno=10",
            "C=false; deptno=10",
            "C=true; deptno=10",
            "C=true; deptno=20")
        .planContains(".distinct(");
  }

  /** Same result (and plan) as {@link #testSelectDistinct}. */
  @Test public void testGroupByNoAggregates() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select \"deptno\"\n"
            + "from \"hr\".\"emps\"\n"
            + "group by \"deptno\"")
        .returnsUnordered(
            "deptno=10",
            "deptno=20")
        .planContains(".distinct(");
  }

  /** Tests that SUM and AVG over empty set return null. COUNT returns 0. */
  @Test public void testAggregateEmpty() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select\n"
            + " count(*) as cs,\n"
            + " count(\"deptno\") as c,\n"
            + " sum(\"deptno\") as s,\n"
            + " avg(\"deptno\") as a\n"
            + "from \"hr\".\"emps\"\n"
            + "where \"deptno\" < 0")
        .returns("CS=0; C=0; S=null; A=null\n");
  }

  /** Tests sorting by a column that is already sorted. */
  @Test public void testOrderByOnSortedTable() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select * from \"time_by_day\"\n"
            + "order by \"time_id\"")
        .explainContains(
            "PLAN=EnumerableSortRel(sort0=[$0], dir0=[ASC])\n"
            + "  EnumerableTableAccessRel(table=[[foodmart2, time_by_day]])\n\n");
  }

  /** Tests sorting by a column that is already sorted. */
  @Ignore("fix output for timezone")
  @Test public void testOrderByOnSortedTable2() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"time_id\", \"the_date\" from \"time_by_day\"\n"
            + "where \"time_id\" < 370\n"
            + "order by \"time_id\"")
        .returns(
            "time_id=367; the_date=1997-01-01 00:00:00.0\n"
            + "time_id=368; the_date=1997-01-02 00:00:00.0\n"
            + "time_id=369; the_date=1997-01-03 00:00:00.0\n")
        .explainContains(
            "PLAN=EnumerableSortRel(sort0=[$0], dir0=[Ascending])\n"
            + "  EnumerableCalcRel(expr#0..9=[{inputs}], expr#10=[370], expr#11=[<($t0, $t10)], proj#0..1=[{exprs}], $condition=[$t11])\n"
            + "    EnumerableTableAccessRel(table=[[foodmart2, time_by_day]])\n\n");
  }

  @Test public void testWithInsideWhereExists() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query("select \"deptno\" from \"hr\".\"emps\"\n"
        + "where exists (\n"
        + "  with dept2 as (select * from \"hr\".\"depts\" where \"depts\".\"deptno\" >= \"emps\".\"deptno\")\n"
        + "  select 1 from dept2 where \"deptno\" <= \"emps\".\"deptno\")")
        .returnsUnordered("deptno=10",
            "deptno=10",
            "deptno=10");
  }

  /** Tests windowed aggregation. */
  @Test public void testWinAgg() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select sum(\"salary\" + \"empid\") over w as s,\n"
            + " 5 as five,\n"
            + " min(\"salary\") over w as m,\n"
            + " count(*) over w as c,\n"
            + " \"deptno\",\n"
            + " \"empid\"\n"
            + "from \"hr\".\"emps\"\n"
            + "window w as (partition by \"deptno\" order by \"empid\" rows 1 preceding)")
        .typeIs(
            "[S REAL, FIVE INTEGER NOT NULL, M REAL, C BIGINT, deptno INTEGER NOT NULL, empid INTEGER NOT NULL]")
        .explainContains(
            "EnumerableCalcRel(expr#0..7=[{inputs}], expr#8=[0], expr#9=[>($t4, $t8)], expr#10=[null], expr#11=[CASE($t9, $t5, $t10)], expr#12=[CAST($t11):JavaType(class java.lang.Float)], expr#13=[5], expr#14=[CAST($t6):JavaType(class java.lang.Float)], expr#15=[CAST($t7):BIGINT], S=[$t12], FIVE=[$t13], M=[$t14], C=[$t15], deptno=[$t1], empid=[$t0])\n"
            + "  EnumerableWindowRel(window#0=[window(partition {1} order by [0] rows between 1 PRECEDING and CURRENT ROW aggs [COUNT($3), $SUM0($3), MIN($2), COUNT()])])\n"
            + "    EnumerableCalcRel(expr#0..4=[{inputs}], expr#5=[+($t3, $t0)], proj#0..1=[{exprs}], salary=[$t3], $3=[$t5])\n"
            + "      EnumerableTableAccessRel(table=[[hr, emps]])\n")
        .returns(
            "S=8200.0; FIVE=5; M=8000.0; C=1; deptno=20; empid=200\n"
            + "S=10100.0; FIVE=5; M=10000.0; C=1; deptno=10; empid=100\n"
            + "S=23220.0; FIVE=5; M=11500.0; C=2; deptno=10; empid=110\n"
            + "S=14300.0; FIVE=5; M=7000.0; C=2; deptno=10; empid=150\n");
  }

  /** Tests windowed aggregation with multiple windows.
   * One window straddles the current row.
   * Some windows have no PARTITION BY clause. */
  @Test public void testWinAgg2() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select sum(\"salary\" + \"empid\") over w as s,\n"
            + " 5 as five,\n"
            + " min(\"salary\") over w as m,\n"
            + " count(*) over w as c,\n"
            + " count(*) over w2 as c2,\n"
            + " count(*) over w11 as c11,\n"
            + " count(*) over w11dept as c11dept,\n"
            + " \"deptno\",\n"
            + " \"empid\"\n"
            + "from \"hr\".\"emps\"\n"
            + "window w as (order by \"empid\" rows 1 preceding),\n"
            + " w2 as (order by \"empid\" rows 2 preceding),\n"
            + " w11 as (order by \"empid\" rows between 1 preceding and 1 following),\n"
            + " w11dept as (partition by \"deptno\" order by \"empid\" rows between 1 preceding and 1 following)")
        .typeIs(
            "[S REAL, FIVE INTEGER NOT NULL, M REAL, C BIGINT, C2 BIGINT, C11 BIGINT, C11DEPT BIGINT, deptno INTEGER NOT NULL, empid INTEGER NOT NULL]")
        // Check that optimizes for window whose PARTITION KEY is empty
        .planContains("tempList.size()")
        .returns(
            "S=16400.0; FIVE=5; M=8000.0; C=2; C2=3; C11=2; C11DEPT=1; deptno=20; empid=200\n"
            + "S=10100.0; FIVE=5; M=10000.0; C=1; C2=1; C11=2; C11DEPT=2; deptno=10; empid=100\n"
            + "S=23220.0; FIVE=5; M=11500.0; C=2; C2=2; C11=3; C11DEPT=3; deptno=10; empid=110\n"
            + "S=14300.0; FIVE=5; M=7000.0; C=2; C2=3; C11=3; C11DEPT=2; deptno=10; empid=150\n");
  }

  /** Tests for RANK and ORDER BY ... DESCENDING, NULLS FIRST, NULLS LAST. */
  @Test public void testWinAggRank() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select  \"deptno\",\n"
            + " \"empid\",\n"
            + " \"commission\",\n"
            + " rank() over (partition by \"deptno\" order by \"commission\" desc nulls first) as rcnf,\n"
            + " rank() over (partition by \"deptno\" order by \"commission\" desc nulls last) as rcnl,\n"
            + " rank() over (partition by \"deptno\" order by \"empid\") as r,\n"
            + " rank() over (partition by \"deptno\" order by \"empid\" desc) as rd\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, commission INTEGER, RCNF INTEGER, RCNL INTEGER, R INTEGER, RD INTEGER]")
        .returns(
            "deptno=20; empid=200; commission=500; RCNF=1; RCNL=1; R=1; RD=1\n"
            + "deptno=10; empid=150; commission=null; RCNF=1; RCNL=3; R=3; RD=1\n"
            + "deptno=10; empid=110; commission=250; RCNF=3; RCNL=2; R=2; RD=2\n"
            + "deptno=10; empid=100; commission=1000; RCNF=2; RCNL=1; R=1; RD=3\n");
  }

  /** Tests WHERE comparing a nullable integer with an integer literal. */
  @Test public void testWhereNullable() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select * from \"hr\".\"emps\"\n"
            + "where \"commission\" > 800")
        .returns(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n");
  }

  /** Tests the LIKE operator. */
  @Test public void testLike() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select * from \"hr\".\"emps\"\n"
            + "where \"name\" like '%i__'")
        .returns(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n");
  }

  /** Tests array index. */
  @Test public void testArrayIndexing() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select \"deptno\", \"employees\"[1] as e from \"hr\".\"depts\"\n")
        .returns(
            "deptno=10; E=Employee [empid: 100, deptno: 10, name: Bill]\n"
            + "deptno=30; E=null\n"
            + "deptno=40; E=Employee [empid: 200, deptno: 20, name: Eric]\n");
  }

  @Test public void testVarcharEquals() {
    OptiqAssert.that()
        .withModel(FOODMART_MODEL)
        .query("select \"lname\" from \"customer\" where \"lname\" = 'Nowmer'")
        .returns("lname=Nowmer\n");

    // lname is declared as VARCHAR(30), comparing it with a string longer
    // than 30 characters would introduce a cast to the least restrictive
    // type, thus lname would be cast to a varchar(40) in this case.
    // These sorts of casts are removed though when constructing the jdbc
    // sql, since e.g. HSQLDB does not support them.
    OptiqAssert.that()
        .withModel(FOODMART_MODEL)
        .query("select count(*) as c from \"customer\" "
            + "where \"lname\" = 'this string is longer than 30 characters'")
        .returns("C=0\n");

    OptiqAssert.that()
        .withModel(FOODMART_MODEL)
        .query("select count(*) as c from \"customer\" "
            + "where cast(\"customer_id\" as char(20)) = 'this string is longer than 30 characters'")
        .returns("C=0\n");
  }

  /** Tests the NOT IN operator. Problems arose in code-generation because
   * the column allows nulls. */
  @Test public void testNotIn() {
    predicate("\"name\" not in ('a', 'b') or \"name\" is null")
        .returns(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n");

    // And some similar combinations...
    predicate("\"name\" in ('a', 'b') or \"name\" is null");
    predicate("\"name\" in ('a', 'b', null) or \"name\" is null");
    predicate("\"name\" in ('a', 'b') or \"name\" is not null");
    predicate("\"name\" in ('a', 'b', null) or \"name\" is not null");
    predicate("\"name\" not in ('a', 'b', null) or \"name\" is not null");
    predicate("\"name\" not in ('a', 'b', null) and \"name\" is not null");
  }

  @Test public void testTrim() {
    OptiqAssert.that()
        .withModel(FOODMART_MODEL)
        .query("select trim(\"lname\") as \"lname\" "
            + "from \"customer\" where \"lname\" = 'Nowmer'")
        .returns("lname=Nowmer\n");

    OptiqAssert.that()
        .withModel(FOODMART_MODEL)
        .query("select trim(leading 'N' from \"lname\") as \"lname\" "
            + "from \"customer\" where \"lname\" = 'Nowmer'")
        .returns("lname=owmer\n");
  }

  private OptiqAssert.AssertQuery predicate(String foo) {
    return OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select * from \"hr\".\"emps\"\n"
            + "where " + foo)
        .runs();
  }

  @Test public void testExistsCorrelated() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select*from \"hr\".\"emps\" where exists (\n"
            + " select 1 from \"hr\".\"depts\"\n"
            + " where \"emps\".\"deptno\"=\"depts\".\"deptno\")")
        .returnsUnordered(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000",
            "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null",
            "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250");
  }

  /** Tests a correlated scalar sub-query in the SELECT clause.
   *
   * <p>Note that there should be an extra row "empid=200; deptno=20;
   * DNAME=null" but left join doesn't work.</p> */
  @Test public void testScalarSubQuery() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select \"empid\", \"deptno\",\n"
            + " (select \"name\" from \"hr\".\"depts\"\n"
            + "  where \"deptno\" = e.\"deptno\") as dname\n"
            + "from \"hr\".\"emps\" as e")
        .returnsUnordered("empid=100; deptno=10; DNAME=Sales",
            "empid=110; deptno=10; DNAME=Sales",
            "empid=150; deptno=10; DNAME=Sales",
            "empid=200; deptno=20; DNAME=null");
  }

  @Test public void testLeftJoin() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select e.\"deptno\", d.\"deptno\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "  left join \"hr\".\"depts\" as d using (\"deptno\")")
        .returnsUnordered(
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=20; deptno=null");
  }

  @Test public void testFullJoin() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select e.\"deptno\", d.\"deptno\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "  full join \"hr\".\"depts\" as d using (\"deptno\")")
        .returnsUnordered(
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=20; deptno=null",
            "deptno=null; deptno=30",
            "deptno=null; deptno=40");
  }

  @Test public void testRightJoin() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select e.\"deptno\", d.\"deptno\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "  right join \"hr\".\"depts\" as d using (\"deptno\")")
        .returnsUnordered(
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=null; deptno=30",
            "deptno=null; deptno=40");
  }

  @Test public void testScalarSubQueryUncorrelated() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select \"empid\", \"deptno\",\n"
            + " (select \"name\" from \"hr\".\"depts\"\n"
            + "  where \"deptno\" = 30) as dname\n"
            + "from \"hr\".\"emps\" as e")
        .returnsUnordered("empid=100; deptno=10; DNAME=Marketing",
            "empid=110; deptno=10; DNAME=Marketing",
            "empid=150; deptno=10; DNAME=Marketing",
            "empid=200; deptno=20; DNAME=Marketing");
  }

  @Test public void testScalarSubQueryInCase() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select e.\"name\",\n"
            + " (CASE e.\"deptno\"\n"
            + "  WHEN (Select \"deptno\" from \"hr\".\"depts\" d\n"
            + "        where d.\"deptno\" = e.\"deptno\")\n"
            + "  THEN (Select d.\"name\" from \"hr\".\"depts\" d\n"
            + "        where d.\"deptno\" = e.\"deptno\")\n"
            + "  ELSE 'DepartmentNotFound'  END ) AS DEPTNAME\n"
            + "from \"hr\".\"emps\" e")
        .returnsUnordered("name=Bill; DEPTNAME=Sales",
            "name=Eric; DEPTNAME=DepartmentNotFound",
            "name=Sebastian; DEPTNAME=Sales",
            "name=Theodore; DEPTNAME=Sales");
  }

  @Test public void testScalarSubQueryInCase2() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select e.\"name\",\n"
            + " (CASE WHEN e.\"deptno\" = (\n"
            + "    Select \"deptno\" from \"hr\".\"depts\" d\n"
            + "    where d.\"name\" = 'Sales')\n"
            + "  THEN 'Sales'\n"
            + "  ELSE 'Not Matched'  END ) AS DEPTNAME\n"
            + "from \"hr\".\"emps\" e")
        .returnsUnordered("name=Bill; DEPTNAME=Sales      ",
            "name=Eric; DEPTNAME=Not Matched",
            "name=Sebastian; DEPTNAME=Sales      ",
            "name=Theodore; DEPTNAME=Sales      ");
  }

  /** Tests the TABLES table in the information schema. */
  @Test public void testMetaTables() {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR_PLUS_METADATA)
        .query("select * from \"metadata\".TABLES")
        .returns(
            OptiqAssert.checkResultContains(
                "tableSchem=metadata; tableName=COLUMNS; tableType=SYSTEM_TABLE; "));

    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR_PLUS_METADATA)
        .query(
            "select count(distinct \"tableSchem\") as c\n"
            + "from \"metadata\".TABLES")
        .returns("C=3\n");
  }

  /** Tests that {@link java.sql.Statement#setMaxRows(int)} is honored. */
  @Test public void testSetMaxRows() throws Exception {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .doWithConnection(
            new Function1<OptiqConnection, Object>() {
              public Object apply(OptiqConnection a0) {
                try {
                  final Statement statement = a0.createStatement();
                  try {
                    statement.setMaxRows(-1);
                    fail("expected error");
                  } catch (SQLException e) {
                    assertEquals(e.getMessage(), "illegal maxRows value: -1");
                  }
                  statement.setMaxRows(2);
                  assertEquals(2, statement.getMaxRows());
                  final ResultSet resultSet = statement.executeQuery(
                      "select * from \"hr\".\"emps\"");
                  assertTrue(resultSet.next());
                  assertTrue(resultSet.next());
                  assertFalse(resultSet.next());
                  resultSet.close();
                  statement.close();
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }

  /** Tests a {@link PreparedStatement} with parameters. */
  @Test public void testPreparedStatement() throws Exception {
    OptiqAssert.that()
        .with(OptiqAssert.Config.REGULAR)
        .doWithConnection(
            new Function1<OptiqConnection, Object>() {
              public Object apply(OptiqConnection connection) {
                try {
                  final PreparedStatement preparedStatement =
                      connection.prepareStatement(
                          "select \"deptno\", \"name\" from \"hr\".\"emps\"\n"
                          + "where \"deptno\" < ? and \"name\" like ?");

                  // execute with both vars null - no results
                  ResultSet resultSet = preparedStatement.executeQuery();
                  assertFalse(resultSet.next());

                  // execute with ?0=15, ?1='%' - 3 rows
                  preparedStatement.setInt(1, 15);
                  preparedStatement.setString(2, "%");
                  resultSet = preparedStatement.executeQuery();
                  assertEquals(
                      "deptno=10; name=Bill\n"
                      + "deptno=10; name=Sebastian\n"
                      + "deptno=10; name=Theodore\n",
                      OptiqAssert.toString(resultSet));

                  // execute with ?0=15 (from last bind), ?1='%r%' - 1 row
                  preparedStatement.setString(2, "%r%");
                  resultSet = preparedStatement.executeQuery();
                  assertEquals(
                      "deptno=10; name=Theodore\n",
                      OptiqAssert.toString(resultSet));

                  resultSet.close();
                  preparedStatement.close();
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }

  /** Tests a JDBC connection that provides a model (a single schema based on
   * a JDBC database). */
  @Test public void testModel() {
    OptiqAssert.that()
        .withModel(FOODMART_MODEL)
        .query("select count(*) as c from \"foodmart\".\"time_by_day\"")
        .returns("C=730\n");
  }

  /** Tests a JSON model with a comment. Not standard JSON, but harmless to
   * allow Jackson's comments extension.
   *
   * <p>Test case for <a href="https://github.com/julianhyde/optiq/issues/160">
   *   optiq-160, "Allow comments in schema definitions"</a>.</p> */
  @Test public void testModelWithComment() {
    final String model =
        FOODMART_MODEL.replace("schemas:", "/* comment */ schemas:");
    assertThat(model, not(equalTo(FOODMART_MODEL)));
    OptiqAssert.that()
        .withModel(model)
        .query("select count(*) as c from \"foodmart\".\"time_by_day\"")
        .returns("C=730\n");
  }

  /** Defines a materialized view and tests that the query is rewritten to use
   * it, and that the query produces the same result with and without it. There
   * are more comprehensive tests in {@link MaterializationTest}. */
  @Ignore("until JdbcSchema can define materialized views")
  @Test public void testModelWithMaterializedView() {
    OptiqAssert.that()
        .withModel(FOODMART_MODEL)
        .enable(false)
        .query(
            "select count(*) as c from \"foodmart\".\"sales_fact_1997\" join \"foodmart\".\"time_by_day\" using (\"time_id\")")
        .returns("C=86837\n");
    OptiqAssert.that().withMaterializations(
        FOODMART_MODEL,
        "agg_c_10_sales_fact_1997",
            "select t.`month_of_year`, t.`quarter`, t.`the_year`, sum(s.`store_sales`) as `store_sales`, sum(s.`store_cost`), sum(s.`unit_sales`), count(distinct s.`customer_id`), count(*) as `fact_count` from `time_by_day` as t join `sales_fact_1997` as s using (`time_id`) group by t.`month_of_year`, t.`quarter`, t.`the_year`")
        .query(
            "select t.\"month_of_year\", t.\"quarter\", t.\"the_year\", sum(s.\"store_sales\") as \"store_sales\", sum(s.\"store_cost\"), sum(s.\"unit_sales\"), count(distinct s.\"customer_id\"), count(*) as \"fact_count\" from \"time_by_day\" as t join \"sales_fact_1997\" as s using (\"time_id\") group by t.\"month_of_year\", t.\"quarter\", t.\"the_year\"")
        .explainContains(
            "JdbcTableScan(table=[[foodmart, agg_c_10_sales_fact_1997]])")
        .enableMaterializations(false)
        .explainContains("JdbcTableScan(table=[[foodmart, sales_fact_1997]])")
        .sameResultWithMaterializationsDisabled();
  }

  /** Tests a JDBC connection that provides a model that contains custom
   * tables. */
  @Test public void testModelCustomTable() {
    OptiqAssert.that()
        .withModel(
            "{\n"
            + "  version: '1.0',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       name: 'adhoc',\n"
            + "       tables: [\n"
            + "         {\n"
            + "           name: 'EMPLOYEES',\n"
            + "           type: 'custom',\n"
            + "           factory: '"
            + EmpDeptTableFactory.class.getName() + "',\n"
            + "           operand: {'foo': 1, 'bar': [345, 357] }\n"
            + "         }\n"
            + "       ]\n"
            + "     }\n"
            + "   ]\n"
            + "}")
        .query("select * from \"adhoc\".EMPLOYEES where \"deptno\" = 10")
        .returns(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n");
  }

  /** Tests a JDBC connection that provides a model that contains custom
   * tables. */
  @Test public void testModelCustomTable2() {
    OptiqAssert.that()
        .withModel(
            "{\n"
            + "  version: '1.0',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       name: 'MATH',\n"
            + "       tables: [\n"
            + "         {\n"
            + "           name: 'INTEGERS',\n"
            + "           type: 'custom',\n"
            + "           factory: '"
            + RangeTable.Factory.class.getName() + "',\n"
            + "           operand: {'column': 'N', 'start': 3, 'end': 7 }\n"
            + "         }\n"
            + "       ]\n"
            + "     }\n"
            + "   ]\n"
            + "}")
        .query("select * from math.integers")
        .returns(
            "N=3\n"
            + "N=4\n"
            + "N=5\n"
            + "N=6\n");
  }

  /** Tests a JDBC connection that provides a model that contains a custom
   * schema. */
  @Test public void testModelCustomSchema() throws Exception {
    final OptiqAssert.AssertThat that =
        OptiqAssert.that().withModel(
            "{\n"
            + "  version: '1.0',\n"
            + "  defaultSchema: 'adhoc',\n"
            + "  schemas: [\n"
            + "    {\n"
            + "      name: 'empty'\n"
            + "    },\n"
            + "    {\n"
            + "      name: 'adhoc',\n"
            + "      type: 'custom',\n"
            + "      factory: '"
            + MySchemaFactory.class.getName()
            + "',\n"
            + "      operand: {'tableName': 'ELVIS'}\n"
            + "    }\n"
            + "  ]\n"
            + "}");
    // check that the specified 'defaultSchema' was used
    that.doWithConnection(
        new Function1<OptiqConnection, Object>() {
          public Object apply(OptiqConnection connection) {
            try {
              assertEquals("adhoc", connection.getSchema());
              return null;
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }
        });
    that.query("select * from \"adhoc\".ELVIS where \"deptno\" = 10")
        .returns(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n");
    that.query("select * from \"adhoc\".EMPLOYEES")
        .throws_("Table 'adhoc.EMPLOYEES' not found");
  }

  /** Tests that an immutable schema in a model cannot contain a view. */
  @Test public void testModelImmutableSchemaCannotContainView()
    throws Exception {
    final OptiqAssert.AssertThat that =
        OptiqAssert.that().withModel(
            "{\n"
            + "  version: '1.0',\n"
            + "  defaultSchema: 'adhoc',\n"
            + "  schemas: [\n"
            + "    {\n"
            + "      name: 'empty'\n"
            + "    },\n"
            + "    {\n"
            + "      name: 'adhoc',\n"
            + "      type: 'custom',\n"
            + "      tables: [\n"
            + "        {\n"
            + "          name: 'v',\n"
            + "          type: 'view',\n"
            + "          sql: 'values (1)'\n"
            + "        }\n"
            + "      ],\n"
            + "      factory: '"
            + MySchemaFactory.class.getName()
            + "',\n"
            + "      operand: {\n"
            + "           'tableName': 'ELVIS',\n"
            + "           'mutable': false\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}");
    that.connectThrows(
        "Cannot define view; parent schema 'adhoc' is not mutable");
  }

  private OptiqAssert.AssertThat modelWithView(String view) {
    final Class<EmpDeptTableFactory> clazz = EmpDeptTableFactory.class;
    return OptiqAssert.that().withModel(
        "{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'EMPLOYEES',\n"
        + "           type: 'custom',\n"
        + "           factory: '" + clazz.getName() + "',\n"
        + "           operand: {'foo': true, 'bar': 345}\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'V',\n"
        + "           type: 'view',\n"
        + "           sql: '" + view + "'\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}");
  }

  /** Tests a JDBC connection that provides a model that contains a view. */
  @Test public void testModelView() throws Exception {
    final OptiqAssert.AssertThat with =
        modelWithView("select * from \"EMPLOYEES\" where \"deptno\" = 10");

    with.query("select * from \"adhoc\".V order by \"name\" desc")
        .returns(
            "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n");

    // Make sure that views appear in metadata.
    with.doWithConnection(
        new Function1<OptiqConnection, Void>() {
          public Void apply(OptiqConnection a0) {
            try {
              final DatabaseMetaData metaData = a0.getMetaData();

              // all table types
              assertEquals(
                  "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=EMPLOYEES; TABLE_TYPE=TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; TABLE_TYPE=VIEW; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n",
                  OptiqAssert.toString(
                      metaData.getTables(null, "adhoc", null, null)));

              // views only
              assertEquals(
                  "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; TABLE_TYPE=VIEW; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n",
                  OptiqAssert.toString(
                      metaData.getTables(
                          null, "adhoc", null,
                          new String[] {
                              Schema.TableType.VIEW.name()})));

              // columns
              assertEquals(
                  "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; COLUMN_NAME=empid; DATA_TYPE=4; TYPE_NAME=JavaType(int) NOT NULL; COLUMN_SIZE=-1; BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; NULLABLE=0; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=1; IS_NULLABLE=NO; SCOPE_CATALOG=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=null; IS_GENERATEDCOLUMN=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; COLUMN_NAME=deptno; DATA_TYPE=4; TYPE_NAME=JavaType(int) NOT NULL; COLUMN_SIZE=-1; BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; NULLABLE=0; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=2; IS_NULLABLE=NO; SCOPE_CATALOG=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=null; IS_GENERATEDCOLUMN=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; COLUMN_NAME=name; DATA_TYPE=12; TYPE_NAME=JavaType(class java.lang.String); COLUMN_SIZE=-1; BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; NULLABLE=1; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=3; IS_NULLABLE=YES; SCOPE_CATALOG=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=null; IS_GENERATEDCOLUMN=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; COLUMN_NAME=salary; DATA_TYPE=7; TYPE_NAME=JavaType(float) NOT NULL; COLUMN_SIZE=-1; BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; NULLABLE=0; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=4; IS_NULLABLE=NO; SCOPE_CATALOG=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=null; IS_GENERATEDCOLUMN=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; COLUMN_NAME=commission; DATA_TYPE=4; TYPE_NAME=JavaType(class java.lang.Integer); COLUMN_SIZE=-1; BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; NULLABLE=1; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=5; IS_NULLABLE=YES; SCOPE_CATALOG=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=null; IS_GENERATEDCOLUMN=null\n",
                  OptiqAssert.toString(
                      metaData.getColumns(
                          null, "adhoc", "V", null)));

              // catalog
              assertEquals(
                  "TABLE_CATALOG=null\n",
                  OptiqAssert.toString(
                      metaData.getCatalogs()));

              // schemas
              assertEquals(
                  "TABLE_SCHEM=adhoc; TABLE_CATALOG=null\n"
                  + "TABLE_SCHEM=metadata; TABLE_CATALOG=null\n",
                  OptiqAssert.toString(
                      metaData.getSchemas()));

              // schemas (qualified)
              assertEquals(
                  "TABLE_SCHEM=adhoc; TABLE_CATALOG=null\n",
                  OptiqAssert.toString(
                      metaData.getSchemas(null, "adhoc")));

              // table types
              assertEquals(
                  "TABLE_TYPE=TABLE\n"
                  + "TABLE_TYPE=VIEW\n",
                  OptiqAssert.toString(
                      metaData.getTableTypes()));

              return null;
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }
        });
  }

  /** Tests a view with ORDER BY and LIMIT clauses. */
  @Test public void testOrderByView() throws Exception {
    final OptiqAssert.AssertThat with =
        modelWithView(
            "select * from \"EMPLOYEES\" where \"deptno\" = 10 "
            + "order by \"empid\" limit 2");
    with
        .query("select \"name\" from \"adhoc\".V order by \"name\"")
        .returns("name=Bill\n"
            + "name=Theodore\n");

    // Now a sub-query with ORDER BY and LIMIT clauses. (Same net effect, but
    // ORDER BY and LIMIT in sub-query were not standard SQL until SQL:2008.)
    with
        .query("select \"name\" from (\n"
            + "select * from \"adhoc\".\"EMPLOYEES\" where \"deptno\" = 10\n"
            + "order by \"empid\" limit 2)\n"
            + "order by \"name\"")
        .returns("name=Bill\n"
            + "name=Theodore\n");
  }

  /** Tests saving query results into temporary tables, per
   * {@link net.hydromatic.avatica.Handler.ResultSink}. */
  @Test public void testAutomaticTemporaryTable() throws Exception {
    final List<Object> objects = new ArrayList<Object>();
    OptiqAssert.that()
        .with(
            new OptiqAssert.ConnectionFactory() {
              public OptiqConnection createConnection() throws Exception {
                OptiqConnection connection = (OptiqConnection)
                    new AutoTempDriver(objects)
                        .connect("jdbc:optiq:", new Properties());
                final SchemaPlus rootSchema = connection.getRootSchema();
                rootSchema.add("hr",
                    new ReflectiveSchema(new HrSchema()));
                connection.setSchema("hr");
                return connection;
              }
            })
        .doWithConnection(
            new Function1<OptiqConnection, Object>() {
              public Object apply(OptiqConnection a0) {
                try {
                  a0.createStatement()
                      .executeQuery(
                          "select * from \"hr\".\"emps\" "
                          + "where \"deptno\" = 10");
                  assertEquals(1, objects.size());
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }

  private OptiqAssert.AssertThat withUdf() {
    return OptiqAssert.that()
        .withModel(
            "{\n"
            + "  version: '1.0',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       name: 'adhoc',\n"
            + "       tables: [\n"
            + "         {\n"
            + "           name: 'EMPLOYEES',\n"
            + "           type: 'custom',\n"
            + "           factory: '"
            + EmpDeptTableFactory.class.getName()
            + "',\n"
            + "           operand: {'foo': true, 'bar': 345}\n"
            + "         }\n"
            + "       ],\n"
            + "       functions: [\n"
            + "         {\n"
            + "           name: 'MY_PLUS',\n"
            + "           className: '"
            + MyPlusFunction.class.getName()
            + "'\n"
            + "         },\n"
            + "         {\n"
            + "           name: 'MY_STR',\n"
            + "           className: '"
            + MyToStringFunction.class.getName()
            + "'\n"
            + "         },\n"
            + "         {\n"
            + "           name: 'MY_DOUBLE',\n"
            + "           className: '"
            + MyDoubleFunction.class.getName()
            + "'\n"
            + "         }\n"
            + "       ]\n"
            + "     }\n"
            + "   ]\n"
            + "}");
  }

  /** Tests user-defined function. */
  @Test public void testUserDefinedFunction() throws Exception {
    final OptiqAssert.AssertThat with = withUdf();
    with.query(
        "select \"adhoc\".my_plus(\"deptno\", 100) as p from \"adhoc\".EMPLOYEES")
        .returns(
            "P=110\n"
            + "P=120\n"
            + "P=110\n"
            + "P=110\n");
    with.query(
        "select \"adhoc\".my_double(\"deptno\") as p from \"adhoc\".EMPLOYEES")
        .returns(
            "P=20\n"
            + "P=40\n"
            + "P=20\n"
            + "P=20\n");
  }

  /**
   * Tests that IS NULL/IS NOT NULL is properly implemented for non-strict
   * functions.
   */
  @Test public void testNotNullImplementor() {
    final OptiqAssert.AssertThat with = withUdf();
    with.query(
        "select upper(\"adhoc\".my_str(\"name\")) as p from \"adhoc\".EMPLOYEES")
        .returns(
            "P=<BILL>\n"
            + "P=<ERIC>\n"
            + "P=<SEBASTIAN>\n"
            + "P=<THEODORE>\n");
    with.query(
        "select \"name\" as p from \"adhoc\".EMPLOYEES\n"
        + "where \"adhoc\".my_str(\"name\") is not null")
        .returns(
            "P=Bill\n"
            + "P=Eric\n"
            + "P=Sebastian\n"
            + "P=Theodore\n");
    with.query(
        "select \"name\" as p from \"adhoc\".EMPLOYEES\n"
        + "where \"adhoc\".my_str(upper(\"name\")) is not null")
        .returns(
            "P=Bill\n"
            + "P=Eric\n"
            + "P=Sebastian\n"
            + "P=Theodore\n");
    with.query(
        "select \"name\" as p from \"adhoc\".EMPLOYEES\n"
        + "where upper(\"adhoc\".my_str(\"name\")) is not null")
        .returns(
            "P=Bill\n"
            + "P=Eric\n"
            + "P=Sebastian\n"
            + "P=Theodore\n");
    with.query(
        "select \"name\" as p from \"adhoc\".EMPLOYEES\n"
        + "where \"adhoc\".my_str(\"name\") is null")
        .returns(
            "");
    with.query(
        "select \"name\" as p from \"adhoc\".EMPLOYEES\n"
        + "where \"adhoc\".my_str(upper(\"adhoc\".my_str(\"name\")"
        + ")) ='8'")
        .returns(
            "");
  }

  /** Tests derived return type of user-defined function. */
  @Test public void testUdfDerivedReturnType() {
    final OptiqAssert.AssertThat with = withUdf();
    with.query(
        "select max(\"adhoc\".my_double(\"deptno\")) as p from \"adhoc\".EMPLOYEES")
        .returns(
            "P=40\n");
    with.query(
        "select max(\"adhoc\".my_str(\"name\")) as p from \"adhoc\".EMPLOYEES\n"
        + "where \"adhoc\".my_str(\"name\") is null")
        .returns(
            "P=null\n");
  }

  /** Test for {@link EigenbaseNewResource#requireDefaultConstructor(String)}. */
  @Test public void testUserDefinedFunction2() throws Exception {
    withBadUdf(AwkwardFunction.class)
        .connectThrows(
            "Declaring class 'net.hydromatic.optiq.test.JdbcTest$AwkwardFunction' of non-static user-defined function must have a public constructor with zero parameters");
  }

  /** Tests user-defined aggregate function. */
  @Test public void testUserDefinedAggregateFunction() throws Exception {
    final String empDept = EmpDeptTableFactory.class.getName();
    final String sum = MyStaticSumFunction.class.getName();
    final String sum2 = MySumFunction.class.getName();
    final OptiqAssert.AssertThat with = OptiqAssert.that()
        .withModel(
            "{\n"
            + "  version: '1.0',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       name: 'adhoc',\n"
            + "       tables: [\n"
            + "         {\n"
            + "           name: 'EMPLOYEES',\n"
            + "           type: 'custom',\n"
            + "           factory: '" + empDept + "',\n"
            + "           operand: {'foo': true, 'bar': 345}\n"
            + "         }\n"
            + "       ],\n"
            + "       functions: [\n"
            + "         {\n"
            + "           name: 'MY_SUM',\n"
            + "           className: '" + sum + "'\n"
            + "         },\n"
            + "         {\n"
            + "           name: 'MY_SUM2',\n"
            + "           className: '" + sum2 + "'\n"
            + "         }\n"
            + "       ]\n"
            + "     }\n"
            + "   ]\n"
            + "}")
        .withSchema("adhoc");
    with.withSchema(null)
        .query(
            "select \"adhoc\".my_sum(\"deptno\") as p from \"adhoc\".EMPLOYEES\n")
        .returns("P=50\n");
    with.query("select my_sum(\"empid\"), \"deptno\" as p from EMPLOYEES\n")
        .throws_(
            "Expression 'deptno' is not being grouped");
    with.query("select my_sum(\"deptno\") as p from EMPLOYEES\n")
        .returns("P=50\n");
    with.query("select my_sum(\"name\") as p from EMPLOYEES\n")
        .throws_(
            "Cannot apply 'MY_SUM' to arguments of type 'MY_SUM(<JAVATYPE(CLASS JAVA.LANG.STRING)>)'. Supported form(s): 'MY_SUM(<NUMERIC>)");
    with.query("select my_sum(\"deptno\", 1) as p from EMPLOYEES\n")
        .throws_(
            "No match found for function signature MY_SUM(<NUMERIC>, <NUMERIC>)");
    with.query("select my_sum() as p from EMPLOYEES\n")
        .throws_(
            "No match found for function signature MY_SUM()");
    with.query(
        "select \"deptno\", my_sum(\"deptno\") as p from EMPLOYEES\n"
        + "group by \"deptno\"")
        .returnsUnordered(
            "deptno=20; P=20",
            "deptno=10; P=30");
    with.query("select \"deptno\", my_sum2(\"deptno\") as p from EMPLOYEES\n"
        + "group by \"deptno\"")
        .returnsUnordered("deptno=20; P=20", "deptno=10; P=30");
  }

  /** Test for {@link EigenbaseNewResource#initAddWrongParamTypes(String)}. */
  @Test public void testUserDefinedAggregateFunction2() throws Exception {
    Util.discard(EigenbaseNewResource.class);
    withBadUdf(SumFunctionBadInitAdd.class).connectThrows(
        "In user-defined aggregate class 'net.hydromatic.optiq.test.JdbcTest$SumFunctionBadInitAdd', parameter types of 'initAdd' method must be same as value type(s)");
  }

  /** Test for {@link EigenbaseNewResource#firstParameterOfAdd(String)}. */
  @Test public void testUserDefinedAggregateFunction3() throws Exception {
    withBadUdf(SumFunctionBadIAdd.class).connectThrows(
        "Caused by: java.lang.RuntimeException: In user-defined aggregate class 'net.hydromatic.optiq.test.JdbcTest$SumFunctionBadIAdd', first parameter to 'add' method must be the accumulator (the return type of the 'init' method)");
  }

  private static OptiqAssert.AssertThat withBadUdf(Class clazz) {
    final String empDept = EmpDeptTableFactory.class.getName();
    final String className = clazz.getName();
    return OptiqAssert.that()
        .withModel(
            "{\n"
            + "  version: '1.0',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       name: 'adhoc',\n"
            + "       tables: [\n"
            + "         {\n"
            + "           name: 'EMPLOYEES',\n"
            + "           type: 'custom',\n"
            + "           factory: '" + empDept + "',\n"
            + "           operand: {'foo': true, 'bar': 345}\n"
            + "         }\n"
            + "       ],\n"
            + "       functions: [\n"
            + "         {\n"
            + "           name: 'AWKWARD',\n"
            + "           className: '" + className + "'\n"
            + "         }\n"
            + "       ]\n"
            + "     }\n"
            + "   ]\n"
            + "}")
        .withSchema("adhoc");
  }

  /** Tests resolution of functions using schema paths. */
  @Test public void testPath() throws Exception {
    final String name = MyPlusFunction.class.getName();
    final OptiqAssert.AssertThat with = OptiqAssert.that()
        .withModel(
            "{\n"
                + "  version: '1.0',\n"
                + "   schemas: [\n"
                + "     {\n"
                + "       name: 'adhoc',\n"
                + "       functions: [\n"
                + "         {\n"
                + "           name: 'MY_PLUS',\n"
                + "           className: '" + name + "'\n"
                + "         }\n"
                + "       ]\n"
                + "     },\n"
                + "     {\n"
                + "       name: 'adhoc2',\n"
                + "       functions: [\n"
                + "         {\n"
                + "           name: 'MY_PLUS2',\n"
                + "           className: '" + name + "'\n"
                + "         }\n"
                + "       ]\n"
                + "     },\n"
                + "     {\n"
                + "       name: 'adhoc3',\n"
                + "       path: ['adhoc2','adhoc3'],\n"
                + "       functions: [\n"
                + "         {\n"
                + "           name: 'MY_PLUS3',\n"
                + "           className: '" + name + "'\n"
                + "         }\n"
                + "       ]\n"
                + "     }\n"
                + "   ]\n"
                + "}");

    final String err = "No match found for function signature";
    final String res = "EXPR$0=2\n";

    // adhoc can see own function MY_PLUS but not adhoc2.MY_PLUS2 unless
    // qualified
    final OptiqAssert.AssertThat adhoc = with.withSchema("adhoc");
    adhoc.query("values MY_PLUS(1, 1)").returns(res);
    adhoc.query("values MY_PLUS2(1, 1)").throws_(err);
    adhoc.query("values \"adhoc2\".MY_PLUS(1, 1)").throws_(err);
    adhoc.query("values \"adhoc2\".MY_PLUS2(1, 1)").returns(res);

    // adhoc2 can see own function MY_PLUS2 but not adhoc2.MY_PLUS unless
    // qualified
    final OptiqAssert.AssertThat adhoc2 = with.withSchema("adhoc2");
    adhoc2.query("values MY_PLUS2(1, 1)").returns(res);
    adhoc2.query("values MY_PLUS(1, 1)").throws_(err);
    adhoc2.query("values \"adhoc\".MY_PLUS(1, 1)").returns(res);

    // adhoc3 can see own adhoc2.MY_PLUS2 because in path, with or without
    // qualification, but can only see adhoc.MY_PLUS with qualification
    final OptiqAssert.AssertThat adhoc3 = with.withSchema("adhoc3");
    adhoc3.query("values MY_PLUS2(1, 1)").returns(res);
    adhoc3.query("values MY_PLUS(1, 1)").throws_(err);
    adhoc3.query("values \"adhoc\".MY_PLUS(1, 1)").returns(res);
  }

  @Test public void testExplain() {
    final OptiqAssert.AssertThat with =
        OptiqAssert.that().with(OptiqAssert.Config.FOODMART_CLONE);
    with.query("explain plan for values (1, 'ab')")
        .returns("PLAN=EnumerableValuesRel(tuples=[[{ 1, 'ab' }]])\n\n");
    with.query("explain plan with implementation for values (1, 'ab')")
        .returns("PLAN=EnumerableValuesRel(tuples=[[{ 1, 'ab' }]])\n\n");
    with.query("explain plan without implementation for values (1, 'ab')")
        .returns("PLAN=ValuesRel(tuples=[[{ 1, 'ab' }]])\n\n");
    with.query("explain plan with type for values (1, 'ab')")
        .returns(
            "PLAN=EXPR$0 INTEGER NOT NULL,\n"
            + "EXPR$1 CHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL\n");
  }

  /** Test case for bug where if two tables have different element classes
   * but those classes have identical fields, Optiq would generate code to use
   * the wrong element class; a {@link ClassCastException} would ensue. */
  @Test public void testDifferentTypesSameFields() throws Exception {
    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    Connection connection = DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection =
        connection.unwrap(OptiqConnection.class);
    final SchemaPlus rootSchema = optiqConnection.getRootSchema();
    rootSchema.add("TEST", new ReflectiveSchema(new MySchema()));
    Statement statement = optiqConnection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("SELECT \"myvalue\" from TEST.\"mytable2\"");
    assertEquals("myvalue=2\n", OptiqAssert.toString(resultSet));
    resultSet.close();
    statement.close();
    connection.close();
  }

  /** Tests that CURRENT_TIMESTAMP gives different values each time a statement
   * is executed. */
  @Test public void testCurrentTimestamp() throws Exception {
    OptiqAssert.that()
        .with(ImmutableMap.of("timezone", "GMT+1:00"))
        .doWithConnection(
            new Function1<OptiqConnection, Void>() {
              public Void apply(OptiqConnection connection) {
                try {
                  final PreparedStatement statement =
                      connection.prepareStatement("VALUES CURRENT_TIMESTAMP");
                  ResultSet resultSet;

                  resultSet = statement.executeQuery();
                  assertTrue(resultSet.next());
                  String s0 = resultSet.getString(1);
                  assertFalse(resultSet.next());

                  try {
                    Thread.sleep(1000);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }

                  resultSet = statement.executeQuery();
                  assertTrue(resultSet.next());
                  String s1 = resultSet.getString(1);
                  assertFalse(resultSet.next());

                  assertTrue(
                      "\n" + "s0=" + s0 + "\n" + "s1=" + s1 + "\n",
                      s0.compareTo(s1) < 0);
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }

  /** Test for timestamps and time zones, based on pgsql TimezoneTest. */
  @Test public void testGetTimestamp() throws Exception {
    OptiqAssert.that()
        .with(ImmutableMap.of("timezone", "GMT+1:00"))
        .doWithConnection(
            new Function1<OptiqConnection, Void>() {
              public Void apply(OptiqConnection connection) {
                try {
                  checkGetTimestamp(connection);
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }

  private void checkGetTimestamp(Connection con) throws SQLException {
    Statement statement = con.createStatement();

    // Not supported yet. We set timezone using connect-string parameters.
    //statement.executeUpdate("alter session set timezone = 'gmt-3'");

    ResultSet rs = statement.executeQuery(
        "SELECT * FROM (VALUES(\n"
        + " TIMESTAMP '1970-01-01 00:00:00',\n"
        + " /* TIMESTAMP '2005-01-01 15:00:00 +0300', */\n"
        + " TIMESTAMP '2005-01-01 15:00:00',\n"
        + " TIME '15:00:00',\n"
        + " /* TIME '15:00:00 +0300', */\n"
        + " DATE '2005-01-01'\n"
        + ")) AS t(ts0, /* tstz, */ ts, t, /* tz, */ d)");
    assertTrue(rs.next());

    TimeZone tzUtc   = TimeZone.getTimeZone("UTC");    // +0000 always
    TimeZone tzGmt03 = TimeZone.getTimeZone("GMT+03"); // +0300 always
    TimeZone tzGmt05 = TimeZone.getTimeZone("GMT-05"); // -0500 always
    TimeZone tzGmt13 = TimeZone.getTimeZone("GMT+13"); // +1000 always

    Calendar cUtc   = Calendar.getInstance(tzUtc);
    Calendar cGmt03 = Calendar.getInstance(tzGmt03);
    Calendar cGmt05 = Calendar.getInstance(tzGmt05);
    Calendar cGmt13 = Calendar.getInstance(tzGmt13);

    Timestamp ts;
    String s;
    int c = 1;

    // timestamp: 1970-01-01 00:00:00
    ts = rs.getTimestamp(c);                     // Convert timestamp to +0100
    assertEquals(-3600000L,      ts.getTime());  // 1970-01-01 00:00:00 +0100
    ts = rs.getTimestamp(c, cUtc);               // Convert timestamp to UTC
    assertEquals(0L,             ts.getTime());  // 1970-01-01 00:00:00 +0000
    ts = rs.getTimestamp(c, cGmt03);             // Convert timestamp to +0300
    assertEquals(-10800000L,     ts.getTime());  // 1970-01-01 00:00:00 +0300
    ts = rs.getTimestamp(c, cGmt05);             // Convert timestamp to -0500
    assertEquals(18000000L,      ts.getTime());  // 1970-01-01 00:00:00 -0500
    ts = rs.getTimestamp(c, cGmt13);             // Convert timestamp to +1300
    assertEquals(-46800000,      ts.getTime());  // 1970-01-01 00:00:00 +1300
    s = rs.getString(c);
    assertEquals("1970-01-01 00:00:00", s);
    ++c;

    if (false) {
      // timestamptz: 2005-01-01 15:00:00+03
      ts = rs.getTimestamp(c);                      // Represents an instant in
                                                    // time, TZ is irrelevant.
      assertEquals(1104580800000L, ts.getTime());   // 2005-01-01 12:00:00 UTC
      ts = rs.getTimestamp(c, cUtc);                // TZ irrelevant, as above
      assertEquals(1104580800000L, ts.getTime());   // 2005-01-01 12:00:00 UTC
      ts = rs.getTimestamp(c, cGmt03);              // TZ irrelevant, as above
      assertEquals(1104580800000L, ts.getTime());   // 2005-01-01 12:00:00 UTC
      ts = rs.getTimestamp(c, cGmt05);              // TZ irrelevant, as above
      assertEquals(1104580800000L, ts.getTime());   // 2005-01-01 12:00:00 UTC
      ts = rs.getTimestamp(c, cGmt13);              // TZ irrelevant, as above
      assertEquals(1104580800000L, ts.getTime());   // 2005-01-01 12:00:00 UTC
      ++c;
    }

    // timestamp: 2005-01-01 15:00:00
    ts = rs.getTimestamp(c);                     // Convert timestamp to +0100
    assertEquals(1104588000000L, ts.getTime());  // 2005-01-01 15:00:00 +0100
    ts = rs.getTimestamp(c, cUtc);               // Convert timestamp to UTC
    assertEquals(1104591600000L, ts.getTime());  // 2005-01-01 15:00:00 +0000
    ts = rs.getTimestamp(c, cGmt03);             // Convert timestamp to +0300
    assertEquals(1104580800000L, ts.getTime());  // 2005-01-01 15:00:00 +0300
    ts = rs.getTimestamp(c, cGmt05);             // Convert timestamp to -0500
    assertEquals(1104609600000L, ts.getTime());  // 2005-01-01 15:00:00 -0500
    ts = rs.getTimestamp(c, cGmt13);             // Convert timestamp to +1300
    assertEquals(1104544800000L, ts.getTime());  // 2005-01-01 15:00:00 +1300
    s = rs.getString(c);
    assertEquals("2005-01-01 15:00:00", s);
    ++c;

    // time: 15:00:00
    ts = rs.getTimestamp(c);
    assertEquals(50400000L, ts.getTime());        // 1970-01-01 15:00:00 +0100
    ts = rs.getTimestamp(c, cUtc);
    assertEquals(54000000L, ts.getTime());        // 1970-01-01 15:00:00 +0000
    ts = rs.getTimestamp(c, cGmt03);
    assertEquals(43200000L, ts.getTime());        // 1970-01-01 15:00:00 +0300
    ts = rs.getTimestamp(c, cGmt05);
    assertEquals(72000000L, ts.getTime());        // 1970-01-01 15:00:00 -0500
    ts = rs.getTimestamp(c, cGmt13);
    assertEquals(7200000L, ts.getTime());         // 1970-01-01 15:00:00 +1300
    s = rs.getString(c);
    assertEquals("15:00:00", s);
    ++c;

    if (false) {
      // timetz: 15:00:00+03
      ts = rs.getTimestamp(c);
      assertEquals(43200000L, ts.getTime());    // 1970-01-01 15:00:00 +0300 ->
                                                // 1970-01-01 13:00:00 +0100
      ts = rs.getTimestamp(c, cUtc);
      assertEquals(43200000L, ts.getTime());    // 1970-01-01 15:00:00 +0300 ->
                                                // 1970-01-01 12:00:00 +0000
      ts = rs.getTimestamp(c, cGmt03);
      assertEquals(43200000L, ts.getTime());    // 1970-01-01 15:00:00 +0300 ->
                                                // 1970-01-01 15:00:00 +0300
      ts = rs.getTimestamp(c, cGmt05);
      assertEquals(43200000L, ts.getTime());    // 1970-01-01 15:00:00 +0300 ->
                                                // 1970-01-01 07:00:00 -0500
      ts = rs.getTimestamp(c, cGmt13);
      assertEquals(43200000L, ts.getTime());    // 1970-01-01 15:00:00 +0300 ->
                                                // 1970-01-02 01:00:00 +1300
      ++c;
    }

    // date: 2005-01-01
    ts = rs.getTimestamp(c);
    assertEquals(1104534000000L, ts.getTime()); // 2005-01-01 00:00:00 +0100
    ts = rs.getTimestamp(c, cUtc);
    assertEquals(1104537600000L, ts.getTime()); // 2005-01-01 00:00:00 +0000
    ts = rs.getTimestamp(c, cGmt03);
    assertEquals(1104526800000L, ts.getTime()); // 2005-01-01 00:00:00 +0300
    ts = rs.getTimestamp(c, cGmt05);
    assertEquals(1104555600000L, ts.getTime()); // 2005-01-01 00:00:00 -0500
    ts = rs.getTimestamp(c, cGmt13);
    assertEquals(1104490800000L, ts.getTime()); // 2005-01-01 00:00:00 +1300
    s = rs.getString(c);
    assertEquals("2005-01-01", s);              // 2005-01-01 00:00:00 +0100
    ++c;

    assertTrue(!rs.next());
  }

  /** Tests accessing a column in a JDBC source whose type is DATE. */
  @Test
  public void testGetDate() throws Exception {
    OptiqAssert.that()
      .with(OptiqAssert.Config.JDBC_FOODMART)
      .doWithConnection(new Function1<OptiqConnection, Object>() {
        public Object apply(OptiqConnection conn) {
          try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
              "select min(\"date\") mindate from \"foodmart\".\"currency\"");
            assertTrue(rs.next());
            assertEquals(
              Date.valueOf("1997-01-01"),
              rs.getDate(1));
            assertFalse(rs.next());
            return null;
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      });
  }

  /** Tests accessing a date as a string in a JDBC source whose type is DATE. */
  @Test
  public void testGetDateAsString() throws Exception {
    OptiqAssert.that()
      .with(OptiqAssert.Config.JDBC_FOODMART)
      .query("select min(\"date\") mindate from \"foodmart\".\"currency\"")
      .returns("MINDATE=1997-01-01\n");
  }

  @Test
  public void testGetTimestampObject() throws Exception {
    OptiqAssert.that()
      .with(OptiqAssert.Config.JDBC_FOODMART)
      .doWithConnection(new Function1<OptiqConnection, Object>() {
        public Object apply(OptiqConnection conn) {
          try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
              "select \"hire_date\" from \"foodmart\".\"employee\" where \"employee_id\" = 1");
            assertTrue(rs.next());
            assertEquals(
              Timestamp.valueOf("1994-12-01 00:00:00"),
              rs.getTimestamp(1));
            assertFalse(rs.next());
            return null;
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      });
  }

  @Test public void testUnicode() throws Exception {
    OptiqAssert.AssertThat with =
        OptiqAssert.that().with(OptiqAssert.Config.FOODMART_CLONE);

    // Note that \u82f1 in a Java string is a Java unicode escape;
    // But \\82f1 in a SQL string is a SQL unicode escape.
    // various ways to create a unicode string literal
    with.query("values _UTF16'\u82f1\u56fd'")
        .returns("EXPR$0=\u82f1\u56fd\n");
    with.query("values U&'\\82F1\\56FD'")
        .returns("EXPR$0=\u82f1\u56fd\n");
    with.query("values u&'\\82f1\\56fd'")
        .returns("EXPR$0=\u82f1\u56fd\n");
    with.query("values '\u82f1\u56fd'")
        .throws_(
            "Failed to encode '\u82f1\u56fd' in character set 'ISO-8859-1'");

    // comparing a unicode string literal with a regular string literal
    with.query(
        "select * from \"employee\" where \"full_name\" = '\u82f1\u56fd'")
        .throws_(
            "Failed to encode '\u82f1\u56fd' in character set 'ISO-8859-1'");
    with.query(
        "select * from \"employee\" where \"full_name\" = _UTF16'\u82f1\u56fd'")
        .throws_(
            "Cannot apply = to the two different charsets ISO-8859-1 and UTF-16LE");

    // The CONVERT function (what SQL:2011 calls "character transliteration") is
    // not implemented yet. See https://github.com/julianhyde/optiq/issues/111.
    with.query("select * from \"employee\"\n"
        + "where convert(\"full_name\" using UTF16) = _UTF16'\u82f1\u56fd'")
        .throws_("Column 'UTF16' not found in any table");
  }

  /** Tests metadata for the MySQL lexical scheme. */
  @Test public void testLexMySQL() throws Exception {
    OptiqAssert.that()
        .with(ImmutableMap.of("lex", "MYSQL"))
        .doWithConnection(
            new Function1<OptiqConnection, Void>() {
              public Void apply(OptiqConnection connection) {
                try {
                  DatabaseMetaData metaData = connection.getMetaData();
                  assertThat(metaData.getIdentifierQuoteString(), equalTo("`"));
                  assertThat(metaData.supportsMixedCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesMixedCaseIdentifiers(),
                      equalTo(true));
                  assertThat(metaData.storesUpperCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesLowerCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                      equalTo(true));
                  assertThat(metaData.storesUpperCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                      equalTo(false));
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }

  /** Tests metadata for different the "SQL_SERVER" lexical scheme. */
  @Test public void testLexSqlServer() throws Exception {
    OptiqAssert.that()
        .with(ImmutableMap.of("lex", "SQL_SERVER"))
        .doWithConnection(
            new Function1<OptiqConnection, Void>() {
              public Void apply(OptiqConnection connection) {
                try {
                  DatabaseMetaData metaData = connection.getMetaData();
                  assertThat(metaData.getIdentifierQuoteString(), equalTo("["));
                  assertThat(metaData.supportsMixedCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesMixedCaseIdentifiers(),
                      equalTo(true));
                  assertThat(metaData.storesUpperCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesLowerCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                      equalTo(true));
                  assertThat(metaData.storesUpperCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                      equalTo(false));
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }

  /** Tests metadata for the ORACLE (and default) lexical scheme. */
  @Test public void testLexOracle() throws Exception {
    OptiqAssert.that()
        .with(ImmutableMap.of("lex", "ORACLE"))
        .doWithConnection(
            new Function1<OptiqConnection, Void>() {
              public Void apply(OptiqConnection connection) {
                try {
                  DatabaseMetaData metaData = connection.getMetaData();
                  assertThat(metaData.getIdentifierQuoteString(),
                      equalTo("\""));
                  assertThat(metaData.supportsMixedCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesMixedCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesUpperCaseIdentifiers(),
                      equalTo(true));
                  assertThat(metaData.storesLowerCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                      equalTo(true));
                  // Oracle JDBC 12.1.0.1.0 returns true here, however it is
                  // not clear if the bug is in JDBC specification or Oracle
                  // driver
                  assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesUpperCaseQuotedIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                      equalTo(false));
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }

  /** Tests metadata for the JAVA lexical scheme. */
  @Test public void testLexJava() throws Exception {
    OptiqAssert.that()
        .with(ImmutableMap.of("lex", "JAVA"))
        .doWithConnection(
            new Function1<OptiqConnection, Void>() {
              public Void apply(OptiqConnection connection) {
                try {
                  DatabaseMetaData metaData = connection.getMetaData();
                  assertThat(metaData.getIdentifierQuoteString(),
                      equalTo("`"));
                  assertThat(metaData.supportsMixedCaseIdentifiers(),
                      equalTo(true));
                  assertThat(metaData.storesMixedCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesUpperCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesLowerCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                      equalTo(true));
                  assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesUpperCaseQuotedIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                      equalTo(false));
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }

  /** Tests metadata for the ORACLE lexical scheme overriden like JAVA. */
  @Test public void testLexOracleAsJava() throws Exception {
    OptiqAssert.that()
        .with(ImmutableMap.<String, String>builder()
            .put("lex", "ORACLE")
            .put("quoting", "BACK_TICK")
            .put("unquotedCasing", "UNCHANGED")
            .put("quotedCasing", "UNCHANGED")
            .put("caseSensitive", "TRUE")
            .build())
        .doWithConnection(
            new Function1<OptiqConnection, Void>() {
              public Void apply(OptiqConnection connection) {
                try {
                  DatabaseMetaData metaData = connection.getMetaData();
                  assertThat(metaData.getIdentifierQuoteString(),
                      equalTo("`"));
                  assertThat(metaData.supportsMixedCaseIdentifiers(),
                      equalTo(true));
                  assertThat(metaData.storesMixedCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesUpperCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesLowerCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                      equalTo(true));
                  assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesUpperCaseQuotedIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                      equalTo(false));
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }

  /** Tests case-insensitive resolution of schema and table names. */
  @Test public void testLexCaseInsensitive() {
    final OptiqAssert.AssertThat with =
        OptiqAssert.that().with(ImmutableMap.of("lex", "MYSQL"));
    with.query("select COUNT(*) as c from metaData.tAbles")
        .returns("c=2\n");
    with.query("select COUNT(*) as c from `metaData`.`tAbles`")
        .returns("c=2\n");

    // case-sensitive gives error
    final OptiqAssert.AssertThat with2 =
        OptiqAssert.that().with(ImmutableMap.of("lex", "JAVA"));
    with2.query("select COUNT(*) as c from `metaData`.`tAbles`")
        .throws_("Table 'metaData.tAbles' not found");
  }

  /** Tests that {@link Hook#PARSE_TREE} works. */
  @Test public void testHook() {
    final int[] callCount = {0};
    final Hook.Closeable hook = Hook.PARSE_TREE.addThread(
        new Function1<Object, Object>() {
          public Object apply(Object a0) {
            Object[] args = (Object[]) a0;
            assertThat(args.length, equalTo(2));
            assertThat(args[0], instanceOf(String.class));
            assertThat((String) args[0], equalTo(
                "select \"deptno\", \"commission\", sum(\"salary\") s\n"
                + "from \"hr\".\"emps\"\n"
                + "group by \"deptno\", \"commission\""));
            assertThat(args[1], instanceOf(SqlSelect.class));
            ++callCount[0];
            return null;
          }
        });
    try {
      // Simple query does not run the hook.
      testSimple();
      assertThat(callCount[0], equalTo(0));

      // Non-trivial query runs hook once.
      testGroupByNull();
      assertThat(callCount[0], equalTo(1));
    } finally {
      hook.close();
    }
  }

  /** Tests {@link SqlDialect}. */
  @Test public void testDialect() {
    final String[] sqls = {null};
    final Hook.Closeable hook = Hook.QUERY_PLAN.addThread(
        new Function1<Object, Object>() {
          public Object apply(Object a0) {
            String sql = (String) a0;
            sqls[0] = sql;
            return null;
          }
        });
    try {
      OptiqAssert.that()
          .with(OptiqAssert.Config.JDBC_FOODMART)
          .query(
              "select count(*) as c from \"foodmart\".\"employee\" as e1\n"
              + "  where \"first_name\" = 'abcde'\n"
              + "  and \"gender\" = 'F'")
          .returns("C=0\n");
      switch (OptiqAssert.CONNECTION_SPEC) {
      case HSQLDB:
        assertThat(Util.toLinux(sqls[0]), equalTo(
            "SELECT COUNT(*) AS \"C\"\n"
            + "FROM (SELECT 0 AS \"DUMMY\"\n"
            + "FROM \"foodmart\".\"employee\"\n"
            + "WHERE \"first_name\" = 'abcde' AND \"gender\" = 'F') AS \"t0\""));
        break;
      }
    } finally {
      hook.close();
    }
  }

  @Test public void testSchemaCaching() throws Exception {
    final OptiqConnection connection = OptiqAssert.getConnection(false);
    final SchemaPlus rootSchema = connection.getRootSchema();

    // create schema "/a"
    final Map<String, Schema> aSubSchemaMap = new HashMap<String, Schema>();
    final SchemaPlus aSchema = rootSchema.add("a", new AbstractSchema() {
      @Override protected Map<String, Schema> getSubSchemaMap() {
        return aSubSchemaMap;
      }
    });
    aSchema.setCacheEnabled(true);
    assertThat(aSchema.getSubSchemaNames().size(), is(0));

    // AbstractSchema never thinks its contents have changed; subsequent tests
    // assume this
    assertThat(aSchema.contentsHaveChangedSince(-1, 1), equalTo(false));
    assertThat(aSchema.contentsHaveChangedSince(1, 1), equalTo(false));

    // first call, to populate the cache
    assertThat(aSchema.getSubSchemaNames().size(), is(0));

    // create schema "/a/b1". Appears only when we disable caching.
    aSubSchemaMap.put("b1", new AbstractSchema());
    assertThat(aSchema.getSubSchemaNames().size(), is(0));
    assertThat(aSchema.getSubSchema("b1"), nullValue());
    aSchema.setCacheEnabled(false);
    assertThat(aSchema.getSubSchemaNames().size(), is(1));
    assertThat(aSchema.getSubSchema("b1"), notNullValue());

    // create schema "/a/b2". Appears immediately, because caching is disabled.
    aSubSchemaMap.put("b2", new AbstractSchema());
    assertThat(aSchema.getSubSchemaNames().size(), is(2));

    // an explicit sub-schema appears immediately, even if caching is enabled
    aSchema.setCacheEnabled(true);
    assertThat(aSchema.getSubSchemaNames().size(), is(2));
    aSchema.add("b3", new AbstractSchema()); // explicit
    aSubSchemaMap.put("b4", new AbstractSchema()); // implicit
    assertThat(aSchema.getSubSchemaNames().size(), is(3));
    aSchema.setCacheEnabled(false);
    assertThat(aSchema.getSubSchemaNames().size(), is(4));
    for (String name : aSchema.getSubSchemaNames()) {
      assertThat(aSchema.getSubSchema(name), notNullValue());
    }

    // create schema "/a2"
    final Map<String, Schema> a2SubSchemaMap = new HashMap<String, Schema>();
    final boolean[] changed = {false};
    final SchemaPlus a2Schema = rootSchema.add("a", new AbstractSchema() {
      @Override protected Map<String, Schema> getSubSchemaMap() {
        return a2SubSchemaMap;
      }
      @Override
      public boolean contentsHaveChangedSince(long lastCheck, long now) {
        return changed[0];
      }
    });
    a2Schema.setCacheEnabled(true);
    assertThat(a2Schema.getSubSchemaNames().size(), is(0));

    // create schema "/a2/b3". Appears only when we mark the schema changed.
    a2SubSchemaMap.put("b3", new AbstractSchema());
    assertThat(a2Schema.getSubSchemaNames().size(), is(0));
    Thread.sleep(1);
    assertThat(a2Schema.getSubSchemaNames().size(), is(0));
    changed[0] = true;
    assertThat(a2Schema.getSubSchemaNames().size(), is(1));
    changed[0] = false;

    // or if we disable caching
    a2SubSchemaMap.put("b4", new AbstractSchema());
    assertThat(a2Schema.getSubSchemaNames().size(), is(1));
    a2Schema.setCacheEnabled(false);
    a2Schema.setCacheEnabled(true);
    assertThat(a2Schema.getSubSchemaNames().size(), is(2));
    for (String name : aSchema.getSubSchemaNames()) {
      assertThat(aSchema.getSubSchema(name), notNullValue());
    }

    // add tables and retrieve with various case sensitivities
    final TableInRootSchemaTest.SimpleTable table =
        new TableInRootSchemaTest.SimpleTable();
    a2Schema.add("table1", table);
    a2Schema.add("TABLE1", table);
    a2Schema.add("tabLe1", table);
    a2Schema.add("tabLe2", table);
    assertThat(a2Schema.getTableNames().size(), equalTo(4));
    final OptiqSchema a2OptiqSchema = OptiqSchema.from(a2Schema);
    assertThat(a2OptiqSchema.getTable("table1", true), notNullValue());
    assertThat(a2OptiqSchema.getTable("table1", false), notNullValue());
    assertThat(a2OptiqSchema.getTable("taBle1", true), nullValue());
    assertThat(a2OptiqSchema.getTable("taBle1", false), notNullValue());
    final TableMacro function = ViewTable.viewMacro(a2Schema, "values 1", null);

    connection.close();
  }

  // Disable checkstyle, so it doesn't complain about fields like "customer_id".
  //CHECKSTYLE: OFF

  public static class HrSchema {
    @Override
    public String toString() {
      return "HrSchema";
    }

    public final Employee[] emps = {
      new Employee(100, 10, "Bill", 10000, 1000),
      new Employee(200, 20, "Eric", 8000, 500),
      new Employee(150, 10, "Sebastian", 7000, null),
      new Employee(110, 10, "Theodore", 11500, 250),
    };
    public final Department[] depts = {
      new Department(10, "Sales", Arrays.asList(emps[0], emps[2])),
      new Department(30, "Marketing", Collections.<Employee>emptyList()),
      new Department(40, "HR", Collections.singletonList(emps[1])),
    };

    public Table foo(int count) {
      return generateStrings(count);
    }

    public Table view(String s) {
      return JdbcTest.view(s);
    }
  }

  public static Table view(String s) {
    return new ViewTable(Object.class,
        new RelProtoDataType() {
          public RelDataType apply(RelDataTypeFactory typeFactory) {
            return typeFactory.builder().add("c", SqlTypeName.INTEGER)
                .build();
          }
        }, "values (1), (3), " + s, ImmutableList.<String>of());
  }

  public static class Employee {
    public final int empid;
    public final int deptno;
    public final String name;
    public final float salary;
    public final Integer commission;

    public Employee(int empid, int deptno, String name, float salary,
        Integer commission) {
      this.empid = empid;
      this.deptno = deptno;
      this.name = name;
      this.salary = salary;
      this.commission = commission;
    }

    public String toString() {
      return "Employee [empid: " + empid + ", deptno: " + deptno
          + ", name: " + name + "]";
    }
  }

  public static class Department {
    public final int deptno;
    public final String name;
    public final List<Employee> employees;

    public Department(
        int deptno, String name, List<Employee> employees) {
      this.deptno = deptno;
      this.name = name;
      this.employees = employees;
    }


    public String toString() {
      return "Department [deptno: " + deptno + ", name: " + name
          + ", employees: " + employees + "]";
    }
  }

  public static class FoodmartSchema {
    public final SalesFact[] sales_fact_1997 = {
      new SalesFact(100, 10),
      new SalesFact(150, 20),
    };
  }

  public static class LingualSchema {
    public final LingualEmp[] EMPS = {
      new LingualEmp(1, 10),
      new LingualEmp(2, 30)
    };
  }

  public static class LingualEmp {
    public final int EMPNO;
    public final int DEPTNO;

    public LingualEmp(int EMPNO, int DEPTNO) {
      this.EMPNO = EMPNO;
      this.DEPTNO = DEPTNO;
    }
  }

  public static class FoodmartJdbcSchema extends JdbcSchema {
    public FoodmartJdbcSchema(DataSource dataSource, SqlDialect dialect,
        JdbcConvention convention, String catalog, String schema) {
      super(dataSource, dialect, convention, catalog, schema);
    }

    public final Table customer = getTable("customer");
  }

  public static class Customer {
    public final int customer_id;

    public Customer(int customer_id) {
      this.customer_id = customer_id;
    }
  }

  public static class SalesFact {
    public final int cust_id;
    public final int prod_id;

    public SalesFact(int cust_id, int prod_id) {
      this.cust_id = cust_id;
      this.prod_id = prod_id;
    }
  }

  //CHECKSTYLE: ON

  public static class IntString {
    public final int n;
    public final String s;

    public IntString(int n, String s) {
      this.n = n;
      this.s = s;
    }

    public String toString() {
      return "{n=" + n + ", s=" + s + "}";
    }
  }

  public abstract static class AbstractModifiableTable
      extends AbstractTable implements ModifiableTable {
    protected AbstractModifiableTable(String tableName) {
      super();
    }

    public TableModificationRelBase toModificationRel(
        RelOptCluster cluster,
        RelOptTable table,
        Prepare.CatalogReader catalogReader,
        RelNode child,
        TableModificationRelBase.Operation operation,
        List<String> updateColumnList,
        boolean flattened) {
      return new TableModificationRel(
          cluster, table, catalogReader, child, operation,
          updateColumnList, flattened);
    }
  }

  public static class EmpDeptTableFactory implements TableFactory<Table> {
    public Table create(
        SchemaPlus schema,
        String name,
        Map<String, Object> operand,
        RelDataType rowType) {
      final Class clazz;
      final Object[] array;
      if (name.equals("EMPLOYEES")) {
        clazz = Employee.class;
        array = new HrSchema().emps;
      } else {
        clazz = Department.class;
        array = new HrSchema().depts;
      }
      return new AbstractQueryableTable(clazz) {
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return ((JavaTypeFactory) typeFactory).createType(clazz);
        }

        public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
            SchemaPlus schema, String tableName) {
          return new AbstractTableQueryable<T>(queryProvider, schema, this,
              tableName) {
            public Enumerator<T> enumerator() {
              @SuppressWarnings("unchecked") final List<T> list =
                  (List) Arrays.asList(array);
              return Linq4j.enumerator(list);
            }
          };
        }
      };
    }
  }

  public static class MySchemaFactory implements SchemaFactory {
    public Schema create(
        SchemaPlus parentSchema,
        String name,
        final Map<String, Object> operand) {
      final boolean mutable =
          SqlFunctions.isNotFalse((Boolean) operand.get("mutable"));
      return new ReflectiveSchema(new HrSchema()) {
        @Override
        protected Map<String, Table> getTableMap() {
          // Mine the EMPS table and add it under another name e.g. ELVIS
          final Map<String, Table> tableMap = super.getTableMap();
          final Table table = tableMap.get("emps");
          final String tableName = (String) operand.get("tableName");
          return ImmutableMap.<String, Table>builder()
              .putAll(tableMap)
              .put(tableName, table)
              .build();
        }

        @Override public boolean isMutable() {
          return mutable;
        }
      };
    }
  }

  /** Mock driver that has a handler that stores the results of each query in
   * a temporary table. */
  public static class AutoTempDriver
      extends net.hydromatic.optiq.jdbc.Driver {
    private final List<Object> results;

    AutoTempDriver(List<Object> results) {
      super();
      this.results = results;
    }

    @Override
    protected Handler createHandler() {
      return new HandlerImpl() {
        @Override
        public void onStatementExecute(
            AvaticaStatement statement,
            ResultSink resultSink) {
          super.onStatementExecute(statement, resultSink);
          results.add(resultSink);
        }
      };
    }
  }

  /** Mock driver that a given {@link Handler}. */
  public static class HandlerDriver extends net.hydromatic.optiq.jdbc.Driver {
    private static final ThreadLocal<Handler> HANDLERS =
        new ThreadLocal<Handler>();

    public HandlerDriver() {
    }

    @Override
    protected Handler createHandler() {
      return HANDLERS.get();
    }
  }

  public static class MyTable {
    public String mykey = "foo";
    public Integer myvalue = 1;
  }

  public static class MyTable2 {
    public String mykey = "foo";
    public Integer myvalue = 2;
  }

  public static class MySchema {
    public MyTable[] mytable = { new MyTable() };
    public MyTable2[] mytable2 = { new MyTable2() };
  }

  /** Example of a UDF with a non-static {@code eval} method. */
  public static class MyPlusFunction {
    public int eval(int x, int y) {
      return x + y;
    }
  }

  /** Example of a non-strict UDF. (Does something useful when passed NULL.) */
  public static class MyToStringFunction {
    public static String eval(Object o) {
      if (o == null) {
        return "<null>";
      }
      return "<" + o.toString() + ">";
    }
  }

  /** Example of a UDF with a static {@code eval} method. Class is abstract,
   * but code-generator should not need to instantiate it. */
  public abstract static class MyDoubleFunction {
    private MyDoubleFunction() {
    }

    public static int eval(int x) {
      return x * 2;
    }
  }

  /** Example of a UDF class that needs to be instantiated but cannot be. */
  public abstract static class AwkwardFunction {
    private AwkwardFunction() {
    }

    public int eval(int x) {
      return 0;
    }
  }

  /** Example of a user-defined aggregate function (UDAF). */
  public static class MySumFunction {
    public MySumFunction() {
    }
    public long init() {
      return 0L;
    }
    public long initAdd(int v) {
      return v;
    }
    public long add(long accumulator, int v) {
      return accumulator + v;
    }
    public long merge(long accumulator0, long accumulator1) {
      return accumulator0 + accumulator1;
    }
    public long result(long accumulator) {
      return accumulator;
    }
  }

  /** Example of a user-defined aggregate function (UDAF), whose methods are
   * static. */
  public static class MyStaticSumFunction {
    public static long init() {
      return 0L;
    }
    public static long initAdd(int v) {
      return v;
    }
    public static long add(long accumulator, int v) {
      return accumulator + v;
    }
    public static long merge(long accumulator0, long accumulator1) {
      return accumulator0 + accumulator1;
    }
    public static long result(long accumulator) {
      return accumulator;
    }
  }

  public static class SumFunctionBadInitAdd {
    public long init() {
      return 0L;
    }
    public long initAdd(int v) {
      return v;
    }
    public long add(long accumulator, long v) {
      return accumulator + v;
    }
  }

  public static class SumFunctionBadIAdd {
    public long init() {
      return 0L;
    }
    public long add(short accumulator, int v) {
      return accumulator + v;
    }
  }

  public static class TableMacroFunction {
    public Table eval(String s) {
      return view(s);
    }
  }

  public static class StaticTableMacroFunction {
    public static Table eval(String s) {
      return view(s);
    }
  }
}

// End JdbcTest.java
