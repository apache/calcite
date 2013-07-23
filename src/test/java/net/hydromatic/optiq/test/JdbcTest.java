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

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.linq4j.expressions.Types;
import net.hydromatic.linq4j.function.Function1;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.*;
import net.hydromatic.optiq.impl.clone.CloneSchema;
import net.hydromatic.optiq.impl.generate.RangeTable;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.java.MapSchema;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.impl.jdbc.JdbcSchema;
import net.hydromatic.optiq.jdbc.*;
import net.hydromatic.optiq.prepare.Prepare;

import org.apache.commons.dbcp.BasicDataSource;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlDialect;
import org.eigenbase.util.Bug;
import org.eigenbase.util.Pair;

import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.sql.*;
import java.sql.Statement;
import java.util.*;
import javax.sql.DataSource;

import static org.junit.Assert.*;

/**
 * Tests for using Optiq via JDBC.
 */
public class JdbcTest {
  public static final Method GENERATE_STRINGS_METHOD =
      Types.lookupMethod(
          JdbcTest.class, "generateStrings", Integer.TYPE);

  public static final Method STRING_UNION_METHOD =
      Types.lookupMethod(
          JdbcTest.class, "stringUnion", Queryable.class, Queryable.class);

  public static final String FOODMART_SCHEMA =
      "     {\n"
      + "       type: 'jdbc',\n"
      + "       name: 'foodmart',\n"
      + "       jdbcUser: 'foodmart',\n"
      + "       jdbcPassword: 'foodmart',\n"
      + "       jdbcUrl: 'jdbc:mysql://localhost',\n"
      + "       jdbcCatalog: 'foodmart',\n"
      + "       jdbcSchema: null\n"
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

  static String toString(ResultSet resultSet) throws SQLException {
    StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      int n = resultSet.getMetaData().getColumnCount();
      String sep = "";
      for (int i = 1; i <= n; i++) {
        buf.append(sep)
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=")
            .append(resultSet.getObject(i));
        sep = "; ";
      }
      buf.append("\n");
    }
    return buf.toString();
  }

  /**
   * Tests a relation that is accessed via method syntax.
   * The function returns a {@link Queryable}.
   */
  @Ignore
  @Test public void testFunction() throws SQLException, ClassNotFoundException {
    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    Connection connection =
        DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection =
        connection.unwrap(OptiqConnection.class);
    JavaTypeFactory typeFactory = optiqConnection.getTypeFactory();
    MutableSchema rootSchema = optiqConnection.getRootSchema();
    MapSchema schema = MapSchema.create(rootSchema, "s");
    final TableFunction<Object> tableFunction =
        Schemas.methodMember(GENERATE_STRINGS_METHOD, typeFactory);
    rootSchema.addTableFunction(
        new TableFunctionInSchemaImpl(schema, "GenerateStrings",
            tableFunction));
    ResultSet resultSet = connection.createStatement().executeQuery(
        "select *\n"
        + "from table(s.\"GenerateStrings\"(5)) as t(c)\n"
        + "where char_length(c) > 3");
    assertTrue(resultSet.next());
  }

  public static <T> Queryable<T> stringUnion(
      Queryable<T> q0, Queryable<T> q1) {
    return q0.concat(q1);
  }

  public static Queryable<IntString> generateStrings(final int count) {
    return new BaseQueryable<IntString>(
        null,
        IntString.class,
        null) {
      public Enumerator<IntString> enumerator() {
        return new Enumerator<IntString>() {
          static final String z = "abcdefghijklm";

          int i = -1;
          IntString o;

          public IntString current() {
            return o;
          }

          public boolean moveNext() {
            if (i < count - 1) {
              o = new IntString(
                  i, z.substring(0, i % z.length()));
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
  }

  static OptiqConnection getConnection(String... schema)
      throws ClassNotFoundException, SQLException {
    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    Connection connection =
        DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection =
        connection.unwrap(OptiqConnection.class);
    MutableSchema rootSchema = optiqConnection.getRootSchema();
    final List<String> schemaList = Arrays.asList(schema);
    if (schemaList.contains("hr")) {
      ReflectiveSchema.create(rootSchema, "hr", new HrSchema());
    }
    if (schemaList.contains("foodmart")) {
      ReflectiveSchema.create(
          rootSchema, "foodmart", new FoodmartSchema());
    }
    if (schemaList.contains("lingual")) {
      ReflectiveSchema.create(
          rootSchema, "SALES", new LingualSchema());
    }
    if (schemaList.contains("metadata")) {
      // always present
    }
    return optiqConnection;
  }

  /**
   * Creates a connection with a given query provider. If provider is null,
   * uses the connection as its own provider. The connection contains a
   * schema called "foodmart" backed by a JDBC connection to MySQL.
   *
   * @param queryProvider Query provider
   * @param withClone Whether to create a "foodmart2" schema as in-memory
   *     clone
   * @return Connection
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  static OptiqConnection getConnection(
      QueryProvider queryProvider,
      boolean withClone)
      throws ClassNotFoundException, SQLException {
    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    Class.forName("com.mysql.jdbc.Driver");
    Connection connection = DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection =
        connection.unwrap(OptiqConnection.class);
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:mysql://localhost");
    dataSource.setUsername("foodmart");
    dataSource.setPassword("foodmart");

    JdbcSchema foodmart =
        JdbcSchema.create(
            optiqConnection.getRootSchema(),
            dataSource,
            "foodmart",
            null,
            "foodmart");
    if (withClone) {
      CloneSchema.create(
          optiqConnection.getRootSchema(), "foodmart2", foodmart);
    }
    optiqConnection.setSchema("foodmart2");
    return optiqConnection;
  }

  /**
   * The example in the README.
   */
  @Test public void testReadme() throws ClassNotFoundException, SQLException {
    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    Connection connection = DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection =
        connection.unwrap(OptiqConnection.class);
    ReflectiveSchema.create(
        optiqConnection.getRootSchema(), "hr", new HrSchema());
    Statement statement = optiqConnection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "select d.\"deptno\", min(e.\"empid\")\n"
        + "from \"hr\".\"emps\" as e\n"
        + "join \"hr\".\"depts\" as d\n"
        + "  on e.\"deptno\" = d.\"deptno\"\n"
        + "group by d.\"deptno\"\n"
        + "having count(*) > 1");
    toString(resultSet);
    resultSet.close();
    statement.close();
    connection.close();
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
    assertEquals(4, driverMinorVersion);

    assertEquals("Optiq", metaData.getDatabaseProductName());
    final String databaseProductVersion =
        metaData.getDatabaseProductVersion();
    final int databaseMajorVersion = metaData.getDatabaseMajorVersion();
    assertEquals(driverMajorVersion, databaseMajorVersion);
    final int databaseMinorVersion = metaData.getDatabaseMinorVersion();
    assertEquals(driverMinorVersion, databaseMinorVersion);

    // Check how version is composed of major and minor version. Note that
    // version is stored in pom.xml; major and minor version are
    // stored in net-hydromatic-optiq-jdbc.properties.
    if (!driverVersion.endsWith("-SNAPSHOT")) {
      assertTrue(driverVersion.startsWith("0."));
      String[] split = driverVersion.split("\\.");
      assertTrue(split.length >= 2);
      assertTrue(
          driverVersion.startsWith(
              driverMajorVersion + "." + driverMinorVersion + "."));
    }
    if (!databaseProductVersion.endsWith("-SNAPSHOT")) {
      assertTrue(databaseProductVersion.startsWith("0."));
      String[] split = databaseProductVersion.split("\\.");
      assertTrue(split.length >= 2);
      assertTrue(
          databaseProductVersion.startsWith(
              databaseMajorVersion + "." + databaseMinorVersion + "."));
    }

    connection.close();
  }

  /** Tests driver's implementation of {@link DatabaseMetaData#getColumns}. */
  @Test public void testMetaDataColumns()
      throws ClassNotFoundException, SQLException {
    Connection connection = getConnection("hr", "foodmart");
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getColumns(null, null, null, null);
    assertTrue(resultSet.next()); // there's something
    resultSet.close();
    connection.close();
  }

  /** Tests driver's implementation of {@link DatabaseMetaData#getColumns}. */
  @Test public void testResultSetMetaData()
      throws ClassNotFoundException, SQLException {
    Connection connection = getConnection("hr", "foodmart");
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

  @Test public void testCloneSchema()
      throws ClassNotFoundException, SQLException {
    final OptiqConnection connection = JdbcTest.getConnection(null, false);
    Schema foodmart = connection.getRootSchema().getSubSchema("foodmart");
    CloneSchema.create(connection.getRootSchema(), "foodmart2", foodmart);
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
    OptiqAssert.assertThat()
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
    OptiqAssert.assertThat()
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
  @Test public void testCloneGroupBy2Plan() {
    // NOTE: Plan is nowhere near optimal yet.
    OptiqAssert.assertThat()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "explain plan for select \"time_by_day\".\"the_year\" as \"c0\", \"time_by_day\".\"quarter\" as \"c1\", \"product_class\".\"product_family\" as \"c2\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"product_class\" as \"product_class\", \"product\" as \"product\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" group by \"time_by_day\".\"the_year\", \"time_by_day\".\"quarter\", \"product_class\".\"product_family\"")
        .returns(
            "PLAN=EnumerableAggregateRel(group=[{0, 1, 2}], m0=[SUM($3)])\n"
            + "  EnumerableCalcRel(expr#0..37=[{inputs}], c0=[$t19], c1=[$t23], c2=[$t37], unit_sales=[$t32])\n"
            + "    EnumerableJoinRel(condition=[AND(=($25, $1), =($0, $33))], joinType=[inner])\n"
            + "      EnumerableTableAccessRel(table=[[foodmart2, product]])\n"
            + "      EnumerableJoinRel(condition=[true], joinType=[inner])\n"
            + "        EnumerableJoinRel(condition=[=($11, $0)], joinType=[inner])\n"
            + "          EnumerableCalcRel(expr#0..9=[{inputs}], expr#10=[CAST($t4):INTEGER], expr#11=[1997], expr#12=[=($t10, $t11)], proj#0..9=[{exprs}], $condition=[$t12])\n"
            + "            EnumerableTableAccessRel(table=[[foodmart2, time_by_day]])\n"
            + "          EnumerableTableAccessRel(table=[[foodmart2, sales_fact_1997]])\n"
            + "        EnumerableTableAccessRel(table=[[foodmart2, product_class]])\n"
            + "\n");
  }

  private static final String[] queries = {
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
      "select \"time_by_day\".\"the_year\" as \"c0\", \"product_class\".\"product_family\" as \"c1\", \"customer\".\"state_province\" as \"c2\", \"customer\".\"city\" as \"c3\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"product_class\" as \"product_class\", \"product\" as \"product\", \"customer\" as \"customer\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" and \"product_class\".\"product_family\" = 'Drink' and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and \"customer\".\"state_province\" = 'WA' and \"customer\".\"city\" in ('Anacortes', 'Ballard', 'Bellingham', 'Bremerton', 'Burien', 'Edmonds', 'Everett', 'Issaquah', 'Kirkland', 'Lynnwood', 'Marysville', 'Olympia', 'Port Orchard', 'Puyallup', 'Redmond', 'Renton', 'Seattle', 'Sedro Woolley', 'Spokane', 'Tacoma', 'Walla Walla', 'Yakima') group by \"time_by_day\".\"the_year\", \"product_class\".\"product_family\", \"customer\".\"state_province\", \"customer\".\"city\"",
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
  };

  /** Test case for
   * <a href="https://github.com/julianhyde/optiq/issues/35">issue #35</a>. */
  @Ignore
  @Test public void testJoinJoin() {
    OptiqAssert.assertThat()
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

  /** Returns a list of (query, expected) pairs. The expected result is
   * sometimes null. */
  public static List<Pair<String, String>> getFoodmartQueries() {
    final List<Pair<String, String>> list =
        new ArrayList<Pair<String, String>>();
    for (int i = 0; i < queries.length; i++) {
      String query = queries[i];
      String expected = null;
      if (i + 1 < queries.length
          && queries[i + 1] != null
          && !queries[i + 1].startsWith("select")) {
        expected = queries[++i];
      }
      list.add(Pair.of(query, expected));
    }
    return list;
  }

  /** A selection of queries generated by Mondrian. */
  @Test public void testCloneQueries() {
    OptiqAssert.AssertThat with =
        OptiqAssert.assertThat()
            .with(OptiqAssert.Config.FOODMART_CLONE);
    final List<Pair<String, String>> queries = getFoodmartQueries();
    for (Ord<Pair<String, String>> query : Ord.zip(queries)) {
      try {
        // uncomment to run specific queries:
//      if (query.i != queries.size() - 1) continue;
        final String sql = query.e.left;
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

  @Test public void testFoo() {
    OptiqAssert.assertThat()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
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
            + "group by \"store\".\"store_country\", \"time_by_day\".\"the_year\"")
//        .explainContains("xxx")
        .runs();
  }

  /** There was a bug representing a nullable timestamp using a {@link Long}
   * internally. */
  @Test public void testNullableTimestamp() {
    OptiqAssert.assertThat()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"hire_date\" from \"employee\" where \"employee_id\" = 1")
        .returns("hire_date=1994-12-01T08:00:00Z\n");
  }

  @Test public void testValues() {
    OptiqAssert.assertThat()
        .query("values (1), (2)")
        .returns(
            "EXPR$0=1\n"
            + "EXPR$0=2\n");
  }

  @Test public void testValuesAlias() {
    OptiqAssert.assertThat()
        .query(
            "select \"desc\" from (VALUES ROW(1, 'SameName')) AS \"t\" (\"id\", \"desc\")")
        .returns("desc=SameName\n");
  }

  @Test public void testValuesMinus() {
    OptiqAssert.assertThat()
        .query("values (-2-1)")
        .returns("EXPR$0=-3\n");
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
    OptiqAssert.assertThat()
        .query("values (1, 'a'), (2, 'abc')")
        .returns(
            "EXPR$0=1; EXPR$1=a  \n"
            + "EXPR$0=2; EXPR$1=abc\n");
  }

  @Test public void testInnerJoinValues() {
    OptiqAssert.assertThat()
        .with(OptiqAssert.Config.LINGUAL)
        .query("select empno, desc from sales.emps,\n"
               + "  (SELECT * FROM (VALUES (10, 'SameName')) AS t (id, desc)) as sn\n"
               + "where emps.deptno = sn.id and sn.desc = 'SameName' group by empno, desc")
        .returns("EMPNO=1; DESC=SameName\n");
    }

  @Test public void testDistinctCount() {
    final String s =
        "select \"time_by_day\".\"the_year\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 group by \"time_by_day\".\"the_year\"";
    OptiqAssert.assertThat()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(s)
        .returns("c0=1997; m0=266773.0000\n");
  }

  /** A difficult query: an IN list so large that the planner promotes it
   * to a semi-join against a VALUES relation. */
  @Test public void testIn() {
    OptiqAssert.assertThat()
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
            + "and \"customer\".\"city\" in ('Anacortes', 'Ballard', 'Bellingham', 'Bremerton', 'Burien', 'Edmonds', 'Everett', 'Issaquah', 'Kirkland', 'Lynnwood', 'Marysville', 'Olympia', 'Port Orchard', 'Puyallup', 'Redmond', 'Renton', 'Seattle', 'Sedro Woolley', 'Spokane', 'Tacoma', 'Walla Walla', 'Yakima') group by \"time_by_day\".\"the_year\", \"product_class\".\"product_family\", \"customer\".\"country\", \"customer\".\"state_province\", \"customer\".\"city\"")
        .returns(
            "c0=1997; c1=Drink; c2=USA; c3=WA; c4=Sedro Woolley; m0=58.0000\n");
  }

  /** Query that uses parenthesized JOIN. */
  @Test public void testSql92JoinParenthesized() {
    if (!Bug.TodoFixed) {
      return;
    }
    OptiqAssert.assertThat()
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

  /** Tests ORDER BY ... DESC NULLS FIRST. */
  @Test public void testOrderByDescNullsFirst() {
    OptiqAssert.assertThat()
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
    OptiqAssert.assertThat()
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
    OptiqAssert.assertThat()
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
    OptiqAssert.assertThat()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select \"store_id\", \"grocery_sqft\" from \"store\"\n"
            + "where \"store_id\" < 3 order by 2 nulls last")
        .returns(
            "store_id=1; grocery_sqft=17475\n"
            + "store_id=2; grocery_sqft=22271\n"
            + "store_id=0; grocery_sqft=null\n");
  }

  /** Tests sorting by a column that is already sorted. */
  @Test public void testOrderByOnSortedTable() {
    OptiqAssert.assertThat()
        .with(OptiqAssert.Config.FOODMART_CLONE)
        .query(
            "select * from \"time_by_day\"\n"
            + "order by \"time_id\"")
        .explainContains(
            "PLAN=EnumerableSortRel(sort0=[$0], dir0=[Ascending])\n"
            + "  EnumerableTableAccessRel(table=[[foodmart2, time_by_day]])\n\n");
  }

  /** Tests sorting by a column that is already sorted. */
  @Ignore("fix output for timezone")
  @Test public void testOrderByOnSortedTable2() {
    OptiqAssert.assertThat()
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

  /** Tests windowed aggregation. */
  @Test public void testWinAgg() {
    OptiqAssert.assertThat()
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
            + "  EnumerableWindowRel(window#0=[window(partition {1} order by [0 Ascending] rows between 1 PRECEDING and CURRENT ROW aggs [COUNT($3), $SUM0($3), MIN($2), COUNT()])])\n"
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
    OptiqAssert.assertThat()
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
    OptiqAssert.assertThat()
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
    OptiqAssert.assertThat()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select * from \"hr\".\"emps\"\n"
            + "where \"commission\" > 800")
        .returns(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n");
  }

  /** Tests the LIKE operator. */
  @Test public void testLike() {
    OptiqAssert.assertThat()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select * from \"hr\".\"emps\"\n"
            + "where \"name\" like '%i__'")
        .returns(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n");
  }

  /** Tests array index. */
  @Test public void testArray() {
    OptiqAssert.assertThat()
        .with(OptiqAssert.Config.REGULAR)
        .query(
            "select \"deptno\", \"employees\"[1] as e from \"hr\".\"depts\"\n")
        .returns(
            "deptno=10; E=Employee [empid: 100, deptno: 10, name: Bill]\n"
            + "deptno=30; E=null\n"
            + "deptno=40; E=Employee [empid: 200, deptno: 20, name: Eric]\n");
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

  private OptiqAssert.AssertQuery predicate(String foo) {
      return OptiqAssert.assertThat()
          .with(OptiqAssert.Config.REGULAR)
          .query(
              "select * from \"hr\".\"emps\"\n"
              + "where " + foo)
          .runs();
  }

  /** Tests the TABLES table in the information schema. */
  @Test public void testMetaTables() {
    OptiqAssert.assertThat()
        .with(OptiqAssert.Config.REGULAR_PLUS_METADATA)
        .query("select * from \"metadata\".TABLES")
        .returns(
            OptiqAssert.checkResultContains(
                "tableSchem=metadata; tableName=COLUMNS; tableType=SYSTEM_TABLE; "));

    OptiqAssert.assertThat()
        .with(OptiqAssert.Config.REGULAR_PLUS_METADATA)
        .query(
            "select count(distinct \"tableSchem\") as c\n"
            + "from \"metadata\".TABLES")
        .returns("C=3\n");
  }

  /** Tests that {@link java.sql.Statement#setMaxRows(int)} is honored. */
  @Test public void testSetMaxRows() throws Exception {
    OptiqAssert.assertThat()
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

  /** Tests a JDBC connection that provides a model (a single schema based on
   * a JDBC database). */
  @Test public void testModel() {
    OptiqAssert.assertThat()
        .withModel(FOODMART_MODEL)
        .query("select count(*) as c from \"foodmart\".\"time_by_day\"")
        .returns("C=730\n");
  }

  /** Defines a materialized view and tests that the query is rewritten to use
   * it, and that the query produces the same result with and without it. There
   * are more comprehensive tests in {@link MaterializationTest}. */
  @Ignore("until JdbcSchema can define materialized views")
   @Test public void testModelWithMaterializedView() {
    OptiqAssert.assertThat()
        .withModel(FOODMART_MODEL)
        .enable(false)
        .query(
            "select count(*) as c from \"foodmart\".\"sales_fact_1997\" join \"foodmart\".\"time_by_day\" using (\"time_id\")")
        .returns("C=86837\n");
    OptiqAssert.assertThat().withMaterializations(
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
    OptiqAssert.assertThat()
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
    OptiqAssert.assertThat()
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
        OptiqAssert.assertThat().withModel(
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
        OptiqAssert.assertThat().withModel(
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
        "Cannot define view; parent schema "
        + "DelegatingSchema(delegate=ReflectiveSchema(target=HrSchema)) is "
        + "not mutable");
  }

  /** Tests a JDBC connection that provides a model that contains a view. */
  @Test public void testModelView() throws Exception {
    final OptiqAssert.AssertThat with = OptiqAssert.assertThat()
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
            + "           operand: {'foo': true, 'bar': 345}\n"
            + "         },\n"
            + "         {\n"
            + "           name: 'V',\n"
            + "           type: 'view',\n"
            + "           sql: 'select * from \"EMPLOYEES\" where \"deptno\" = 10'\n"
            + "         }\n"
            + "       ]\n"
            + "     }\n"
            + "   ]\n"
            + "}");

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
                  JdbcTest.toString(
                      metaData.getTables(null, "adhoc", null, null)));

              // views only
              assertEquals(
                  "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; TABLE_TYPE=VIEW; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n",
                  JdbcTest.toString(
                      metaData.getTables(
                          null, "adhoc", null,
                          new String[] {
                              Schema.TableType.VIEW.name()})));
              return null;
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }
        });
  }

  /** Tests saving query results into temporary tables, per
   * {@link net.hydromatic.optiq.jdbc.Handler.ResultSink}. */
  @Test public void testAutomaticTemporaryTable() throws Exception {
    final List<Object> objects = new ArrayList<Object>();
    OptiqAssert.assertThat()
        .with(
            new OptiqAssert.ConnectionFactory() {
              public OptiqConnection createConnection() throws Exception {
                OptiqConnection connection = (OptiqConnection)
                    new AutoTempDriver(objects)
                        .connect("jdbc:optiq:", new Properties());
                MutableSchema rootSchema = connection.getRootSchema();
                ReflectiveSchema.create(
                    rootSchema, "hr", new HrSchema());
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

  @Test public void testExplain() {
    final OptiqAssert.AssertThat with =
        OptiqAssert.assertThat().with(OptiqAssert.Config.FOODMART_CLONE);
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
    final MutableSchema rootSchema = optiqConnection.getRootSchema();
    ReflectiveSchema.create(rootSchema, "TEST", new MySchema());
    Statement statement = optiqConnection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("SELECT \"myvalue\" from TEST.\"mytable2\"");
    assertEquals("myvalue=2\n", toString(resultSet));
    resultSet.close();
    statement.close();
    connection.close();
  }

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
    public FoodmartJdbcSchema(
        MutableSchema parentSchema,
        DataSource dataSource,
        SqlDialect dialect,
        String catalog,
        String schema,
        JavaTypeFactory typeFactory,
        Expression expression) {
      super(
          parentSchema.getQueryProvider(),
          parentSchema,
          "FoodMart",
          dataSource,
          dialect,
          catalog,
          schema,
          typeFactory,
          expression);
    }

    public final Table<Customer> customer =
        getTable("customer", Customer.class);
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

  public static class AnInt {
    public final int n;

    public AnInt(int n) {
      this.n = n;
    }
  }

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

  public static abstract class AbstractModifiableTable<T>
      extends AbstractTable<T>
      implements ModifiableTable<T> {
    protected AbstractModifiableTable(
        Schema schema,
        Type elementType,
        RelDataType relDataType,
        String tableName) {
      super(schema, elementType, relDataType, tableName);
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
        Schema schema,
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
      return new AbstractTable(
          schema,
          clazz,
          schema.getTypeFactory().createType(clazz),
          name) {
        public Enumerator enumerator() {
          return Linq4j.enumerator(Arrays.asList(array));
        }
      };
    }
  }

  public static class MySchemaFactory implements SchemaFactory {
    public Schema create(
        MutableSchema parentSchema,
        String name,
        Map<String, Object> operand) {
      final ReflectiveSchema schema =
          ReflectiveSchema.create(parentSchema, name, new HrSchema());

      // Mine the EMPS table and add it under another name e.g. ELVIS
      final Table table = schema.getTable("emps", Object.class);

      String tableName = (String) operand.get("tableName");
      schema.addTable(
          new TableInSchemaImpl(
              schema, tableName, Schema.TableType.TABLE, table));

      final Boolean mutable = (Boolean) operand.get("mutable");
      if (mutable == null || mutable) {
        return schema;
      } else {
        // Wrap the schema in DelegatingSchema so that it does not
        // implement MutableSchema.
        return new DelegatingSchema(schema);
      }
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
            OptiqStatement statement,
            ResultSink resultSink) {
          super.onStatementExecute(statement, resultSink);
          results.add(resultSink);
        }
      };
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
}

// End JdbcTest.java
