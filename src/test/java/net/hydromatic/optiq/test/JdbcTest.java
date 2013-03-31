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
import net.hydromatic.linq4j.function.Predicate1;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.AbstractTable;
import net.hydromatic.optiq.impl.ViewTable;
import net.hydromatic.optiq.impl.clone.CloneSchema;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.java.MapSchema;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.impl.jdbc.JdbcSchema;
import net.hydromatic.optiq.jdbc.*;
import net.hydromatic.optiq.prepare.Prepare;

import junit.framework.TestCase;

import org.apache.commons.dbcp.BasicDataSource;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlDialect;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.sql.*;
import java.sql.Statement;
import java.util.*;
import javax.sql.DataSource;

/**
 * Tests for using Optiq via JDBC.
 *
 * @author jhyde
 */
public class JdbcTest extends TestCase {
    public static final Method LINQ4J_AS_ENUMERABLE_METHOD =
        Types.lookupMethod(
            Linq4j.class,
            "asEnumerable",
            Object[].class);

    public static final Method GENERATE_STRINGS_METHOD =
        Types.lookupMethod(
            JdbcTest.class, "generateStrings", Integer.TYPE);

    public static final Method STRING_UNION_METHOD =
        Types.lookupMethod(
            JdbcTest.class, "stringUnion", Queryable.class, Queryable.class);

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
     * Test that uses a JDBC connection as a linq4j {@link QueryProvider}.
     *
     * @throws Exception on error
     */
    public void testQueryProvider() throws Exception {
        Connection connection = getConnection("hr", "foodmart");
        QueryProvider queryProvider = connection.unwrap(QueryProvider.class);
        ParameterExpression e = Expressions.parameter(Employee.class, "e");

        // "Enumerable<T> asEnumerable(final T[] ts)"
        List<Object[]> list =
            queryProvider.createQuery(
                Expressions.call(
                    Expressions.call(
                        Types.of(
                            Enumerable.class,
                            Employee.class),
                        null,
                        LINQ4J_AS_ENUMERABLE_METHOD,
                        Arrays.<Expression>asList(
                            Expressions.constant(new HrSchema().emps))),
                    "asQueryable",
                    Collections.<Expression>emptyList()),
                Employee.class)
                .where(
                    Expressions.<Predicate1<Employee>>lambda(
                        Expressions.lessThan(
                            Expressions.field(
                                e, "empid"),
                            Expressions.constant(160)),
                        Arrays.asList(e)))
                .where(
                    Expressions.<Predicate1<Employee>>lambda(
                        Expressions.greaterThan(
                            Expressions.field(
                                e, "empid"),
                            Expressions.constant(140)),
                        Arrays.asList(e)))
                .select(
                    Expressions.<Function1<Employee, Object[]>>lambda(
                        Expressions.new_(
                            Object[].class,
                            Arrays.<Expression>asList(
                                Expressions.field(
                                    e, "empid"),
                                Expressions.call(
                                    Expressions.field(
                                        e, "name"),
                                    "toUpperCase",
                                    Collections.<Expression>emptyList()))),
                        Arrays.asList(e)))
                .toList();
        assertEquals(1, list.size());
        assertEquals(2, list.get(0).length);
        assertEquals(150, list.get(0)[0]);
        assertEquals("SEBASTIAN", list.get(0)[1]);
    }

    public void testQueryProviderSingleColumn() throws Exception {
        Connection connection = getConnection("hr", "foodmart");
        QueryProvider queryProvider = connection.unwrap(QueryProvider.class);
        ParameterExpression e = Expressions.parameter(Employee.class, "e");

        // "Enumerable<T> asEnumerable(final T[] ts)"
        List<Integer> list =
            queryProvider.createQuery(
                Expressions.call(
                    Expressions.call(
                        Types.of(
                            Enumerable.class, Employee.class),
                        null,
                        LINQ4J_AS_ENUMERABLE_METHOD,
                        Arrays.<Expression>asList(
                            Expressions.constant(new HrSchema().emps))),
                    "asQueryable",
                    Collections.<Expression>emptyList()), Employee.class)
                .select(
                    Expressions.<Function1<Employee, Integer>>lambda(
                        Expressions.field(
                            e, "empid"),
                        Arrays.asList(e)))
                .toList();
        assertEquals(Arrays.asList(100, 200, 150), list);
    }

    /**
     * Tests a relation that is accessed via method syntax.
     * The function returns a {@link Queryable}.
     */
    public void _testFunction() throws SQLException, ClassNotFoundException {
        Class.forName("net.hydromatic.optiq.jdbc.Driver");
        Connection connection =
            DriverManager.getConnection("jdbc:optiq:");
        OptiqConnection optiqConnection =
            connection.unwrap(OptiqConnection.class);
        JavaTypeFactory typeFactory = optiqConnection.getTypeFactory();
        MutableSchema rootSchema = optiqConnection.getRootSchema();
        MapSchema schema = MapSchema.create(optiqConnection, rootSchema, "s");
        rootSchema.addTableFunction(
            "GenerateStrings",
            Schemas.methodMember(
                GENERATE_STRINGS_METHOD, typeFactory));
        ResultSet resultSet = connection.createStatement().executeQuery(
            "select *\n"
            + "from table(s.\"GenerateStrings\"(5)) as t(c)\n"
            + "where char_length(c) > 3");
        assertTrue(resultSet.next());
    }

    public static <T> Queryable<T> stringUnion(
        Queryable<T> q0, Queryable<T> q1)
    {
        return q0.concat(q1);
    }

    public static Queryable<IntString> generateStrings(final int count) {
        return new BaseQueryable<IntString>(
            null,
            IntString.class,
            null)
        {
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
                };
            }
        };
    }

    /**
     * Tests a relation that is accessed via method syntax.
     * The function returns a {@link Queryable}.
     */
    public void _testOperator() throws SQLException, ClassNotFoundException {
        Class.forName("net.hydromatic.optiq.jdbc.Driver");
        Connection connection =
            DriverManager.getConnection("jdbc:optiq:");
        OptiqConnection optiqConnection =
            connection.unwrap(OptiqConnection.class);
        JavaTypeFactory typeFactory = optiqConnection.getTypeFactory();
        MutableSchema rootSchema = optiqConnection.getRootSchema();
        MapSchema schema = MapSchema.create(optiqConnection, rootSchema, "s");
        schema.addTableFunction(
            "GenerateStrings",
            Schemas.methodMember(
                GENERATE_STRINGS_METHOD, typeFactory));
        schema.addTableFunction(
            "StringUnion",
            Schemas.methodMember(
                STRING_UNION_METHOD, typeFactory));
        ReflectiveSchema.create(
            optiqConnection, rootSchema, "hr", new HrSchema());
        ResultSet resultSet = connection.createStatement().executeQuery(
            "select *\n"
            + "from table(s.StringUnion(\n"
            + "  GenerateStrings(5),\n"
            + "  cursor (select name from emps)))\n"
            + "where char_length(s) > 3");
        assertTrue(resultSet.next());
    }

    /**
     * Tests a view.
     */
    public void testView() throws SQLException, ClassNotFoundException {
        Class.forName("net.hydromatic.optiq.jdbc.Driver");
        Connection connection =
            DriverManager.getConnection("jdbc:optiq:");
        OptiqConnection optiqConnection =
            connection.unwrap(OptiqConnection.class);
        MutableSchema rootSchema = optiqConnection.getRootSchema();
        MapSchema schema = MapSchema.create(optiqConnection, rootSchema, "s");
        schema.addTableFunction(
            "emps_view",
            ViewTable.viewFunction(
                schema,
                "emps_view",
                "select * from \"hr\".\"emps\" where \"deptno\" = 10",
                Collections.<String>emptyList()));
        ReflectiveSchema.create(
            optiqConnection, rootSchema, "hr", new HrSchema());
        ResultSet resultSet = connection.createStatement().executeQuery(
            "select *\n"
            + "from \"s\".\"emps_view\"\n"
            + "where \"empid\" < 120");
        assertEquals(
            "empid=100; deptno=10; name=Bill; commission=1000\n",
            toString(resultSet));
    }

    static OptiqConnection getConnection(String... schema)
        throws ClassNotFoundException, SQLException
    {
        Class.forName("net.hydromatic.optiq.jdbc.Driver");
        Connection connection =
            DriverManager.getConnection("jdbc:optiq:");
        OptiqConnection optiqConnection =
            connection.unwrap(OptiqConnection.class);
        MutableSchema rootSchema = optiqConnection.getRootSchema();
        final List<String> schemaList = Arrays.asList(schema);
        if (schemaList.contains("hr")) {
            ReflectiveSchema.create(
                optiqConnection, rootSchema, "hr", new HrSchema());
        }
        if (schemaList.contains("foodmart")) {
            ReflectiveSchema.create(
                optiqConnection, rootSchema, "foodmart", new FoodmartSchema());
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
        throws ClassNotFoundException, SQLException
    {
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
                optiqConnection,
                optiqConnection.getRootSchema(),
                dataSource,
                "foodmart",
                "",
                "foodmart");
        if (withClone) {
            CloneSchema.create(
                optiqConnection, optiqConnection.getRootSchema(),
                "foodmart2", foodmart);
        }
        optiqConnection.setSchema("foodmart2");
        return optiqConnection;
    }

    /**
     * The example in the README.
     */
    public void testReadme() throws ClassNotFoundException, SQLException {
        Class.forName("net.hydromatic.optiq.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:optiq:");
        OptiqConnection optiqConnection =
            connection.unwrap(OptiqConnection.class);
        ReflectiveSchema.create(
            optiqConnection, optiqConnection.getRootSchema(),
            "hr", new HrSchema());
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

    /** Tests driver's implementation of {@link DatabaseMetaData#getColumns}. */
    public void testMetaDataColumns()
        throws ClassNotFoundException, SQLException
    {
        Connection connection = getConnection("hr", "foodmart");
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet = metaData.getColumns(null, null, null, null);
        assertTrue(resultSet.next()); // there's something
        resultSet.close();
        connection.close();
    }

    /** Tests driver's implementation of {@link DatabaseMetaData#getColumns}. */
    public void testResultSetMetaData()
        throws ClassNotFoundException, SQLException
    {
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

    public void testCloneSchema() throws ClassNotFoundException, SQLException {
        final OptiqConnection connection = JdbcTest.getConnection(null, false);
        Schema foodmart = connection.getRootSchema().getSubSchema("foodmart");
        CloneSchema.create(
            connection, connection.getRootSchema(), "foodmart2", foodmart);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select count(*) from \"foodmart2\".\"time_by_day\"");
        assertTrue(resultSet.next());
        assertEquals(730, resultSet.getInt(1));
        resultSet.close();
        connection.close();
    }

    public void testCloneGroupBy() {
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

    public void testCloneGroupBy2() {
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
    };

    /** A selection of queries generated by Mondrian. */
    public void testCloneQueries() {
        OptiqAssert.AssertThat with =
            OptiqAssert.assertThat()
                .with(OptiqAssert.Config.FOODMART_CLONE);
        for (int i = 0; i < queries.length; i++) {
            String query = queries[i];
            try {
                String expected = null;
                if (i + 1 < queries.length
                    && queries[i + 1] != null
                    && !queries[i + 1].startsWith("select"))
                {
                    expected = queries[++i];
                }
                // uncomment to run specific queries:
                //if (i != 75) continue;
                final OptiqAssert.AssertQuery query1 = with.query(query);
                if (expected != null) {
                    query1.returns(expected);
                } else {
                    query1.runs();
                }
            } catch (Throwable e) {
                throw new RuntimeException("while running query #" + i, e);
            }
        }
    }

    public void testValues() {
        OptiqAssert.assertThat()
            .query("values (1), (2)")
            .returns(
                "EXPR$0=1\n"
                + "EXPR$0=2\n");
    }

    public void testValuesMinus() {
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
    public void testValuesComposite() {
        OptiqAssert.assertThat()
            .query("values (1, 'a'), (2, 'abc')")
            .returns(
                "EXPR$0=1; EXPR$1=a  \n"
                + "EXPR$0=2; EXPR$1=abc\n");
    }

    public void testDistinctCount() {
        final String s =
            "select \"time_by_day\".\"the_year\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 group by \"time_by_day\".\"the_year\"";
        OptiqAssert.assertThat()
            .with(OptiqAssert.Config.FOODMART_CLONE)
            .query(s)
            .returns("c0=1997; m0=266773.0000\n");
    }

    /** A difficult query: an IN list so large that the planner promotes it
     * to a semi-join against a VALUES relation. */
    public void testIn() {
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

    /** Tests ORDER BY ... DESC NULLS FIRST. */
    public void testOrderByDescNullsFirst() {
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
    public void testOrderByNullsFirst() {
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
    public void testOrderByDescNullsLast() {
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
    public void testOrderByNullsLast() {
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

    /** Tests WHERE comparing a nullable integer with an integer literal. */
    public void testWhereNullable() {
        OptiqAssert.assertThat()
            .with(OptiqAssert.Config.REGULAR)
            .query(
                "select * from \"hr\".\"emps\"\n"
                + "where \"commission\" > 800")
            .returns(
                "empid=100; deptno=10; name=Bill; commission=1000\n");
    }

    /** Tests array index. */
    public void testArray() {
        OptiqAssert.assertThat()
            .with(OptiqAssert.Config.REGULAR)
            .query(
                "select \"deptno\", \"employees\"[1] as e from \"hr\".\"depts\"\n")
            .returns(
                "deptno=10; E=Employee [empid: 100, deptno: 10, name: Bill]\n"
                + "deptno=30; E=null\n"
                + "deptno=40; E=Employee [empid: 200, deptno: 20, name: Eric]\n");
    }

    /** Tests the TABLES table in the information schema. */
    public void testMetaTables() {
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
    public void testSetMaxRows() throws Exception {
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
                                assertEquals(
                                    e.getMessage(),
                                    "illegal maxRows value: -1");
                            }
                            statement.setMaxRows(2);
                            assertEquals(2, statement.getMaxRows());
                            final ResultSet resultSet =
                                statement.executeQuery(
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
                }
            );
    }
    /** Tests a JDBC connection that provides a model (a single schema based on
     * a JDBC database). */
    public void testModel() {
        OptiqAssert.assertThat()
            .withModel(
                "{\n"
                + "  version: '1.0',\n"
                + "   schemas: [\n"
                + "     {\n"
                + "       type: 'jdbc',\n"
                + "       name: 'foodmart',\n"
                + "       jdbcUser: 'foodmart',\n"
                + "       jdbcPassword: 'foodmart',\n"
                + "       jdbcUrl: 'jdbc:mysql://localhost',\n"
                + "       jdbcCatalog: 'foodmart',\n"
                + "       jdbcSchema: ''\n"
                + "     }\n"
                + "   ]\n"
                + "}")
            .query("select count(*) as c from \"foodmart\".\"time_by_day\"")
            .returns("C=730\n");
    }

    /** Tests a JDBC connection that provides a model that contains custom
     * tables. */
    public void testModelCustom() {
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
                + EmpDeptFactory.class.getName() + "',\n"
                + "           operand: ['foo', 'bar', 345]\n"
                + "         }\n"
                + "       ]\n"
                + "     }\n"
                + "   ]\n"
                + "}")
            .query("select * from \"adhoc\".EMPLOYEES where \"deptno\" = 10")
            .returns(
                "empid=100; deptno=10; name=Bill; commission=1000\n"
                + "empid=150; deptno=10; name=Sebastian; commission=null\n");
    }

    /** Tests a JDBC connection that provides a model that contains a view. */
    public void testModelView() {
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
                + EmpDeptFactory.class.getName() + "',\n"
                + "           operand: ['foo', 'bar', 345]\n"
                + "         },\n"
                + "         {\n"
                + "           name: 'V',\n"
                + "           type: 'view',\n"
                + "           sql: 'select * from \"EMPLOYEES\" where \"deptno\" = 10'\n"
                + "         }\n"
                + "       ]\n"
                + "     }\n"
                + "   ]\n"
                + "}")
            .query("select * from \"adhoc\".V order by \"name\" desc")
            .returns(
                "empid=150; deptno=10; name=Sebastian; commission=null\n"
                + "empid=100; deptno=10; name=Bill; commission=1000\n");
    }

    /** Tests saving query results into temporary tables, per
     * {@link net.hydromatic.optiq.jdbc.Handler.ResultSink}. */
    public void testAutomaticTemporaryTable() throws Exception {
        final List<Object> objects = new ArrayList<Object>();
        OptiqAssert.assertThat()
            .with(
                new OptiqAssert.ConnectionFactory() {
                    public OptiqConnection createConnection() throws Exception {
                        OptiqConnection connection = new AutoTempDriver(objects)
                            .connect("jdbc:optiq:", new Properties());
                        MutableSchema rootSchema = connection.getRootSchema();
                        ReflectiveSchema.create(
                            connection, rootSchema, "hr", new HrSchema());
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

    public void testExplain() {
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

    public static class HrSchema {
        public final Employee[] emps = {
            new Employee(100, 10, "Bill", 1000),
            new Employee(200, 20, "Eric", 500),
            new Employee(150, 10, "Sebastian", null),
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
        public final Integer commission;

        public Employee(int empid, int deptno, String name, Integer commission)
        {
            this.empid = empid;
            this.deptno = deptno;
            this.name = name;
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
            int deptno, String name, List<Employee> employees)
        {
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

    public static class FoodmartJdbcSchema extends JdbcSchema {
        public FoodmartJdbcSchema(
            QueryProvider queryProvider,
            DataSource dataSource,
            SqlDialect dialect,
            String catalog,
            String schema,
            JavaTypeFactory typeFactory,
            Expression expression)
        {
            super(
                queryProvider,
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
        implements ModifiableTable<T>
    {
        protected AbstractModifiableTable(
            Schema schema,
            Type elementType,
            RelDataType relDataType,
            String tableName)
        {
            super(schema, elementType, relDataType, tableName);
        }

        public TableModificationRelBase toModificationRel(
            RelOptCluster cluster,
            RelOptTable table,
            Prepare.CatalogReader catalogReader,
            RelNode child,
            TableModificationRelBase.Operation operation,
            List<String> updateColumnList,
            boolean flattened)
        {
            return new TableModificationRel(
                cluster, table, catalogReader, child, operation,
                updateColumnList, flattened);
        }
    }

    public static class EmpDeptFactory implements TableFactory {
        public Table create(
            JavaTypeFactory typeFactory,
            Schema schema, String name, Object operand, RelDataType rowType)
        {
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
                typeFactory.createType(clazz),
                name)
            {
                public Enumerator enumerator() {
                    return Linq4j.enumerator(Arrays.asList(array));
                }
            };
        }
    }

    /** Mock driver that has a handler that stores the results of each query in
     * a temporary table. */
    public static class AutoTempDriver
        extends net.hydromatic.optiq.jdbc.Driver
    {
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
                    ResultSink resultSink)
                {
                    super.onStatementExecute(statement, resultSink);
                    results.add(resultSink);
                }
            };
        }
    }
}

// End JdbcTest.java
