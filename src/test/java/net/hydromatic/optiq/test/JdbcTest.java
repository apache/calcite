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
import net.hydromatic.optiq.impl.clone.CloneSchema;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.java.MapSchema;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.impl.jdbc.JdbcSchema;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.prepare.Factory;

import junit.framework.TestCase;

import org.apache.commons.dbcp.BasicDataSource;

import org.eigenbase.oj.stmt.OJPreparingStmt;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlDialect;
import org.eigenbase.util.Util;

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
        JavaTypeFactory typeFactory = optiqConnection.getTypeFactory();
        MutableSchema rootSchema = optiqConnection.getRootSchema();
        MapSchema schema = MapSchema.create(optiqConnection, rootSchema, "s");
        schema.addTableFunction(
            "emps_view",
            viewFunction(
                schema,
                typeFactory,
                "emps_view",
                "select * from \"hr\".\"emps\" where \"deptno\" = 10"));
        ReflectiveSchema.create(
            optiqConnection, rootSchema, "hr", new HrSchema());
        ResultSet resultSet = connection.createStatement().executeQuery(
            "select *\n"
            + "from \"s\".\"emps_view\"\n"
            + "where \"empid\" < 120");
        assertEquals("empid=100; deptno=10; name=Bill\n", toString(resultSet));
    }

    private <T> TableFunction<T> viewFunction(
        final Schema schema,
        final JavaTypeFactory typeFactory,
        final String name,
        final String viewSql)
    {
        final OptiqConnection optiqConnection =
            (OptiqConnection) schema.getQueryProvider();
        return new TableFunction<T>() {
            public List<Parameter> getParameters() {
                return Collections.emptyList();
            }

            public Table<T> apply(List<Object> arguments) {
                OptiqPrepare.ParseResult parsed =
                    Factory.implement().parse(
                        new OptiqPrepare.Context() {
                            public JavaTypeFactory getTypeFactory() {
                                return typeFactory;
                            }

                            public Schema getRootSchema() {
                                return optiqConnection.getRootSchema();
                            }

                            public List<String> getDefaultSchemaPath() {
                                return Collections.emptyList();
                            }
                        },
                        viewSql);
                return new ViewTable<T>(
                    schema,
                    typeFactory.getJavaClass(parsed.rowType),
                    parsed.rowType,
                    name,
                    viewSql);
            }

            public Type getElementType() {
                return apply(Collections.emptyList()).getElementType();
            }
        };
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
        "c0=1997; m0=266773\n",
        "select \"time_by_day\".\"the_year\" as \"c0\", \"promotion\".\"media_type\" as \"c1\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"promotion\" as \"promotion\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\" group by \"time_by_day\".\"the_year\", \"promotion\".\"media_type\"",
        "c0=1997; c1=Bulk Mail; m0=4320\n"
        + "c0=1997; c1=Radio; m0=2454\n"
        + "c0=1997; c1=Street Handout; m0=5753\n"
        + "c0=1997; c1=TV; m0=3607\n"
        + "c0=1997; c1=No Media; m0=195448\n"
        + "c0=1997; c1=In-Store Coupon; m0=3798\n"
        + "c0=1997; c1=Sunday Paper, Radio, TV; m0=2726\n"
        + "c0=1997; c1=Product Attachment; m0=7544\n"
        + "c0=1997; c1=Daily Paper; m0=7738\n"
        + "c0=1997; c1=Cash Register Handout; m0=6697\n"
        + "c0=1997; c1=Daily Paper, Radio; m0=6891\n"
        + "c0=1997; c1=Daily Paper, Radio, TV; m0=9513\n"
        + "c0=1997; c1=Sunday Paper, Radio; m0=5945\n"
        + "c0=1997; c1=Sunday Paper; m0=4339\n",
    };

    /** A selection of queries generated by Mondrian. */
    public void testCloneQueries() {
        OptiqAssert.AssertThat with =
            OptiqAssert.assertThat()
                .with(OptiqAssert.Config.FOODMART_CLONE);
        for (int i = 0; i < queries.length; i++) {
            String query = queries[i];
            try {
                if (i + 1 < queries.length
                    && queries[i + 1] != null
                    && !queries[i + 1].startsWith("select"))
                {
                    String expected = queries[++i];
                    with.query(query).returns(expected);
                } else {
                    with.query(query).runs();
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

    public void testValuesComposite() {
        OptiqAssert.assertThat()
            .query("values (1, 'a'), (2, 'abc')")
            .returns(
                "EXPR$0=1; EXPR$1=a  \n"
                + "EXPR$0=2; EXPR$1=abc\n");
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
                "c0=1997; c1=Drink; c2=USA; c3=WA; c4=Sedro Woolley; m0=58\n");
    }

    /** Tests the TABLES table in the information schema. */
    public void testMetaTables() {
        OptiqAssert.assertThat()
            .with(OptiqAssert.Config.REGULAR_PLUS_METADATA)
            .query("select * from \"metadata\".TABLES")
            .returns(
                OptiqAssert.checkResultContains(
                    "tableSchem=metadata; tableName=COLUMNS; tableType=null; "));

        OptiqAssert.assertThat()
            .with(OptiqAssert.Config.REGULAR_PLUS_METADATA)
            .query(
                "select count(distinct \"tableSchem\") as c\n"
                + "from \"metadata\".TABLES")
            .returns("C=3\n");
    }

    public static class HrSchema {
        public final Employee[] emps = {
            new Employee(100, 10, "Bill"),
            new Employee(200, 20, "Eric"),
            new Employee(150, 10, "Sebastian"),
        };
        public final Department[] depts = {
            new Department(10, "Sales"),
            new Department(30, "Marketing"),
            new Department(40, "HR"),
        };
    }

    public static class Employee {
        public final int empid;
        public final int deptno;
        public final String name;

        public Employee(int empid, int deptno, String name) {
            this.empid = empid;
            this.deptno = deptno;
            this.name = name;
        }
    }

    public static class Department {
        public final int deptno;
        public final String name;

        public Department(int deptno, String name) {
            this.deptno = deptno;
            this.name = name;
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

    public static abstract class AbstractTable<T>
        extends AbstractQueryable<T>
        implements Table<T>
    {
        protected final Type elementType;
        private final RelDataType relDataType;
        protected final Schema schema;
        protected final String tableName;

        protected AbstractTable(
            Schema schema,
            Type elementType,
            RelDataType relDataType,
            String tableName)
        {
            this.schema = schema;
            this.elementType = elementType;
            this.relDataType = relDataType;
            this.tableName = tableName;
            assert schema != null;
            assert relDataType != null;
            assert elementType != null;
            assert tableName != null;
        }

        public QueryProvider getProvider() {
            return schema.getQueryProvider();
        }

        public DataContext getDataContext() {
            return schema;
        }

        public Type getElementType() {
            return elementType;
        }

        public RelDataType getRowType() {
            return relDataType;
        }

        public Expression getExpression() {
            return Expressions.call(
                schema.getExpression(),
                "getTable",
                Expressions.<Expression>list()
                    .append(Expressions.constant(tableName))
                    .appendIf(
                        elementType instanceof Class,
                        Expressions.constant(elementType)));
        }

        public Iterator<T> iterator() {
            return Linq4j.enumeratorIterator(enumerator());
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
    }

    static class ViewTable<T>
        extends AbstractTable<T>
        implements TranslatableTable<T>
    {
        private final String viewSql;

        protected ViewTable(
            Schema schema,
            Type elementType,
            RelDataType relDataType,
            String tableName,
            String viewSql)
        {
            super(schema, elementType, relDataType, tableName);
            this.viewSql = viewSql;
        }

        public Enumerator<T> enumerator() {
            return schema
                .getQueryProvider()
                .<T>createQuery(getExpression(), elementType)
                .enumerator();
        }

        public RelNode toRel(
            RelOptTable.ToRelContext context,
            RelOptTable relOptTable)
        {
            return expandView(
                context.getPreparingStmt(),
                ((JavaTypeFactory) context.getCluster().getTypeFactory())
                    .createType(elementType),
                viewSql);
        }

        private RelNode expandView(
            OJPreparingStmt preparingStmt,
            RelDataType rowType,
            String queryString)
        {
            try {
                RelNode rel =
                    preparingStmt.expandView(rowType, queryString);

                rel = RelOptUtil.createCastRel(rel, rowType, true);
                rel = preparingStmt.flattenTypes(rel, false);
                return rel;
            } catch (Throwable e) {
                throw Util.newInternal(
                    e, "Error while parsing view definition:  " + queryString);
            }
        }
    }
}

// End JdbcTest.java
