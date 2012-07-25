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
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.java.MapSchema;
import net.hydromatic.optiq.impl.jdbc.JdbcQueryProvider;
import net.hydromatic.optiq.impl.jdbc.JdbcSchema;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.dbcp.BasicDataSource;

import org.eigenbase.sql.SqlDialect;
import org.eigenbase.util.Util;

import java.io.PrintWriter;
import java.io.StringWriter;
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

    /**
     * Runs a simple query that reads from a table in an in-memory schema.
     */
    public void testSelect() {
        assertQuery(
            "select *\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "where s.\"cust_id\" = 100").returns(
                "cust_id=100; prod_id=10\n");
    }

    /**
     * Runs a simple query that joins between two in-memory schemas.
     */
    public void testJoin() {
        assertQuery(
            "select *\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "join \"hr\".\"emps\" as e\n"
            + "on e.\"empid\" = s.\"cust_id\"").returns(
                "cust_id=100; prod_id=10; empid=100; deptno=10; name=Bill\n"
                + "cust_id=150; prod_id=20; empid=150; deptno=10; name=Sebastian\n");
    }

    /**
     * Simple GROUP BY.
     */
    public void testGroupBy() {
        assertQuery(
            "select \"deptno\", sum(\"empid\") as s, count(*) as c\n"
            + "from \"hr\".\"emps\" as e\n"
            + "group by \"deptno\"").returns(
                "deptno=20; S=200; C=1\n"
                + "deptno=10; S=250; C=2\n");
    }

    /**
     * Simple ORDER BY.
     */
    public void testOrderBy() {
        assertQuery(
            "select upper(\"name\") as un, \"deptno\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "order by \"deptno\", \"name\" desc").returns(
                "UN=SEBASTIAN; deptno=10\n"
                + "UN=BILL; deptno=10\n"
                + "UN=ERIC; deptno=20\n");
    }

    /**
     * Simple UNION, plus ORDER BY.
     *
     * <p>Also tests a query that returns a single column. We optimize this case
     * internally, using non-array representations for rows.</p>
     */
    public void testUnionAllOrderBy() {
        assertQuery(
            "select \"name\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "union all\n"
            + "select \"name\"\n"
            + "from \"hr\".\"depts\"\n"
            + "order by 1 desc").returns(
                "name=Sebastian\n"
                + "name=Sales\n"
                + "name=Marketing\n"
                + "name=HR\n"
                + "name=Eric\n"
                + "name=Bill\n");
    }

    /**
     * Tests UNION.
     */
    public void testUnion() {
        assertQuery(
            "select substring(\"name\" from 1 for 1) as x\n"
            + "from \"hr\".\"emps\" as e\n"
            + "union\n"
            + "select substring(\"name\" from 1 for 1) as y\n"
            + "from \"hr\".\"depts\"").returns(
                "X=E\n"
                + "X=S\n"
                + "X=B\n"
                + "X=M\n"
                + "X=H\n");
    }

    /**
     * Tests INTERSECT.
     */
    public void testIntersect() {
        assertQuery(
            "select substring(\"name\" from 1 for 1) as x\n"
            + "from \"hr\".\"emps\" as e\n"
            + "intersect\n"
            + "select substring(\"name\" from 1 for 1) as y\n"
            + "from \"hr\".\"depts\"").returns(
                "X=S\n");
    }

    /**
     * Tests EXCEPT.
     */
    public void testExcept() {
        assertQuery(
            "select substring(\"name\" from 1 for 1) as x\n"
            + "from \"hr\".\"emps\" as e\n"
            + "except\n"
            + "select substring(\"name\" from 1 for 1) as y\n"
            + "from \"hr\".\"depts\"").returns(
                "X=E\n"
                + "X=B\n");
    }

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

    public void testWhereBad() {
        assertQuery(
            "select *\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "where empid > 120")
            .throwz("Column 'EMPID' not found in any table");
    }

    public void _testWhere() {
        assertQuery(
            "select *\n"
            + "from \"hr\".\"emps\" as e\n"
            + "where e.\"empid\" > 120 and e.\"name\" like 'B%'").returns(
                "cust_id=100; prod_id=10; empid=100; name=Bill\n"
                + "cust_id=150; prod_id=20; empid=150; name=Sebastian\n");
    }

    /**
     * Test that uses a JDBC connection as a linq4j {@link QueryProvider}.
     *
     * @throws Exception on error
     */
    public void testQueryProvider() throws Exception {
        Connection connection = getConnectionWithHrFoodmart();
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
        Connection connection = getConnectionWithHrFoodmart();
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
                        Expressions.new_(
                            AnInt.class,
                            Arrays.<Expression>asList(
                                Expressions.field(
                                    e, "empid"))),
                        Arrays.asList(e)))
                .toList();
        assertEquals(Arrays.asList(100, 200, 150), list);
    }

    /**
     * Tests a relation that is accessed via method syntax.
     * The function returns a {@link Queryable}.
     */
    public void testFunction() throws SQLException, ClassNotFoundException {
        Class.forName("net.hydromatic.optiq.jdbc.Driver");
        Connection connection =
            DriverManager.getConnection("jdbc:optiq:");
        OptiqConnection optiqConnection =
            connection.unwrap(OptiqConnection.class);
        JavaTypeFactory typeFactory = optiqConnection.getTypeFactory();
        MapSchema schema =
            new MapSchema(optiqConnection, typeFactory, null);
        schema.addTableFunction(
            "GenerateStrings",
            Schemas.methodMember(
                GENERATE_STRINGS_METHOD, typeFactory));
        optiqConnection.getRootSchema().addSchema("s", schema);
        ResultSet resultSet = connection.createStatement().executeQuery(
            "select *\n"
            + "from table(s.GenerateStrings(5))\n"
            + "where char_length(s) > 3");
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
    public void testOperator() throws SQLException, ClassNotFoundException {
        Class.forName("net.hydromatic.optiq.jdbc.Driver");
        Connection connection =
            DriverManager.getConnection("jdbc:optiq:");
        OptiqConnection optiqConnection =
            connection.unwrap(OptiqConnection.class);
        JavaTypeFactory typeFactory = optiqConnection.getTypeFactory();
        MapSchema schema =
            new MapSchema(optiqConnection, typeFactory, null);
        schema.addTableFunction(
            "GenerateStrings",
            Schemas.methodMember(
                GENERATE_STRINGS_METHOD, typeFactory));
        schema.addTableFunction(
            "StringUnion",
            Schemas.methodMember(
                STRING_UNION_METHOD, typeFactory));
        MutableSchema rootSchema = optiqConnection.getRootSchema();
        rootSchema.addSchema("s", schema);
        rootSchema.addReflectiveSchema("hr", new HrSchema());
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
        MapSchema schema =
            new MapSchema(optiqConnection, typeFactory, null);
        schema.addTableFunction(
            "emps_view",
            viewFunction(
                typeFactory, "emps_view", "select * from \"hr\".\"emps\""));
        MutableSchema rootSchema = optiqConnection.getRootSchema();
        rootSchema.addSchema("s", schema);
        rootSchema.addReflectiveSchema("hr", new HrSchema());
        ResultSet resultSet = connection.createStatement().executeQuery(
            "select *\n"
            + "from \"emps_view\"");
        assertTrue(resultSet.next());
    }

    private <T> TableFunction<T> viewFunction(
        JavaTypeFactory typeFactory, final String name, String viewSql)
    {
        return new TableFunction<T>() {
            public List<Parameter> getParameters() {
                return Collections.emptyList();
            }

            public Type getElementType() {
                // TODO: return type returned from validation
                throw new UnsupportedOperationException();
            }

            public Table<T> apply(List<Object> arguments) {
                // TODO: return a Queryable that wraps the parse tree
                throw new UnsupportedOperationException();
            }
        };
    }

    private Function1<String, Void> checkResult(final String expected) {
        return new Function1<String, Void>() {
            public Void apply(String p0) {
                Assert.assertEquals(expected, p0);
                return null;
            }
        };
    }

    private Function1<Exception, Void> checkException(final String expected) {
        return new Function1<Exception, Void>() {
            public Void apply(Exception p0) {
                StringWriter stringWriter = new StringWriter();
                PrintWriter printWriter = new PrintWriter(stringWriter);
                p0.printStackTrace(printWriter);
                printWriter.flush();
                String stack = stringWriter.toString();
                Assert.assertTrue(stack, stack.contains(expected));
                return null;
            }
        };
    }

    private void assertQueryReturns(String sql, String expected)
        throws Exception
    {
        assertQuery(sql, checkResult(expected), null);
    }

    private void assertQueryThrows(String sql, String message)
        throws Exception
    {
        assertQuery(sql, null, checkException(message));
    }

    private void assertQuery(
        String sql,
        Function1<String, Void> resultChecker,
        Function1<Exception, Void> exceptionChecker)
        throws Exception
    {
        Connection connection = getConnectionWithHrFoodmart();
        Statement statement = connection.createStatement();
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
        }
        StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            int n = resultSet.getMetaData().getColumnCount();
            for (int i = 1;; i++) {
                buf.append(resultSet.getMetaData().getColumnLabel(i))
                    .append("=")
                    .append(resultSet.getObject(i));
                if (i == n) {
                    break;
                }
                buf.append("; ");
            }
            buf.append("\n");
        }
        resultSet.close();
        statement.close();
        connection.close();

        if (resultChecker != null) {
            resultChecker.apply(buf.toString());
        }
    }

    private Connection getConnectionWithHrFoodmart()
        throws ClassNotFoundException, SQLException
    {
        Class.forName("net.hydromatic.optiq.jdbc.Driver");
        Connection connection =
            DriverManager.getConnection("jdbc:optiq:");
        OptiqConnection optiqConnection =
            connection.unwrap(OptiqConnection.class);
        MutableSchema rootSchema = optiqConnection.getRootSchema();
        rootSchema.addReflectiveSchema("hr", new HrSchema());
        rootSchema.addReflectiveSchema("foodmart", new FoodmartSchema());
        return connection;
    }

    private OptiqConnection getConnection(QueryProvider queryProvider)
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

        // FIXME: Sub-schema should not need to build its own expression.
        final Expression expression =
            Expressions.call(
                optiqConnection.getRootSchema().getExpression(),
                "getSubSchema",
                Expressions.constant("foodmart"));

        optiqConnection.getRootSchema().addSchema(
            "foodmart",
            new JdbcSchema(
                queryProvider == null ? optiqConnection : queryProvider,
                dataSource,
                JdbcSchema.createDialect(dataSource),
                "foodmart",
                "",
                optiqConnection.getTypeFactory(),
                expression));
        return optiqConnection;
    }

    public void testFoodMartJdbc() {
        assertQuery("select * from \"foodmart\".\"days\"")
            .inJdbcFoodmart()
            .returns(
                "day=1; week_day=Sunday\n" + "day=2; week_day=Monday\n"
                + "day=5; week_day=Thursday\n" + "day=4; week_day=Wednesday\n"
                + "day=3; week_day=Tuesday\n" + "day=6; week_day=Friday\n"
                + "day=7; week_day=Saturday\n");
    }

    public void testFoodMartJdbcWhere() {
        assertQuery("select * from \"foodmart\".\"days\" where \"day\" < 3")
            .inJdbcFoodmart()
            .returns(
                "day=1; week_day=Sunday\n"
                + "day=2; week_day=Monday\n");
    }

    public void testFoodMartJdbcWhere2() {
        assertQuery("select * from \"foodmart\".\"days\" where \"day\" < 3")
            .inJdbcFoodmart2()
            .returns(
                "day=1; week_day=Sunday\n"
                + "day=2; week_day=Monday\n");
    }

    public void testJdbcBackendLinqFrontend() {
        try {
            final OptiqConnection connection = getConnection(null);
            JdbcSchema schema =
                (JdbcSchema)
                connection.getRootSchema().getSubSchema("foodmart");
            ParameterExpression c =
                Expressions.parameter(
                    Customer.class, "c");
            String s =
            schema.getTable("customer", Customer.class)
                .where(
                    Expressions.<Predicate1<Customer>>lambda(
                        Expressions.lessThan(
                            Expressions.field(c, "customer_id"),
                            Expressions.constant(50)),
                        c))
                .toList()
                .toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testFoodMartJdbcGroup() {
        assertQuery(
            "select s, count(*) as c from (\n"
            + "select substring(\"week_day\" from 1 for 1) as s\n"
            + "from \"foodmart\".\"days\")\n"
            + "group by s")
            .inJdbcFoodmart()
            .returns(
                "S=T; C=2\n"
                + "S=F; C=1\n"
                + "S=W; C=1\n"
                + "S=S; C=2\n"
                + "S=M; C=1\n");
    }

    public void testFoodMartJdbcGroupEmpty() {
        assertQuery(
            "select count(*) as c\n"
            + "from \"foodmart\".\"days\"")
            .inJdbcFoodmart()
            .returns("C=7\n");
    }

    public void testFoodMartJdbcJoinGroupByEmpty() {
        assertQuery(
            "select count(*) from (\n"
            + "  select *\n"
            + "  from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "  join \"foodmart\".\"customer\" as c\n"
            + "  on s.\"customer_id\" = c.\"customer_id\")")
            .inJdbcFoodmart()
            .returns("EXPR$0=86837\n");
    }

    public void testFoodMartJdbcJoinGroupByOrderBy() {
        assertQuery(
            "select count(*), c.\"state_province\", sum(s.\"unit_sales\") as s\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "  join \"foodmart\".\"customer\" as c\n"
            + "  on s.\"customer_id\" = c.\"customer_id\"\n"
            + "group by c.\"state_province\"\n"
            + "order by c.\"state_province\"")
            .inJdbcFoodmart()
            .returns(
                "EXPR$0=24442; state_province=CA; S=74748.0000\n"
                + "EXPR$0=21611; state_province=OR; S=67659.0000\n"
                + "EXPR$0=40784; state_province=WA; S=124366.0000\n");
    }

    public void testFoodMartJdbcCompositeGroupBy() {
        assertQuery(
            "select count(*) as c, c.\"state_province\"\n"
            + "from \"foodmart\".\"customer\" as c\n"
            + "group by c.\"state_province\", c.\"country\"\n"
            + "order by c.\"state_province\", 1")
            .inJdbcFoodmart()
            .returns(
                "C=1717; state_province=BC\n"
                + "C=4222; state_province=CA\n"
                + "C=347; state_province=DF\n"
                + "C=106; state_province=Guerrero\n"
                + "C=104; state_province=Jalisco\n"
                + "C=97; state_province=Mexico\n"
                + "C=1051; state_province=OR\n"
                + "C=90; state_province=Oaxaca\n"
                + "C=78; state_province=Sinaloa\n"
                + "C=93; state_province=Veracruz\n"
                + "C=2086; state_province=WA\n"
                + "C=99; state_province=Yucatan\n"
                + "C=191; state_province=Zacatecas\n");
    }

    public void testFoodMartJdbcDistinctCount() {
        // Complicating factors:
        // Composite GROUP BY key
        // Order by select item, referenced by ordinal
        // Distinct count
        // Not all GROUP columns are projected
        assertQuery(
            "select c.\"state_province\",\n"
            + "  sum(s.\"unit_sales\") as s,\n"
            + "  count(distinct c.\"customer_id\") as dc\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "  join \"foodmart\".\"customer\" as c\n"
            + "  on s.\"customer_id\" = c.\"customer_id\"\n"
            + "group by c.\"state_province\", c.\"country\"\n"
            + "order by c.\"state_province\", 2")
            .inJdbcFoodmart()
            .returns(
                "state_province=CA; S=74748.0000; DC=24442\n"
                + "state_province=OR; S=67659.0000; DC=21611\n"
                + "state_province=WA; S=124366.0000; DC=40784\n");
    }

    public AssertQuery assertQuery(String s) {
        return new AssertQuery(s);
    }

    private class AssertQuery {
        private final String sql;
        private Config config = Config.REGULAR;

        AssertQuery(String sql) {
            super();
            this.sql = sql;
        }

        void returns(String expected) {
            try {
                Connection connection;
                Statement statement;
                ResultSet resultSet;

                switch (config) {
                case REGULAR:
                    connection = getConnectionWithHrFoodmart();
                    break;
                case JDBC_FOODMART2:
                    connection = getConnection(null);
                    break;
                case JDBC_FOODMART:
                    connection = getConnection(JdbcQueryProvider.INSTANCE);
                    break;
                default:
                    throw Util.unexpected(config);
                }
                statement = connection.createStatement();
                resultSet = statement.executeQuery(sql);
                String actual = JdbcTest.toString(resultSet);
                resultSet.close();
                statement.close();
                connection.close();

                assertEquals(expected, actual);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void throwz(String message) {
            try {
                assertQueryThrows(sql, message);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public AssertQuery inJdbcFoodmart() {
            config = Config.JDBC_FOODMART;
            return this;
        }

        public AssertQuery inJdbcFoodmart2() {
            config = Config.JDBC_FOODMART2;
            return this;
        }
    }


    private enum Config {
        REGULAR,
        JDBC_FOODMART,
        JDBC_FOODMART2,
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
}

// End JdbcTest.java
