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
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.ParameterExpression;
import net.hydromatic.linq4j.expressions.Types;
import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.Predicate1;

import net.hydromatic.optiq.Function;
import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Parameter;
import net.hydromatic.optiq.SchemaObject;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.java.MapSchema;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.eigenbase.reltype.RelDataType;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
     *
     * @throws Exception on error
     */
    public void testSelect() throws Exception {
        Connection connection = getConnectionWithHrFoodmart();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select *\n"
                + "from \"foodmart\".\"sales_fact_1997\" as s\n"
                + "where s.\"cust_id\" = 100");
        String actual = toString(resultSet);
        resultSet.close();
        statement.close();
        connection.close();

        assertEquals(
            "cust_id=100; prod_id=10\n",
            actual);
    }

    /**
     * Runs a simple query that joins between two in-memory schemas.
     *
     * @throws Exception on error
     */
    public void testJoin() throws Exception {
        Connection connection = getConnectionWithHrFoodmart();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select *\n"
                + "from \"foodmart\".\"sales_fact_1997\" as s\n"
                + "join \"hr\".\"emps\" as e\n"
                + "on e.\"empid\" = s.\"cust_id\"");
        String actual = toString(resultSet);
        resultSet.close();
        statement.close();
        connection.close();

        assertEquals(
            "cust_id=100; prod_id=10; empid=100; deptno=10; name=Bill\n"
            + "cust_id=150; prod_id=20; empid=150; deptno=10; name=Sebastian\n",
            actual);
    }

    /**
     * Simple GROUP BY.
     *
     * @throws Exception on error
     */
    public void testGroupBy() throws Exception {
        Connection connection = getConnectionWithHrFoodmart();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select \"deptno\", sum(\"empid\"), count(*)\n"
                + "from \"hr\".\"emps\" as e\n"
                + "group by \"deptno\"");
        String actual = toString(resultSet);
        resultSet.close();
        statement.close();
        connection.close();

        assertEquals(
            "cust_id=100; prod_id=10; empid=100; name=Bill\n"
            + "cust_id=150; prod_id=20; empid=150; name=Sebastian\n",
            actual);
    }

    private String toString(ResultSet resultSet) throws SQLException {
        StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            int n = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= n; i++) {
                buf.append(
                    i > 1
                        ? "; "
                        : "")
                    .append(resultSet.getMetaData().getColumnLabel(i))
                    .append("=")
                    .append(resultSet.getObject(i));
            }
            buf.append("\n");
        }
        return buf.toString();
    }

    public void testWhereBad() throws Exception {
        assertQueryThrows(
            "select *\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "where empid > 120",
            "Column 'EMPID' not found in any table");
    }

    public void _testWhere() throws Exception {
        assertQueryReturns(
            "select *\n"
            + "from \"hr\".\"emps\" as e\n"
            + "where e.\"empid\" > 120 and e.\"name\" like 'B%'",
            "cust_id=100; prod_id=10; empid=100; name=Bill\n"
            + "cust_id=150; prod_id=20; empid=150; name=Sebastian\n");
    }

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
        MapSchema schema = new MapSchema(typeFactory);
        schema.add(
            ReflectiveSchema.methodFunction(
                null,
                GENERATE_STRINGS_METHOD,
                typeFactory));
        optiqConnection.getRootSchema().add("s", schema, null);
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
        return new Extensions.AbstractQueryable2<IntString>(
            IntString.class,
            null,
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
        MapSchema schema = new MapSchema(typeFactory);
        schema.add(
            ReflectiveSchema.methodFunction(
                null,
                GENERATE_STRINGS_METHOD,
                typeFactory));
        schema.add(
            ReflectiveSchema.methodFunction(
                null,
                STRING_UNION_METHOD,
                typeFactory));
        optiqConnection.getRootSchema().add("s", schema, null);
        optiqConnection.getRootSchema().add("hr", new HrSchema());
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
        MapSchema schema = new MapSchema(typeFactory);
        schema.add(
            viewFunction(
                typeFactory,
                "emps_view",
                "select * from \"hr\".\"emps\""));
        MutableSchema rootSchema = optiqConnection.getRootSchema();
        rootSchema.add("s", schema, schema.getInstanceMap());
        rootSchema.add("hr", new HrSchema());
        ResultSet resultSet = connection.createStatement().executeQuery(
            "select *\n"
            + "from \"emps_view\"");
        assertTrue(resultSet.next());
    }

    private SchemaObject viewFunction(
        JavaTypeFactory typeFactory, final String name, String viewSql)
    {
        return new Function() {
            public List<Parameter> getParameters() {
                return Collections.emptyList();
            }

            public RelDataType getType() {
                // TODO: return type returned from validation
                throw new UnsupportedOperationException();
            }

            public Object evaluate(List<Object> arguments) {
                // TODO: return a Queryable that wraps the parse tree
                throw new UnsupportedOperationException();
            }

            public String getName() {
                return name;
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
            for (int i = 1; i <= n; i++) {
                buf.append(
                    (i > 1 ? "; " : "")
                    + resultSet.getMetaData().getColumnLabel(i)
                    + "="
                    + resultSet.getObject(i));
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
        optiqConnection.getRootSchema().add("hr", new HrSchema());
        optiqConnection.getRootSchema().add("foodmart", new FoodmartSchema());
        return connection;
    }

    public static class HrSchema {
        public final Employee[] emps = {
            new Employee(100, 10, "Bill"),
            new Employee(200, 20, "Eric"),
            new Employee(150, 10, "Sebastian"),
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

    public static class FoodmartSchema {
        public final SalesFact[] sales_fact_1997 = {
            new SalesFact(100, 10),
            new SalesFact(150, 20),
        };
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
