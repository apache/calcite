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
                        },
                        viewSql);
                return new ViewTable<T>(
                    typeFactory.getJavaClass(parsed.rowType),
                    schema,
                    name,
                    viewSql);
            }

            public Type getElementType() {
                return apply(Collections.emptyList()).getElementType();
            }
        };
    }

    static Connection getConnectionWithHrFoodmart()
        throws ClassNotFoundException, SQLException
    {
        Class.forName("net.hydromatic.optiq.jdbc.Driver");
        Connection connection =
            DriverManager.getConnection("jdbc:optiq:");
        OptiqConnection optiqConnection =
            connection.unwrap(OptiqConnection.class);
        MutableSchema rootSchema = optiqConnection.getRootSchema();
        ReflectiveSchema.create(
            optiqConnection, rootSchema, "hr", new HrSchema());
        ReflectiveSchema.create(
            optiqConnection, rootSchema, "foodmart", new FoodmartSchema());
        return connection;
    }

    /**
     * Creates a connection with a given query provider. If provider is null,
     * uses the connection as its own provider. The connection contains a
     * schema called "foodmart" backed by a JDBC connection to MySQL.
     *
     * @param queryProvider Query provider
     * @return Connection
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    static OptiqConnection getConnection(QueryProvider queryProvider)
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

        JdbcSchema.create(
            optiqConnection,
            optiqConnection.getRootSchema(),
            dataSource,
            "foodmart",
            "",
            "foodmart");
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
        protected final Schema schema;
        protected final String tableName;

        protected AbstractTable(
            Type elementType,
            Schema schema,
            String tableName)
        {
            this.elementType = elementType;
            this.schema = schema;
            this.tableName = tableName;
            assert elementType != null;
            assert schema != null;
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

    static class ViewTable<T>
        extends AbstractTable<T>
        implements TranslatableTable<T>
    {
        private final String viewSql;

        protected ViewTable(
            Type elementType,
            Schema schema,
            String tableName,
            String viewSql)
        {
            super(elementType, schema, tableName);
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
