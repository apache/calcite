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

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.impl.java.MapSchema;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import junit.framework.TestCase;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static net.hydromatic.optiq.test.OptiqAssert.assertThat;

/**
 * Tests for a JDBC front-end (with some quite complex SQL) and Linq4j back-end
 * (based on in-memory collections).
 *
 * @author jhyde
 */
public class JdbcFrontLinqBackTest extends TestCase {
    /**
     * Runs a simple query that reads from a table in an in-memory schema.
     */
    public void testSelect() {
        assertThat()
            .query(
                "select *\n"
                + "from \"foodmart\".\"sales_fact_1997\" as s\n"
                + "where s.\"cust_id\" = 100")
            .returns(
                "cust_id=100; prod_id=10\n");
    }

    /**
     * Runs a simple query that joins between two in-memory schemas.
     */
    public void testJoin() {
        assertThat()
            .query(
                "select *\n"
                + "from \"foodmart\".\"sales_fact_1997\" as s\n"
                + "join \"hr\".\"emps\" as e\n"
                + "on e.\"empid\" = s.\"cust_id\"")
            .returns(
                "cust_id=100; prod_id=10; empid=100; deptno=10; name=Bill; commission=1000\n"
                + "cust_id=150; prod_id=20; empid=150; deptno=10; name=Sebastian; commission=null\n");
    }

    /**
     * Simple GROUP BY.
     */
    public void testGroupBy() {
        assertThat()
            .query(
                "select \"deptno\", sum(\"empid\") as s, count(*) as c\n"
                + "from \"hr\".\"emps\" as e\n"
                + "group by \"deptno\"")
            .returns(
                "deptno=20; S=200; C=1\n"
                + "deptno=10; S=250; C=2\n");
    }

    /**
     * Simple ORDER BY.
     */
    public void testOrderBy() {
        assertThat()
            .query(
                "select upper(\"name\") as un, \"deptno\"\n"
                + "from \"hr\".\"emps\" as e\n"
                + "order by \"deptno\", \"name\" desc")
            .returns(
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
        assertThat()
            .query(
                "select \"name\"\n"
                + "from \"hr\".\"emps\" as e\n"
                + "union all\n"
                + "select \"name\"\n"
                + "from \"hr\".\"depts\"\n"
                + "order by 1 desc")
            .returns(
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
        assertThat()
            .query(
                "select substring(\"name\" from 1 for 1) as x\n"
                + "from \"hr\".\"emps\" as e\n"
                + "union\n"
                + "select substring(\"name\" from 1 for 1) as y\n"
                + "from \"hr\".\"depts\"")
            .returns(
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
        assertThat()
            .query(
                "select substring(\"name\" from 1 for 1) as x\n"
                + "from \"hr\".\"emps\" as e\n"
                + "intersect\n"
                + "select substring(\"name\" from 1 for 1) as y\n"
                + "from \"hr\".\"depts\"")
            .returns(
                "X=S\n");
    }

    /**
     * Tests EXCEPT.
     */
    public void testExcept() {
        assertThat()
            .query(
                "select substring(\"name\" from 1 for 1) as x\n"
                + "from \"hr\".\"emps\" as e\n"
                + "except\n"
                + "select substring(\"name\" from 1 for 1) as y\n"
                + "from \"hr\".\"depts\"")
            .returns(
                "X=E\n"
                + "X=B\n");
    }

    public void testWhereBad() {
        assertThat()
            .query(
                "select *\n"
                + "from \"foodmart\".\"sales_fact_1997\" as s\n"
                + "where empid > 120")
            .throws_("Column 'EMPID' not found in any table");
    }

    /** Test case for https://github.com/julianhyde/optiq/issues/9. */
    public void testWhereOr() {
        assertThat()
            .query(
                "select * from \"hr\".\"emps\"\n"
                + "where (\"empid\" = 100 or \"empid\" = 200)\n"
                + "and \"deptno\" = 10")
            .returns("empid=100; deptno=10; name=Bill; commission=1000\n");
    }

    public void testWhereLike() {
        if (false)
         // TODO: fix current error "Operands E.name, 'B%' not comparable to
         // each other"
        assertThat()
            .query(
                "select *\n"
                + "from \"hr\".\"emps\" as e\n"
                + "where e.\"empid\" > 120 and e.\"name\" like 'B%'")
            .returns(
                "cust_id=100; prod_id=10; empid=100; name=Bill\n"
                + "cust_id=150; prod_id=20; empid=150; name=Sebastian\n");
    }

    public void testInsert() {
        final List<JdbcTest.Employee> employees =
            new ArrayList<JdbcTest.Employee>();
        OptiqAssert.AssertThat with = mutable(employees);
        with.query("select * from \"foo\".\"bar\"")
            .returns("empid=0; deptno=0; name=first; commission=null\n");
        with.query("insert into \"foo\".\"bar\" select * from \"hr\".\"emps\"")
            .returns("ROWCOUNT=3\n");
        with.query("select count(*) as c from \"foo\".\"bar\"")
            .returns("C=4\n");
        with.query(
            "insert into \"foo\".\"bar\" "
            + "select * from \"hr\".\"emps\" where \"deptno\" = 10")
            .returns("ROWCOUNT=2\n");
        with.query(
            "select \"name\", count(*) as c from \"foo\".\"bar\" "
            + "group by \"name\"")
            .returns(
                "name=Bill; C=2\n"
                + "name=Eric; C=1\n"
                + "name=first; C=1\n"
                + "name=Sebastian; C=2\n");
    }

    private OptiqAssert.AssertThat mutable(
        final List<JdbcTest.Employee> employees)
    {
        employees.add(new JdbcTest.Employee(0, 0, "first", null));
        return assertThat()
            .with(
                new OptiqAssert.ConnectionFactory() {
                    public OptiqConnection createConnection() throws Exception {
                        final Connection connection =
                            JdbcTest.getConnection("hr", "foodmart");
                        OptiqConnection optiqConnection = connection.unwrap(
                            OptiqConnection.class);
                        MutableSchema rootSchema =
                            optiqConnection.getRootSchema();
                        MapSchema mapSchema = MapSchema.create(
                            optiqConnection, rootSchema, "foo");
                        final String tableName = "bar";
                        final JdbcTest.AbstractModifiableTable table =
                            new JdbcTest.AbstractModifiableTable(
                                mapSchema,
                                JdbcTest.Employee.class,
                                optiqConnection.getTypeFactory().createType(
                                    JdbcTest.Employee.class),
                                tableName)
                            {
                                public Enumerator enumerator() {
                                    return Linq4j.enumerator(employees);
                                }

                                public Collection getModifiableCollection() {
                                    return employees;
                                }
                            };
                        mapSchema.addTable(
                            new TableInSchemaImpl(
                                mapSchema, tableName, Schema.TableType.TABLE,
                                table));
                        return optiqConnection;
                    }
                });
    }

    public void testInsert2() {
        final List<JdbcTest.Employee> employees =
            new ArrayList<JdbcTest.Employee>();
        OptiqAssert.AssertThat with = mutable(employees);
        with.query("insert into \"foo\".\"bar\" values (1, 1, 'second', 2)")
            .returns("ROWCOUNT=1\n");
        with.query(
            "insert into \"foo\".\"bar\"\n"
            + "values (1, 3, 'third', 3), (1, 4, 'fourth', 4), (1, 5, 'fifth ', 3)")
            .returns("ROWCOUNT=3\n");
        with.query("select count(*) as c from \"foo\".\"bar\"")
            .returns("C=5\n");
        with.query("insert into \"foo\".\"bar\" values (1, 6, null, null)")
            .returns("ROWCOUNT=1\n");
        with.query("select count(*) as c from \"foo\".\"bar\"")
            .returns("C=6\n");
    }

    /** Some of the rows have the wrong number of columns. */
    public void testInsertMultipleRowMismatch() {
        final List<JdbcTest.Employee> employees =
            new ArrayList<JdbcTest.Employee>();
        OptiqAssert.AssertThat with = mutable(employees);
        with.query(
            "insert into \"foo\".\"bar\" values\n"
            + " (1, 3, 'third'),\n"
            + " (1, 4, 'fourth'),\n"
            + " (1, 5, 'fifth ', 3)")
            .throws_("Incompatible types");
    }
}

// End JdbcFrontLinqBackTest.java
