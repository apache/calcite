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

import junit.framework.TestCase;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import java.sql.*;

/**
 * Tests for using Optiq via JDBC.
 *
 * @author jhyde
 */
public class JdbcTest extends TestCase {

    /**
     * Runs a simple query that joins between two in-memory schemas.
     *
     * @throws Exception on error
     */
    public void testJoin() throws Exception {
        Class.forName("net.hydromatic.optiq.jdbc.Driver");
        Connection connection =
            DriverManager.getConnection("jdbc:optiq:");
        OptiqConnection optiqConnection =
            connection.unwrap(OptiqConnection.class);
        JavaTypeFactory typeFactory = optiqConnection.getTypeFactory();
        optiqConnection.getRootSchema().add(
            "hr",
            new ReflectiveSchema(new HrSchema(), typeFactory));
        optiqConnection.getRootSchema().add(
            "foodmart",
            new ReflectiveSchema(new FoodmartSchema(), typeFactory));
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select *\n"
                + "from \"foodmart\".\"sales_fact_1997\" as s\n"
                + "join \"hr\".\"emps\" as e\n"
                + "on e.\"empid\" = s.\"cust_id\"");
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

        assertEquals(
            "cust_id=100; prod_id=10; empid=100; name=Bill\n"
            + "cust_id=150; prod_id=20; empid=150; name=Sebastian\n",
            buf.toString());
    }

    public void _testWhere() throws Exception {
        assertQueryReturns(
            "select *\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "where empid > 120",
            "cust_id=100; prod_id=10; empid=100; name=Bill\n"
            + "cust_id=150; prod_id=20; empid=150; name=Sebastian\n");
    }

    private void assertQueryReturns(String sql, String expected)
        throws ClassNotFoundException, SQLException
    {
        Class.forName("net.hydromatic.optiq.jdbc.Driver");
        Connection connection =
            DriverManager.getConnection("jdbc:optiq:");
        OptiqConnection optiqConnection =
            connection.unwrap(OptiqConnection.class);
        JavaTypeFactory typeFactory = optiqConnection.getTypeFactory();
        optiqConnection.getRootSchema().add(
            "hr",
            new ReflectiveSchema(new HrSchema(), typeFactory));
        optiqConnection.getRootSchema().add(
            "foodmart",
            new ReflectiveSchema(new FoodmartSchema(), typeFactory));
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(sql);
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

        assertEquals(expected, buf.toString());
    }

    public static class HrSchema {
        public final Employee[] emps = {
            new Employee(100, "Bill"),
            new Employee(200, "Eric"),
            new Employee(150, "Sebastian"),
        };
    }

    public static class Employee {
        public final int empid;
        public final String name;

        public Employee(int empid, String name) {
            this.empid = empid;
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
}

// End JdbcTest.java
