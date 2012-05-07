package net.hydromatic.optiq.examples.foodmart.java;

import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import org.eigenbase.reltype.RelDataTypeFactory;

import java.sql.*;

/**
 * Example of using JLINQ via JDBC.
 *
 * <p>Schema is specified programmatically.</p>
 */
public class JdbcExample {
    public static void main(String[] args) throws Exception {
        new JdbcExample().run();
    }

    public void run() throws ClassNotFoundException, SQLException {
        Class.forName("net.hydromatic.optiq.jdbc.Driver");
        Connection connection =
            DriverManager.getConnection("jdbc:optiq:");
        OptiqConnection optiqConnection =
            connection.unwrap(OptiqConnection.class);
        RelDataTypeFactory typeFactory = optiqConnection.getTypeFactory();
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
        resultSet.close();
        statement.close();
        connection.close();
    }

    public static class HrSchema {
        public final Employee[] emps = {

        };
    }

    public static class Employee {
        public final int empid;

        public Employee(int empid, String name) {
            this.empid = empid;
        }
    }

    public static class FoodmartSchema {
        public final SalesFact[] sales_fact_1997 = {

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

// End JdbcExample.java
