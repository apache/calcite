/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.examples;

import kotlin.test.Asserter;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MergeJoinTest {
  public static void main(String[] args) throws Exception {
    new MergeJoinTest().run();
  }

  public void run() throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("hr", new ReflectiveSchema(new Hr()));
    rootSchema.add("foodmart", new ReflectiveSchema(new Foodmart()));
    rootSchema.add("customers", new ReflectiveSchema(new Customers()));
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("select \"S\".\"cust_id\"\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "left join \"hr\".\"emps\" as e\n"
            + "on e.\"empid\" = s.\"cust_id\""
            + "join \"customers\".\"customers\" as f\n"
            + "on e.\"empid\" = f.\"id\"");
    int[] ids = new int[3];
    int rowNo = 0;
    while (resultSet.next()) {
      int n = resultSet.getMetaData().getColumnCount();
      for (int i = 1; i <= n; i++) {
        ids[rowNo++] = resultSet.getInt(i);
      }
    }
    final int[] r = {1, 2, 4};
    assertArrayEquals(ids, r);
    resultSet.close();
    statement.close();
    connection.close();
  }

  /**
   * Object that will be used via reflection to create the "hr" schema.
   */
  public static class Hr {
    public final Employee[] emps = {
        new Employee(1, "Bill"),
        new Employee(2, "Eric"),
        new Employee(3, "Sebastian"),
        new Employee(4, "Anneke"),
    };
  }

  /**
   * Object that will be used via reflection to create the "emps" table.
   */
  public static class Employee {
    public final int empid;
    public final String name;

    public Employee(int empid, String name) {
      this.empid = empid;
      this.name = name;
    }
  }

  /**
   * Object that will be used via reflection to create the "foodmart"
   * schema.
   */
  public static class Foodmart {
    public final SalesFact[] sales_fact_1997 = {
        new SalesFact(1, 10),
        new SalesFact(2, 20),
        new SalesFact(4, 30),
    };
  }


  /**
   * Object that will be used via reflection to create the
   * "sales_fact_1997" fact table.
   */
  public static class SalesFact {
    public final int cust_id;
    public final int prod_id;

    public SalesFact(int cust_id, int prod_id) {
      this.cust_id = cust_id;
      this.prod_id = prod_id;
    }
  }

  /**
   * Object that will be used via reflection to create the "customers"
   * schema.
   */
  public static class Customers {
    public final Customer[] customers = {
        new Customer(1, "Tzvetan"),
        new Customer(2, "Mary"),
        new Customer(3, "Patricio"),
        new Customer(4, "Lillian"),
    };
  }
  /**
   * Object that will be used via reflection to create the
   * "customers" table.
   */
  public static class Customer {
    public final int id;
    public final String name;

    public Customer(int id, String name) {
      this.id = id;
      this.name = name;
    }
  }
}
