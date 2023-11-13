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

import org.apache.calcite.jdbc.CalciteConnection;

import java.net.URISyntaxException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class TestApp {
  public static void main(String[] args) throws URISyntaxException, ClassNotFoundException, SQLException {

    // Schema definition file("model-mongo.json")
    String absolutePath = "/Users/eunji/Documents/2023FallSemester/GraduateProject/ApacheCalcite/mongodb/src/test/resources/mongo-model.json";
    // /Users/eunji/Documents/2023FallSemester/GraduateProject/ApacheCalcite/mongodb/src/test/resources/mongo-model.json

    // Load Calcite jdbc driver
    Class.forName("org.apache.calcite.jdbc.Driver");


    //		 check Table -> outputs by schemaPattern
//		 "MONGOA": departments, employees, "MONGOB": DEPARTMENTS2, EMPLOYEES2, null: COLUMNS, TABLES
//		Connection connection3 = DriverManager.getConnection("jdbc:calcite:model=" + absolutePath);
//		DatabaseMetaData metaData3 = connection3.getMetaData();
//		ResultSet tables3 = metaData3.getTables(null, null, null, null);
//		System.out.println("TABLE-NAME");
//		while (tables3.next()) {
//			String tableName = tables3.getString("TABLE_NAME");
//			System.out.println(tableName);
//		}


    // Make a calcite connection of which model is "model-mongo.json"
    // and Execute queries
    System.out.println("-------------SCAN--------------");
    Scanner sc = new Scanner(System.in);
    System.out.println("Please write a query statement.");

    System.out.println("-------------READ INPUT QUERY--------------");
    String query = sc.nextLine();

    while (!query.equals("q")) {
      System.out.println("-------------CONNECTION WITH MODEL--------------");
      Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + absolutePath);

      System.out.println("-------------CONNECTION WITH CALCITE--------------");

      try (
          CalciteConnection calciteConnection = (CalciteConnection) connection;
      ) {
        System.out.println("-------------CREATE STATEMENT--------------");
        Statement statement = calciteConnection.createStatement();

        System.out.println("-------------EXECUTE QUERY--------------");
        ResultSet rs = statement.executeQuery(query);

        // Get the metadata of the result set
        System.out.println("-------------GET METADATA--------------");
        ResultSetMetaData rsmd = rs.getMetaData();
//				ResultSetMetaData rsmdRedis = rsRedis.getMetaData();

        System.out.println("-------------CREATE COLUMNS ARRAY--------------");
        List<String> columns = new ArrayList<String>(rsmd.getColumnCount());

        // Add the column names of the result set to a list
        System.out.println("\n=== Column names ===\n");
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
          // Print column names
          System.out.println(rsmd.getColumnName(i));
          columns.add(rsmd.getColumnName(i));
        }

        while (rs.next()) {
          for (String col : columns) {
            System.out.println("" + col + " : " + rs.getString(col));
          }
          System.out.println("---------");
        }

        System.out.println("\n\nPlease write a query statement.");
        query = sc.nextLine();
      } catch (SQLException e) {
        System.out.println(e);
        System.out.println("\n\nPlease correct the query statement.");
        query = sc.nextLine();
        continue;
      }
    }
  }
}

// select * from EMPLOYEES2 limit 2
// select * from EMPLOYEES2 where SALARY < 5000
// select * from EMPLOYEES2 WHERE EMP_NO IN (20, 30)

// X
// select * from EMPLOYEES2 WHERE FIRST_NAME LIKE "%inn"
// select * from EMPLOYEES2 WHERE FIRST_NAME LIKE FIRST_NAME
// select * from EMPLOYEES2 WHERE FIRST_NAME IN ("John", "Jane")
// select * from EMPLOYEES2 WHERE EMP_NO LIKE 10

// select * from DEPARTMENTS2 WHERE DEPT_NO IN (10, 20)
// select * from DEPARTMENTS2 WHERE DEPT_NAME="Sales"

// DEBUG WITH THIS!
//select * from DEPARTMENTS2 WHERE DEPT_NO LIKE 10
// select * from EMPLOYEES2 where FIRST_NAME like '%n%'
// select * from EMPLOYEES2 where LAST_NAME like '%oe'
// select * from DEPARTMENTS2 where DEPT_NAME like '%es'
// select * from DEPARTMENTS2 where DEPT_NAME like 'Sales'
