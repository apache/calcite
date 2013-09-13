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

import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertTrue;

/**
 * Tests for using Optiq with Spark as an internal engine.
 */
public class SparkTest {
  /**
   * Tests a VALUES query evaluated using Spark.
   * There are no data sources.
   */
  @Test public void testValues() throws SQLException, ClassNotFoundException {
    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    Connection connection =
        DriverManager.getConnection("jdbc:optiq:spark=true");
    ResultSet resultSet = connection.createStatement().executeQuery(
        "select *\n"
        + "from (values (1, 'a'), (2, 'b'))");
    assertTrue(resultSet.next());
  }
}

// End SparkTest.java
