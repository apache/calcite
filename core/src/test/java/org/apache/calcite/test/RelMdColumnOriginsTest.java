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
package org.apache.calcite.test;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.util.Smalls;

import com.google.common.collect.ImmutableMultiset;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test case for CALCITE-542. */
class RelMdColumnOriginsTest {
  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-542">[CALCITE-542]
   * Support for Aggregate with grouping sets in RelMdColumnOrigins</a>. */
  @Test void testQueryWithAggregateGroupingSets() throws Exception {
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);

    calciteConnection.getRootSchema().add("T1",
        new Smalls.SimpleTable());
    Statement statement = calciteConnection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("SELECT TABLE1.ID, TABLE2.ID FROM "
                + "(SELECT GROUPING(A) AS ID FROM T1 "
                + "GROUP BY ROLLUP(A,B)) TABLE1 "
                + "JOIN "
                + "(SELECT GROUPING(A) AS ID FROM T1 "
                + "GROUP BY ROLLUP(A,B)) TABLE2 "
                + "ON TABLE1.ID = TABLE2.ID");

    final String result1 = "ID=0; ID=0";
    final String result2 = "ID=1; ID=1";
    final ImmutableMultiset<String> expectedResult =
        ImmutableMultiset.<String>builder()
            .addCopies(result1, 25)
            .add(result2)
            .build();
    assertThat(CalciteAssert.toSet(resultSet), equalTo(expectedResult));

    final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertThat(resultSetMetaData.getColumnName(1), equalTo("ID"));
    assertThat(resultSetMetaData.getTableName(1), nullValue());
    assertThat(resultSetMetaData.getSchemaName(1), nullValue());
    assertThat(resultSetMetaData.getColumnName(2), equalTo("ID"));
    assertThat(resultSetMetaData.getTableName(2), nullValue());
    assertThat(resultSetMetaData.getSchemaName(2), nullValue());
    resultSet.close();
    statement.close();
    connection.close();
  }
}
