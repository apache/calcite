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
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.util.Util;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Tests for a linq4j front-end and JDBC back-end.
 */
public class LinqFrontJdbcBackTest {
  @Test public void testTableWhere() throws SQLException,
      ClassNotFoundException {
    final Connection connection =
        CalciteAssert.that(CalciteAssert.Config.JDBC_FOODMART).connect();
    final CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    final SchemaPlus rootSchema = calciteConnection.getRootSchema();
    ParameterExpression c =
        Expressions.parameter(JdbcTest.Customer.class, "c");
    String s =
        Schemas.queryable(Schemas.createDataContext(connection, rootSchema),
            rootSchema.getSubSchema("foodmart"),
            JdbcTest.Customer.class, "customer")
            .where(
                Expressions.lambda(
                    Expressions.lessThan(
                        Expressions.field(c, "customer_id"),
                        Expressions.constant(5)),
                    c))
            .toList()
            .toString();
    Util.discard(s);
  }
}

// End LinqFrontJdbcBackTest.java
