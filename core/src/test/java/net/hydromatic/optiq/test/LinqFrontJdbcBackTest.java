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
package net.hydromatic.optiq.test;

import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.ParameterExpression;
import net.hydromatic.linq4j.function.Predicate1;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Schemas;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import org.eigenbase.util.Util;

import org.junit.Test;

import java.sql.SQLException;

/**
 * Tests for a linq4j front-end and JDBC back-end.
 */
public class LinqFrontJdbcBackTest {
  @Test public void testTableWhere() throws SQLException,
      ClassNotFoundException {
    final OptiqConnection connection =
        OptiqAssert.getConnection(OptiqAssert.SchemaSpec.JDBC_FOODMART);
    final SchemaPlus schema =
        connection.getRootSchema().getSubSchema("foodmart");
    ParameterExpression c =
        Expressions.parameter(JdbcTest.Customer.class, "c");
    String s =
        Schemas.queryable(Schemas.createDataContext(connection), schema,
            JdbcTest.Customer.class, "customer")
            .where(
                Expressions.<Predicate1<JdbcTest.Customer>>lambda(
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
