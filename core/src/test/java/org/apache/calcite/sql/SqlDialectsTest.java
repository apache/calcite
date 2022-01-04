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
package org.apache.calcite.sql;

import org.apache.calcite.jdbc.Driver;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link SqlDialects}.
 */
public class SqlDialectsTest {
  @Test void testCreateContextFromCalciteMetaData() throws SQLException {
    Connection connection =
        DriverManager.getConnection(Driver.CONNECT_STRING_PREFIX);
    DatabaseMetaData metaData = connection.getMetaData();

    SqlDialect.Context context = SqlDialects.createContext(metaData);
    assertThat(context.databaseProductName(),
        is(metaData.getDatabaseProductName()));
    assertThat(context.databaseMajorVersion(),
        is(metaData.getDatabaseMajorVersion()));
  }
}
