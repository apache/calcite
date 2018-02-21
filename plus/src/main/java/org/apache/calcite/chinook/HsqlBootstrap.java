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
package org.apache.calcite.chinook;

import net.hydromatic.chinook.data.hsqldb.ChinookHsqldb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Bootstrap of HSQLDB schema under calcite. It contains data for end 2 end tests.
 */
public class HsqlBootstrap {

  private static HsqlBootstrap instance;

  private HsqlBootstrap() {
  }

  public static HsqlBootstrap instance() {
    if (instance == null) {
      new org.hsqldb.jdbc.JDBCDriver();
      instance = new HsqlBootstrap();
    }
    return instance;
  }

  public Connection connection() throws SQLException {
    return DriverManager.getConnection(
            ChinookHsqldb.URI,
            ChinookHsqldb.USER,
            ChinookHsqldb.PASSWORD);
  }
}
// End HsqlBootstrap.java
