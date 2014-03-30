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
package net.hydromatic.optiq.impl.splunk;

import net.hydromatic.avatica.DriverVersion;
import net.hydromatic.avatica.UnregisteredDriver;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.impl.jdbc.JdbcSchema;
import net.hydromatic.optiq.impl.splunk.search.SplunkConnection;
import net.hydromatic.optiq.jdbc.*;

import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;

/**
 * JDBC driver for Splunk.
 *
 * <p>It accepts connect strings that start with "jdbc:splunk:".</p>
 */
public class SplunkDriver extends UnregisteredDriver {
  protected SplunkDriver() {
    super();
  }

  static {
    new SplunkDriver().register();
  }

  protected String getConnectStringPrefix() {
    return "jdbc:splunk:";
  }

  protected DriverVersion createDriverVersion() {
    return new SplunkDriverVersion();
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    Connection connection = super.connect(url, info);
    OptiqConnection optiqConnection = (OptiqConnection) connection;
    SplunkConnection splunkConnection;
    try {
      String url1 = info.getProperty("url");
      if (url1 == null) {
        throw new IllegalArgumentException(
            "Must specify 'url' property");
      }
      URL url2 = new URL(url1);
      String user = info.getProperty("user");
      if (user == null) {
        throw new IllegalArgumentException(
            "Must specify 'user' property");
      }
      String password = info.getProperty("password");
      if (password == null) {
        throw new IllegalArgumentException(
            "Must specify 'password' property");
      }
      splunkConnection = new SplunkConnection(url2, user, password);
    } catch (Exception e) {
      throw new SQLException("Cannot connect", e);
    }
    final SchemaPlus rootSchema = optiqConnection.getRootSchema();
    rootSchema.add("splunk", new SplunkSchema(splunkConnection));

    // Include a schema called "mysql" in every splunk connection.
    // This is a hack for demo purposes. TODO: Add a config file mechanism.
    if (true) {
      final String mysqlSchemaName = "mysql";
      try {
        Class.forName("com.mysql.jdbc.Driver");
      } catch (ClassNotFoundException e) {
        throw new SQLException(e);
      }
      final DataSource dataSource =
          JdbcSchema.dataSource("jdbc:mysql://localhost", null, "foodmart",
              "foodmart");
      rootSchema.add("foodmart",
          JdbcSchema.create(optiqConnection.getRootSchema(), "foodmart",
              dataSource, "", mysqlSchemaName));
    }

    return connection;
  }
}

// End SplunkDriver.java
