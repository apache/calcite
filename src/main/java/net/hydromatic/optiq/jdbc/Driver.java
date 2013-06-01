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
package net.hydromatic.optiq.jdbc;

import net.hydromatic.optiq.model.ModelHandler;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Optiq JDBC driver.
 */
public class Driver extends UnregisteredDriver {
  public static final String CONNECT_STRING_PREFIX = "jdbc:optiq:";

  static {
    new Driver().register();
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(
        Driver.class,
        "net-hydromatic-optiq-jdbc.properties",
        "Optiq JDBC Driver",
        "unknown version",
        "Optiq",
        "unknown version");
  }

  @Override
  protected Handler createHandler() {
    return new HandlerImpl() {
      @Override
      public void onConnectionInit(OptiqConnection connection)
          throws SQLException {
        super.onConnectionInit(connection);
        final String model =
            ConnectionProperty.MODEL.getString(
                connection.getProperties());
        if (model != null) {
          try {
            new ModelHandler(connection, model);
          } catch (IOException e) {
            throw new SQLException(e);
          }
        }
      }
    };
  }
}

// End Driver.java
