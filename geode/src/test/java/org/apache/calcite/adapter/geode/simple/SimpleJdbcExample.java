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
package org.apache.calcite.adapter.geode.simple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Example of using Geode via JDBC.
 */
public class SimpleJdbcExample {

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(SimpleJdbcExample.class.getName());

  private SimpleJdbcExample() {
  }

  public static void main(String[] args) throws Exception {

    Properties info = new Properties();
    final String model = "inline:"
        + "{\n"
        + "  version: '1.0',\n"
        + "  schemas: [\n"
        + "     {\n"
        + "       type: 'custom',\n"
        + "       name: 'TEST',\n"
        + "       factory: 'org.apache.calcite.adapter.geode.simple"
        + ".GeodeSimpleSchemaFactory',\n"
        + "       operand: {\n"
        + "         locatorHost: 'localhost',\n"
        + "         locatorPort: '10334',\n"
        + "         regions: 'BookMaster',\n"
        + "         pdxSerializablePackagePath: 'org.apache.calcite.adapter.geode.domain.*'\n"
        + "       }\n"
        + "     }\n"
        + "  ]\n"
        + "}";
    info.put("model", model);

    Class.forName("org.apache.calcite.jdbc.Driver");

    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);

    Statement statement = connection.createStatement();

    ResultSet resultSet = statement.executeQuery("SELECT * FROM \"TEST\".\"BookMaster\"");

    final StringBuilder buf = new StringBuilder();

    while (resultSet.next()) {

      int columnCount = resultSet.getMetaData().getColumnCount();

      for (int i = 1; i <= columnCount; i++) {

        buf.append(i > 1 ? "; " : "")
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=")
            .append(resultSet.getObject(i));
      }

      LOGGER.info("Entry: " + buf.toString());

      buf.setLength(0);
    }

    resultSet.close();
    statement.close();
    connection.close();
  }
}

// End SimpleJdbcExample.java
