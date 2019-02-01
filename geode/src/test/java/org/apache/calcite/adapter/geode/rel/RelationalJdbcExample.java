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
package org.apache.calcite.adapter.geode.rel;

import org.apache.calcite.adapter.geode.util.GeodeUtils;

import org.apache.geode.cache.GemFireCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Properties;
import javax.naming.Context;
import javax.naming.InitialContext;

/**
 * Example of using Geode via JDBC.
 *
 * <p>Before using this example, you need to populate Geode, as follows:
 *
 * <blockquote><code>
 * git clone https://github.com/vlsi/calcite-test-dataset<br>
 * cd calcite-test-dataset<br>
 * mvn install
 * </code></blockquote>
 *
 * <p>This will create a virtual machine with Geode and the "bookshop" and "zips"
 * test data sets.
 */
public class RelationalJdbcExample {

  protected static final Logger LOGGER = LoggerFactory.getLogger(
      RelationalJdbcExample.class.getName());

  private RelationalJdbcExample() {
  }

  private static void runExampleForGivenGeodeModelJson(String geodeModelJson) throws Exception {
    Class.forName("org.apache.calcite.jdbc.Driver");

    Properties info = new Properties();
    info.put("model", geodeModelJson);

    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "SELECT \"b\".\"author\", \"b\".\"retailCost\", \"i\".\"quantityInStock\"\n"
            + "FROM \"TEST\".\"BookMaster\" AS \"b\" "
            + " INNER JOIN \"TEST\".\"BookInventory\" AS \"i\""
            + "  ON \"b\".\"itemNumber\" = \"i\".\"itemNumber\"\n "
            + "WHERE  \"b\".\"retailCost\" > 0");

    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        buf.append(i > 1 ? "; " : "")
            .append(metaData.getColumnLabel(i)).append("=").append(resultSet.getObject(i));
      }
      LOGGER.info("Result entry: " + buf.toString());
      buf.setLength(0);
    }

    GeodeUtils.closeClientCache();
    resultSet.close();
    statement.close();
    connection.close();
  }

  private static void runExampleWithLocatorHostPort() throws Exception {
    final String geodeModelJson =
        "inline:"
            + "{\n"
            + "  version: '1.0',\n"
            + "  schemas: [\n"
            + "     {\n"
            + "       type: 'custom',\n"
            + "       name: 'TEST',\n"
            + "       factory: 'org.apache.calcite.adapter.geode.rel.GeodeSchemaFactory',\n"
            + "       operand: {\n"
            + "         locatorHost: 'localhost', \n"
            + "         locatorPort: '10334', \n"
            + "         regions: 'BookMaster,BookCustomer,BookInventory,BookOrder', \n"
            + "         pdxSerializablePackagePath: 'org.apache.calcite.adapter.geode.domain.*' \n"
            + "       }\n"
            + "     }\n"
            + "   ]\n"
            + "}";

    runExampleForGivenGeodeModelJson(geodeModelJson);
  }

  private static void runExampleWithJndiClientCacheObject() throws Exception {
    Hashtable env = new Hashtable();
    env.put(Context.INITIAL_CONTEXT_FACTORY, GeodeSchemaFactory.GEODE_JNDI_INITIAL_CONTEXT_FACTORY);
    Context context = new InitialContext(env);
    GemFireCache gemFireCache = GeodeUtils.createClientCache("localhost", 10334,
        "org.apache.calcite.adapter.geode.domain.*", true);
    context.bind("java:/TestClientCacheObject", gemFireCache);
    final String geodeModelJson =
        "inline:"
            + "{\n"
            + "  version: '1.0',\n"
            + "  schemas: [\n"
            + "     {\n"
            + "       type: 'custom',\n"
            + "       name: 'TEST',\n"
            + "       factory: 'org.apache.calcite.adapter.geode.rel.GeodeSchemaFactory',\n"
            + "       operand: {\n"
            + "         geodeClientCacheObjectKey: 'java:/TestClientCacheObject', \n"
            + "         regions: 'BookMaster,BookCustomer,BookInventory,BookOrder' \n"
            + "       }\n"
            + "     }\n"
            + "   ]\n"
            + "}";

    runExampleForGivenGeodeModelJson(geodeModelJson);
  }

  private static void doExample(int i) throws Exception {
    switch (i) {
    case 0:
      runExampleWithLocatorHostPort();
      break;
    case 1:
      runExampleWithJndiClientCacheObject();
      break;
    default:
      throw new AssertionError("unknown example " + i);
    }
  }

  public static void main(String[] args) throws Exception {
    for (int i = 0; i < 2; i++) {
      doExample(i);
    }
  }
}

// End RelationalJdbcExample.java
