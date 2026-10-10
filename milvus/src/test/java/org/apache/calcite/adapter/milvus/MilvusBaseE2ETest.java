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
package org.apache.calcite.adapter.milvus;

import org.apache.calcite.adapter.milvus.extension.MilvusExtension;
import org.apache.calcite.adapter.milvus.factory.MilvusSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import io.milvus.client.MilvusServiceClient;
import io.milvus.v2.client.MilvusClientV2;

import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Base class for Milvus E2E tests providing utility methods and constants.
 *
 * <p>Tests extending this class should also use
 * {@code @ExtendWith(MilvusExtension.class)} to ensure Milvus containers
 * are properly initialized.
 */
@ExtendWith(MilvusExtension.class)
public abstract class MilvusBaseE2ETest {

  public static final String MILVUS_CONVERTER = "MilvusToEnumerableConverter";
  public static final String MILVUS_SCAN = "MilvusTableScan";


  public static MilvusServiceClient getMilvusServiceClientV1() {
    return MilvusExtension.getMilvusClientV1();
  }


  public static MilvusClientV2 getMilvusServiceClientV2() {
    return MilvusExtension.getMilvusClient();
  }


  /**
   * Check if the execution plan contains a specific Milvus operator.
   *
   * @param executionPlan the execution plan string
   * @param operator the operator name to search for
   * @return true if the operator is present
   */
  protected boolean containsMilvusOperator(String executionPlan, String operator) {
    for (String plan : executionPlan.split("\n")) {
      if (plan.contains(operator)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the execution plan for a SQL query.
   *
   * @param sql the SQL query
   * @param connection the Calcite connection
   * @return the execution plan string
   * @throws SQLException if query execution fails
   */
  public String getExecutionPlan(String sql, Connection connection) throws SQLException {
    String explainSql = "EXPLAIN PLAN FOR " + sql;

    String executionPlan = "";
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(explainSql);
      while (resultSet.next()) {
        executionPlan = resultSet.getString(1);
      }
    }
    System.out.println("\n=== Execution Plan for SQL ===");
    System.out.println(executionPlan);
    return executionPlan;
  }

  /**
   * Set up a Calcite connection with Milvus schema.
   *
   * @return Connection to Calcite with Milvus schema
   * @throws Exception if setup fails
   */
  public static Connection setupCalciteConnection() throws Exception {
    // Enable Janino source printing
//    System.setProperty("calcite.debug", "true");
//    System.setProperty("calcite.debug.janino", "true");

    Map<String, Object> params = MilvusExtension.getConnectionParams();
    String host = (String) params.get("host");
    Integer port = (Integer) params.get("port");

    Properties info = new Properties();
    info.setProperty("lex", "JAVA");

    final Driver driver = new Driver();
    Connection connection = driver.connect("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Map<String, Object> operands = new HashMap<>();
    operands.put("host", host);
    operands.put("port", port);
    operands.put("databaseName", "default");

    MilvusSchemaFactory schemaFactory = new MilvusSchemaFactory();
    // Create schema
    Schema milvusSchema = schemaFactory.create(rootSchema, "milvus", operands);
    // Add to root
    rootSchema.add("milvus", milvusSchema);

    return connection;
  }

  protected List<String> getSqlResult(String sql, Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      List<String> actual = new ArrayList<>();
      int columnCount = resultSet.getMetaData().getColumnCount();
      while (resultSet.next()) {
        StringBuilder rowValue = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
          String value = resultSet.getString(i);
          if (i > 1) {
            rowValue.append(",");
          }
          rowValue.append(value);
        }
        actual.add(rowValue.toString());
      }
      return actual;
    }
  }
}
