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
package org.apache.calcite.slt.executors;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import net.hydromatic.sqllogictest.OptionsParser;
import net.hydromatic.sqllogictest.SltSqlStatement;
import net.hydromatic.sqllogictest.executors.HsqldbExecutor;
import net.hydromatic.sqllogictest.executors.JdbcExecutor;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Set;
import javax.sql.DataSource;

/**
 * Executor for SQL logic tests using Calcite's JDBC adapter.
 */
public class CalciteExecutor extends JdbcExecutor {
  /**
   * This executor is used for executing the statements (CREATE TABLE,
   * CREATE VIEW, INSERT).  Queries are executed by Calcite.
   */
  private final JdbcExecutor statementExecutor;

  public static void register(OptionsParser parser) {
    parser.registerExecutor("calcite", () -> {
      OptionsParser.SuppliedOptions options = parser.getOptions();
      HsqldbExecutor statementExecutor = new HsqldbExecutor(options);
      try {
        CalciteExecutor result = new CalciteExecutor(options, statementExecutor);
        Set<String> bugs = options.readBugsFile();
        result.avoid(bugs);
        return result;
      } catch (IOException | SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override public void establishConnection() throws SQLException {
    this.statementExecutor.establishConnection();
  }

  @Override public void dropAllTables() throws SQLException {
    this.statementExecutor.dropAllTables();
  }

  @Override public void dropAllViews() throws SQLException {
    this.statementExecutor.dropAllViews();
  }

  @Override public void statement(SltSqlStatement statement) throws SQLException {
    this.statementExecutor.statement(statement);
  }

  public CalciteExecutor(OptionsParser.SuppliedOptions options, JdbcExecutor statementExecutor)
      throws SQLException {
    super(options, "jdbc:calcite:lex=ORACLE", "", "");
    this.statementExecutor = statementExecutor;
    // Build our connection
    this.connection =
        DriverManager.getConnection("jdbc:calcite:lex=ORACLE");
    CalciteConnection calciteConnection = this.connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    DataSource hsqldb =
        JdbcSchema.dataSource(statementExecutor.dbUrl,
        "org.hsqldb.jdbcDriver",
        "",
        "");
    final String schemaName = "SLT";
    JdbcSchema jdbcSchema = JdbcSchema.create(rootSchema, schemaName, hsqldb, null, null);
    rootSchema.add(schemaName, jdbcSchema);
    calciteConnection.setSchema(schemaName);
  }
}
