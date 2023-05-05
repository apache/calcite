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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Set;
import javax.sql.DataSource;

import net.hydromatic.sqllogictest.ExecutionOptions;
import net.hydromatic.sqllogictest.TestStatistics;
import net.hydromatic.sqllogictest.SltSqlStatement;
import net.hydromatic.sqllogictest.SqlTestQuery;
import net.hydromatic.sqllogictest.SltTestFile;
import net.hydromatic.sqllogictest.ISqlTestOperation;
import net.hydromatic.sqllogictest.executors.JdbcExecutor;
import net.hydromatic.sqllogictest.executors.SqlSltTestExecutor;
import net.hydromatic.sqllogictest.executors.HsqldbExecutor;

public class CalciteExecutor extends SqlSltTestExecutor {
  private final JdbcExecutor statementExecutor;
  private final Connection connection;

  public static void register(ExecutionOptions options) {
    options.registerExecutor("calcite", () -> {
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

  public CalciteExecutor(ExecutionOptions options, JdbcExecutor statementExecutor) throws SQLException {
    super(options);
    this.statementExecutor = statementExecutor;
    // Build our connection
    this.connection = DriverManager.getConnection(
        "jdbc:calcite:lex=ORACLE");
    CalciteConnection calciteConnection = this.connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    DataSource hsqldb = JdbcSchema.dataSource(
        statementExecutor.dbUrl,
        "org.hsqldb.jdbcDriver",
        "",
        ""
    );
    final String SCHEMA_NAME = "SLT";
    JdbcSchema jdbcSchema = JdbcSchema.create(rootSchema, SCHEMA_NAME, hsqldb, null, null);
    rootSchema.add(SCHEMA_NAME, jdbcSchema);
    calciteConnection.setSchema(SCHEMA_NAME);
  }

  boolean statement(SltSqlStatement statement) throws SQLException {
    this.options.message(this.statementsExecuted + ": " + statement.statement, 2);
    assert this.connection != null;
    if (this.buggyOperations.contains(statement.statement)
        || this.options.doNotExecute) {
      options.message("Skipping " + statement.statement, 2);
      return false;
    }
    this.statementExecutor.statement(statement);
    this.statementsExecuted++;
    return true;
  }

  /**
   * Run a query.
   * @param query       Query to execute.
   * @param statistics  Execution statistics recording the result of
   *                    the query execution.
   * @return            True if we need to stop executing.
   */
  boolean query(SqlTestQuery query, TestStatistics statistics) throws SQLException {
    String q = query.getQuery();
    this.options.message( "Executing: " + q, 2);
    if (this.buggyOperations.contains(q) || this.options.doNotExecute) {
      statistics.incIgnored();
      options.message("Skipping " + query.getQuery(), 2);
      return false;
    }
    try (PreparedStatement ps = this.connection.prepareStatement(q)) {
      ps.execute();
      try (ResultSet resultSet = ps.getResultSet()) {
        return this.statementExecutor.validate(query, resultSet, query.outputDescription, statistics);
      } catch (NoSuchAlgorithmException e) {
        // This should never happen
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public TestStatistics execute(SltTestFile file, ExecutionOptions options)
      throws SQLException {
    this.startTest();
    this.statementExecutor.establishConnection();
    this.statementExecutor.dropAllViews();
    this.statementExecutor.dropAllTables();

    TestStatistics result = new TestStatistics(options.stopAtFirstError);
    for (ISqlTestOperation operation : file.fileContents) {
      SltSqlStatement stat = operation.as(SltSqlStatement.class);
      if (stat != null) {
        try {
          this.statement(stat);
        } catch (SQLException ex) {
          options.err.println("Error while processing #"
              + (result.totalTests() + 1) + " " + operation);
          throw ex;
        }
        this.statementsExecuted++;
      } else {
        SqlTestQuery query = operation.to(options.err, SqlTestQuery.class);
        boolean stop;
        try {
          stop = this.query(query, result);
        } catch (SQLException ex) {
          options.message("Error while processing "
              + query.getQuery() + " " + ex.getMessage(), 1);
          stop = result.addFailure(
              new TestStatistics.FailedTestDescription(query,
                  ex.getMessage(), ex, options.verbosity > 0));
        }
        if (stop) {
          break;
        }
      }
    }
    this.statementExecutor.dropAllViews();
    this.statementExecutor.dropAllTables();
    this.statementExecutor.closeConnection();
    options.message(this.elapsedTime(file.getTestCount()), 1);
    return result;
  }
}
