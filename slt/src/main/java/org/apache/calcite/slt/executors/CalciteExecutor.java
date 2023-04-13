/*
 * Copyright 2023 VMware, Inc.
 * SPDX-License-Identifier: MIT
 * SPDX-License-Identifier: Apache-2.0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.apache.calcite.slt.executors;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.slt.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class CalciteExecutor extends SqlSLTTestExecutor {
  Logger logger = Logger.getLogger("CalciteExecutor");
  private final JDBCExecutor statementExecutor;
  private final Connection connection;

  public CalciteExecutor(JDBCExecutor statementExecutor) throws SQLException {
    this.statementExecutor = statementExecutor;
    // Build our connection
    this.connection = DriverManager.getConnection(
        "jdbc:calcite:lex=ORACLE");
    CalciteConnection calciteConnection = this.connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    DataSource hsqldb = JdbcSchema.dataSource(
        "jdbc:hsqldb:mem:db",
        "org.hsqldb.jdbcDriver",
        "",
        ""
    );
    final String SCHEMA_NAME = "SLT";
    JdbcSchema jdbcSchema = JdbcSchema.create(rootSchema, SCHEMA_NAME, hsqldb, null, null);
    rootSchema.add(SCHEMA_NAME, jdbcSchema);
    calciteConnection.setSchema(SCHEMA_NAME);
  }

  boolean statement(SLTSqlStatement statement) throws SQLException {
    this.statementExecutor.statement(statement);
    return true;
  }

  void query(SqlTestQuery query, TestStatistics statistics) throws UnsupportedEncodingException {
    String q = query.getQuery();
    logger.info(() -> "Executing query " + q);
    try (PreparedStatement ps = this.connection.prepareStatement(q)) {
      ps.execute();
      try (ResultSet resultSet = ps.getResultSet()) {
        this.statementExecutor.validate(query, resultSet, query.outputDescription, statistics);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    } catch (SQLException e) {
      StringPrintStream str = new StringPrintStream();
      e.printStackTrace(str.getPrintStream());
      statistics.addFailure(new TestStatistics.FailedTestDescription(
          query, str.toString()));
    }
  }

  @Override
  public TestStatistics execute(SLTTestFile file, ExecutionOptions options)
      throws IOException, SQLException {
    this.statementExecutor.establishConnection();
    this.statementExecutor.dropAllViews();
    this.statementExecutor.dropAllTables();

    TestStatistics result = new TestStatistics(options.stopAtFirstError);
    for (ISqlTestOperation operation : file.fileContents) {
      SLTSqlStatement stat = operation.as(SLTSqlStatement.class);
      if (stat != null) {
        boolean status;
        try {
          if (this.buggyOperations.contains(stat.statement)) {
            logger.info(() -> "Skipping buggy test " + stat.statement);
            status = stat.shouldPass;
          } else {
            status = this.statement(stat);
          }
        } catch (SQLException ex) {
          logger.warning("Statement failed " + stat.statement);
          status = false;
        }
        this.statementsExecuted++;
        if (this.validateStatus &&
            status != stat.shouldPass)
          throw new RuntimeException("Statement " + stat.statement + " status " + status + " expected " + stat.shouldPass);
      } else {
        SqlTestQuery query = operation.to(SqlTestQuery.class);
        if (this.buggyOperations.contains(query.getQuery())) {
          logger.info(() -> "Skipping buggy test " + query.getQuery());
          result.incIgnored();
          continue;
        }
        this.query(query, result);
      }
    }
    this.statementExecutor.closeConnection();
    this.reportTime(result.getPassed());
    logger.info("Finished executing " + file);
    return result;
  }
}
