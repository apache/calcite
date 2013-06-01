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
package net.hydromatic.optiq.test;

import net.hydromatic.optiq.jdbc.OptiqConnection;

import org.eigenbase.sql.test.*;
import org.eigenbase.sql.validate.SqlConformance;
import org.eigenbase.test.SqlValidatorTestCase;

import java.sql.*;

/**
 * Embodiment of {@link org.eigenbase.sql.test.SqlOperatorBaseTest}
 * that generates SQL statements and executes them using Optiq.
 */
public class OptiqSqlOperatorTest extends SqlOperatorBaseTest {

  private static SqlTester STATIC_TESTER;

  public OptiqSqlOperatorTest(String testName) {
    super(testName, false);
  }

  @Override
  protected void setUp() throws Exception {
    if (STATIC_TESTER != null) {
      return;
    }
    final OptiqConnection connection =
        JdbcTest.getConnection("hr");

    STATIC_TESTER =
        new SqlValidatorTestCase.TesterImpl(SqlConformance.Default) {
          @Override
          public void check(
              String query,
              TypeChecker typeChecker,
              ResultChecker resultChecker) {
            System.out.println(query);
            super.check(
                query,
                typeChecker,
                resultChecker);
            Statement statement = null;
            try {
              statement = connection.createStatement();
              final ResultSet resultSet =
                  statement.executeQuery(query);
              resultChecker.checkResult(resultSet);
            } catch (Exception e) {
              throw new RuntimeException(e);
            } finally {
              if (statement != null) {
                try {
                  statement.close();
                } catch (SQLException e) {
                  // ignore
                }
              }
            }
          }
        };
  }

  @Override
  protected SqlTester getTester() {
    return STATIC_TESTER;
  }
}

// End OptiqSqlOperatorTest.java
