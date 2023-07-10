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
package org.apache.calcite.sql;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.MockRelOptPlanner;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SqlBasicCall}.
 */
public class SqlBasicCallTest {

  @Test void testSqlBasicCallSetOperands() {
    final String sql = "SELECT { fn TRUNCATE(\n"
        + "        { fn QUARTER(\n"
        + "                { fn TIMESTAMPADD(\n"
        + "                        SQL_TSI_HOUR,\n"
        + "                        1,\n"
        + "                        { ts '1900-01-01 00:00:00' }\n"
        + "                    ) }\n"
        + "            ) },\n"
        + "        0\n"
        + "    ) }\n"
        + "FROM EMP\n"
        + "GROUP BY 1.1000000000000001";
    try {
      RelOptPlanner planner = new MockRelOptPlanner(Contexts.empty());
      planner.addRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
      SqlTestFactory sqlTestFactory = SqlTestFactory.INSTANCE
          .withPlannerFactory(context -> planner);
      SqlParser parser = sqlTestFactory.createParser(sql);
      final SqlNode sqlNode = parser.parseQuery();
      SqlToRelConverter sqlToRelConverter = sqlTestFactory.createSqlToRelConverter();
      RelRoot relRoot = sqlToRelConverter.convertQuery(sqlNode, true, true);
      planner.setRoot(relRoot.rel);
      planner.findBestExp();
      // TRUNCATE(EXTRACT(FLAG(QUARTER), +(1900-01-01 00:00:00, *(3600000:INTERVAL HOUR, 1))), 0)
      RexCall rexCall = (RexCall) ((LogicalProject) relRoot.rel).getProjects().get(0);
      RexCall operand = (RexCall) rexCall.getOperands().get(0);
      assert operand.getOperator().getName().equals(SqlStdOperatorTable.EXTRACT.getName());
    } catch (SqlParseException e) {
      throw new RuntimeException(e);
    }
  }
}
