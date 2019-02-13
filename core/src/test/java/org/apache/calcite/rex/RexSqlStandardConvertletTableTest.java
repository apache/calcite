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
package org.apache.calcite.rex;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.SqlToRelTestBase;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Closer;
import org.apache.calcite.util.TestUtil;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link org.apache.calcite.rex.RexSqlStandardConvertletTable}.
 */
public class RexSqlStandardConvertletTableTest extends SqlToRelTestBase {

  @Test
  public void testCoalesce() {
    final Project project = (Project) convertSqlToRel(
            "SELECT COALESCE(NULL, 'a')", false);
    final RexNode rex = project.getChildExps().get(0);
    final RexToSqlNodeConverter rexToSqlNodeConverter = rexToSqlNodeConverter();
    final SqlNode convertedSql = rexToSqlNodeConverter.convertNode(rex);
    assertEquals(
            "CASE WHEN NULL IS NOT NULL THEN NULL ELSE 'a' END",
            convertedSql.toString());
  }

  @Test
  public void testCaseWithValue() {
    final Project project =
            (Project) convertSqlToRel(
                    "SELECT CASE NULL WHEN NULL THEN NULL ELSE 'a' END", false);
    final RexNode rex = project.getChildExps().get(0);
    final RexToSqlNodeConverter rexToSqlNodeConverter = rexToSqlNodeConverter();
    final SqlNode convertedSql = rexToSqlNodeConverter.convertNode(rex);
    assertEquals(
            "CASE WHEN NULL = NULL THEN NULL ELSE 'a' END",
            convertedSql.toString());
  }

  @Test
  public void testCaseNoValue() {
    final Project project = (Project) convertSqlToRel(
            "SELECT CASE WHEN NULL IS NULL THEN NULL ELSE 'a' END", false);
    final RexNode rex = project.getChildExps().get(0);
    final RexToSqlNodeConverter rexToSqlNodeConverter = rexToSqlNodeConverter();
    final SqlNode convertedSql = rexToSqlNodeConverter.convertNode(rex);
    assertEquals(
            "CASE WHEN NULL IS NULL THEN NULL ELSE 'a' END",
            convertedSql.toString());
  }

  private RelNode convertSqlToRel(String sql, boolean simplifyRex) {
    final FrameworkConfig config = Frameworks.newConfigBuilder()
            .defaultSchema(CalciteSchema.createRootSchema(false).plus())
            .parserConfig(SqlParser.configBuilder().build())
            .build();
    final Planner planner = Frameworks.getPlanner(config);
    try (Closer closer = new Closer()) {
      closer.add(Hook.REL_BUILDER_SIMPLIFY.addThread(Hook.propertyJ(simplifyRex)));
      final SqlNode parsed = planner.parse(sql);
      final SqlNode validated = planner.validate(parsed);
      return planner.rel(validated).rel;
    } catch (SqlParseException | RelConversionException | ValidationException e) {
      throw TestUtil.rethrow(e);
    }
  }

  private static RexToSqlNodeConverter rexToSqlNodeConverter() {
    final RexSqlStandardConvertletTable convertletTable = new RexSqlStandardConvertletTable();
    return new RexToSqlNodeConverterImpl(convertletTable);
  }

}

// End RexSqlStandardConvertletTableTest.java
