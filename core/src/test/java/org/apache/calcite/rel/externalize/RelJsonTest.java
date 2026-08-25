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
package org.apache.calcite.rel.externalize;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.DiffRepository;
import org.apache.calcite.test.schemata.hr.HrSchema;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.JsonBuilder;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for @{@link RelJson}.
 */
public class RelJsonTest {

  private static final DiffRepository REPO =  DiffRepository.lookup(RelJsonTest.class);

  @Test void testToJsonWithStructRelDatatypeField() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = typeFactory.builder()
        .add("street", SqlTypeName.VARCHAR, 50)
        .add("number", SqlTypeName.INTEGER)
        .add("building", SqlTypeName.VARCHAR, 20).nullable(true)
        .build();
    RelDataTypeField address =
        new RelDataTypeFieldImpl("address", 0, type);

    JsonBuilder builder = new JsonBuilder();
    Object jsonObj = RelJson.create().withJsonBuilder(builder).toJson(address);
    REPO.assertEquals("content", "${content}", builder.toJsonString(jsonObj));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7113">[CALCITE-7113]
   * RelJson cannot serialize RexLambda</a>. */
  @Test void testLambda() throws SqlParseException, ValidationException, RelConversionException {
    final String query = "SELECT \"EXISTS\"(ARRAY[1], x -> x > 2)";
    SqlOperatorTable opTab = SqlLibraryOperatorTableFactory.INSTANCE
        .getOperatorTable(EnumSet.of(SqlLibrary.STANDARD, SqlLibrary.SPARK));
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .operatorTable(opTab)
        .defaultSchema(rootSchema)
        .build();
    Planner planner = Frameworks.getPlanner(config);
    SqlNode n = planner.parse(query);
    n = planner.validate(n);
    RelNode root = planner.rel(n).project();
    String plan =
        RelOptUtil.dumpPlan("-- Plan", root,
            SqlExplainFormat.JSON, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertThat(
        plan, containsString("{\n"
        + "              \"op\": \"lambda\",\n"
        + "              \"parameters\": [\n"
        + "                {\n"
        + "                  \"index\": 0,\n"
        + "                  \"name\": \"X\",\n"
        + "                  \"type\": {\n"
        + "                    \"type\": \"INTEGER\",\n"
        + "                    \"nullable\": false\n"
        + "                  }\n"
        + "                }\n"
        + "              ],\n"
        + "              \"expression\": {\n"
        + "                \"pos\": {\n"
        + "                  \"line\": 1,\n"
        + "                  \"column\": 32,\n"
        + "                  \"end_line\": 1,\n"
        + "                  \"end_column\": 36\n"
        + "                },\n"
        + "                \"op\": {\n"
        + "                  \"name\": \">\",\n"
        + "                  \"kind\": \"GREATER_THAN\",\n"
        + "                  \"syntax\": \"BINARY\"\n"
        + "                },\n"
        + "                \"operands\": [\n"
        + "                  {\n"
        + "                    \"index\": 0,\n"
        + "                    \"name\": \"X\",\n"
        + "                    \"type\": {\n"
        + "                      \"type\": \"INTEGER\",\n"
        + "                      \"nullable\": false\n"
        + "                    }\n"
        + "                  },\n"
        + "                  {\n"
        + "                    \"literal\": 2,\n"
        + "                    \"type\": {\n"
        + "                      \"type\": \"INTEGER\",\n"
        + "                      \"nullable\": false\n"
        + "                    }\n"
        + "                  }\n"
        + "                ]\n"
        + "              }\n"
        + "            }"));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7251">[CALCITE-7251]
   * SEARCH and WINDOW operations should carry source position information</a>. */
  @Test void testSearchPosition()
      throws SqlParseException, ValidationException, RelConversionException {
    final String query = "SELECT val IN (1, 2, 3, 4)\n"
        + "FROM (\n"
        + "  VALUES (10), (30), (20), (40)\n"
        + ") AS t(val)";
    SqlOperatorTable opTab = SqlLibraryOperatorTableFactory.INSTANCE
        .getOperatorTable(EnumSet.of(SqlLibrary.STANDARD, SqlLibrary.SPARK));
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .operatorTable(opTab)
        .defaultSchema(rootSchema)
        .build();
    Planner planner = Frameworks.getPlanner(config);
    SqlNode n = planner.parse(query);
    n = planner.validate(n);
    RelNode root = planner.rel(n).project();
    String plan =
        RelOptUtil.dumpPlan("-- Plan", root,
            SqlExplainFormat.JSON, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertThat(
        plan, containsString("\"exprs\": [\n"
            + "        {\n"
            + "          \"pos\": {\n"
            + "            \"line\": 1,\n"
            + "            \"column\": 8,\n"
            + "            \"end_line\": 1,\n"
            + "            \"end_column\": 25\n"
            + "          },\n"
            + "          \"op\": {\n"
            + "            \"name\": \"SEARCH\",\n"
            + "            \"kind\": \"SEARCH\",\n"
            + "            \"syntax\": \"INTERNAL\"\n"
            + "          },"));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7743">[CALCITE-7743]
   * RelJson cannot emit hints</a>. */
  @Test void testHint() {
    final String sql = "select *\n"
        + "from \"emps\" /*+ index(name) */";
    final String plan =
        Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
          final SchemaPlus schema =
              rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
          final FrameworkConfig config = Frameworks.newConfigBuilder()
              .parserConfig(SqlParser.Config.DEFAULT)
              .defaultSchema(schema)
              .sqlToRelConverterConfig(SqlToRelConverter.config()
                  .withHintStrategyTable(HintStrategyTable.builder()
                      .hintStrategy("index", HintPredicates.TABLE_SCAN)
                      .build()))
              .build();
          try {
            final Planner planner = Frameworks.getPlanner(config);
            final SqlNode n = planner.validate(planner.parse(sql));
            final RelNode root = planner.rel(n).project();
            final String json =
                RelOptUtil.dumpPlan("", root,
                    SqlExplainFormat.JSON, SqlExplainLevel.DIGEST_ATTRIBUTES);

            // Reads the plan back and verifies that the hints survive.
            final RelJsonReader reader =
                new RelJsonReader(cluster, relOptSchema, schema);
            final RelNode root2 = reader.read(json);
            final List<RelHint> expectedHints = tableScan(root).getHints();
            assertThat(tableScan(root2).getHints(), is(expectedHints));
            return json;
          } catch (SqlParseException | ValidationException
              | RelConversionException | IOException e) {
            throw new RuntimeException(e);
          }
        });
    assertThat(plan, containsString("\"name\": \"INDEX\""));
    assertThat(plan, containsString("\"options\": ["));
    assertThat(plan, containsString("\"NAME\""));
  }

  /** Returns the {@link TableScan} of this plan. */
  private static TableScan tableScan(RelNode rel) {
    if (rel instanceof TableScan) {
      return (TableScan) rel;
    }
    return (TableScan) rel.getInput(0);
  }
}
