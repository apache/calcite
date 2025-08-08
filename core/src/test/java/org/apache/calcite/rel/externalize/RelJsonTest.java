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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
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
import org.apache.calcite.test.DiffRepository;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.JsonBuilder;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;

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
}
