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
package org.apache.calcite.plan;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.MockSqlOperatorTable;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.test.schemata.hr.HrSchema;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.TestUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static java.util.Objects.requireNonNull;

/**
 * Unit test for {@link org.apache.calcite.rel.externalize.RelJson}.
 */
class RelWriterTest {
  public static final String XX = "{\n"
      + "  \"rels\": [\n"
      + "    {\n"
      + "      \"id\": \"0\",\n"
      + "      \"relOp\": \"LogicalTableScan\",\n"
      + "      \"table\": [\n"
      + "        \"hr\",\n"
      + "        \"emps\"\n"
      + "      ],\n"
      + "      \"inputs\": []\n"
      + "    },\n"
      + "    {\n"
      + "      \"id\": \"1\",\n"
      + "      \"relOp\": \"LogicalFilter\",\n"
      + "      \"condition\": {\n"
      + "        \"op\": {\n"
      + "          \"name\": \"=\",\n"
      + "          \"kind\": \"EQUALS\",\n"
      + "          \"syntax\": \"BINARY\"\n"
      + "        },\n"
      + "        \"operands\": [\n"
      + "          {\n"
      + "            \"input\": 1,\n"
      + "            \"name\": \"$1\"\n"
      + "          },\n"
      + "          {\n"
      + "            \"literal\": 10,\n"
      + "            \"type\": {\n"
      + "              \"type\": \"INTEGER\",\n"
      + "              \"nullable\": false\n"
      + "            }\n"
      + "          }\n"
      + "        ]\n"
      + "      }\n"
      + "    },\n"
      + "    {\n"
      + "      \"id\": \"2\",\n"
      + "      \"relOp\": \"LogicalAggregate\",\n"
      + "      \"group\": [\n"
      + "        0\n"
      + "      ],\n"
      + "      \"aggs\": [\n"
      + "        {\n"
      + "          \"agg\": {\n"
      + "            \"name\": \"COUNT\",\n"
      + "            \"kind\": \"COUNT\",\n"
      + "            \"syntax\": \"FUNCTION_STAR\"\n"
      + "          },\n"
      + "          \"type\": {\n"
      + "            \"type\": \"BIGINT\",\n"
      + "            \"nullable\": false\n"
      + "          },\n"
      + "          \"distinct\": true,\n"
      + "          \"operands\": [\n"
      + "            1\n"
      + "          ],\n"
      + "          \"name\": \"c\"\n"
      + "        },\n"
      + "        {\n"
      + "          \"agg\": {\n"
      + "            \"name\": \"COUNT\",\n"
      + "            \"kind\": \"COUNT\",\n"
      + "            \"syntax\": \"FUNCTION_STAR\"\n"
      + "          },\n"
      + "          \"type\": {\n"
      + "            \"type\": \"BIGINT\",\n"
      + "            \"nullable\": false\n"
      + "          },\n"
      + "          \"distinct\": false,\n"
      + "          \"operands\": [],\n"
      + "          \"name\": \"d\"\n"
      + "        }\n"
      + "      ]\n"
      + "    }\n"
      + "  ]\n"
      + "}";

  public static final String XXNULL = "{\n"
      + "  \"rels\": [\n"
      + "    {\n"
      + "      \"id\": \"0\",\n"
      + "      \"relOp\": \"LogicalTableScan\",\n"
      + "      \"table\": [\n"
      + "        \"hr\",\n"
      + "        \"emps\"\n"
      + "      ],\n"
      + "      \"inputs\": []\n"
      + "    },\n"
      + "    {\n"
      + "      \"id\": \"1\",\n"
      + "      \"relOp\": \"LogicalFilter\",\n"
      + "      \"condition\": {\n"
      + "        \"op\": {"
      + "            \"name\": \"=\",\n"
      + "            \"kind\": \"EQUALS\",\n"
      + "            \"syntax\": \"BINARY\"\n"
      + "          },\n"
      + "        \"operands\": [\n"
      + "          {\n"
      + "            \"input\": 1,\n"
      + "            \"name\": \"$1\"\n"
      + "          },\n"
      + "          {\n"
      + "            \"literal\": null,\n"
      + "            \"type\": \"INTEGER\"\n"
      + "          }\n"
      + "        ]\n"
      + "      }\n"
      + "    },\n"
      + "    {\n"
      + "      \"id\": \"2\",\n"
      + "      \"relOp\": \"LogicalAggregate\",\n"
      + "      \"group\": [\n"
      + "        0\n"
      + "      ],\n"
      + "      \"aggs\": [\n"
      + "        {\n"
      + "        \"agg\": {\n"
      + "            \"name\": \"COUNT\",\n"
      + "            \"kind\": \"COUNT\",\n"
      + "            \"syntax\": \"FUNCTION_STAR\"\n"
      + "          },\n"
      + "          \"type\": {\n"
      + "            \"type\": \"BIGINT\",\n"
      + "            \"nullable\": false\n"
      + "          },\n"
      + "          \"distinct\": true,\n"
      + "          \"operands\": [\n"
      + "            1\n"
      + "          ]\n"
      + "        },\n"
      + "        {\n"
      + "        \"agg\": {\n"
      + "            \"name\": \"COUNT\",\n"
      + "            \"kind\": \"COUNT\",\n"
      + "            \"syntax\": \"FUNCTION_STAR\"\n"
      + "          },\n"
      + "          \"type\": {\n"
      + "            \"type\": \"BIGINT\",\n"
      + "            \"nullable\": false\n"
      + "          },\n"
      + "          \"distinct\": false,\n"
      + "          \"operands\": []\n"
      + "        }\n"
      + "      ]\n"
      + "    }\n"
      + "  ]\n"
      + "}";

  public static final String XX2 = "{\n"
      + "  \"rels\": [\n"
      + "    {\n"
      + "      \"id\": \"0\",\n"
      + "      \"relOp\": \"LogicalTableScan\",\n"
      + "      \"table\": [\n"
      + "        \"hr\",\n"
      + "        \"emps\"\n"
      + "      ],\n"
      + "      \"inputs\": []\n"
      + "    },\n"
      + "    {\n"
      + "      \"id\": \"1\",\n"
      + "      \"relOp\": \"LogicalProject\",\n"
      + "      \"fields\": [\n"
      + "        \"field0\",\n"
      + "        \"field1\",\n"
      + "        \"field2\"\n"
      + "      ],\n"
      + "      \"exprs\": [\n"
      + "        {\n"
      + "          \"input\": 0,\n"
      + "          \"name\": \"$0\"\n"
      + "        },\n"
      + "        {\n"
      + "          \"op\": {\n"
      + "            \"name\": \"COUNT\",\n"
      + "            \"kind\": \"COUNT\",\n"
      + "            \"syntax\": \"FUNCTION_STAR\"\n"
      + "          },\n"
      + "          \"operands\": [\n"
      + "            {\n"
      + "              \"input\": 0,\n"
      + "              \"name\": \"$0\"\n"
      + "            }\n"
      + "          ],\n"
      + "          \"distinct\": false,\n"
      + "          \"type\": {\n"
      + "            \"type\": \"BIGINT\",\n"
      + "            \"nullable\": false\n"
      + "          },\n"
      + "          \"window\": {\n"
      + "            \"partition\": [\n"
      + "              {\n"
      + "                \"input\": 2,\n"
      + "                \"name\": \"$2\"\n"
      + "              }\n"
      + "            ],\n"
      + "            \"order\": [\n"
      + "              {\n"
      + "                \"expr\": {\n"
      + "                  \"input\": 1,\n"
      + "                  \"name\": \"$1\"\n"
      + "                },\n"
      + "                \"direction\": \"ASCENDING\",\n"
      + "                \"null-direction\": \"LAST\"\n"
      + "              }\n"
      + "            ],\n"
      + "            \"rows-lower\": {\n"
      + "              \"type\": \"UNBOUNDED_PRECEDING\"\n"
      + "            },\n"
      + "            \"rows-upper\": {\n"
      + "              \"type\": \"CURRENT_ROW\"\n"
      + "            }\n"
      + "          }\n"
      + "        },\n"
      + "        {\n"
      + "          \"op\": {\n"
      + "            \"name\": \"SUM\",\n"
      + "            \"kind\": \"SUM\",\n"
      + "            \"syntax\": \"FUNCTION\"\n"
      + "          },\n"
      + "          \"operands\": [\n"
      + "            {\n"
      + "              \"input\": 0,\n"
      + "              \"name\": \"$0\"\n"
      + "            }\n"
      + "          ],\n"
      + "          \"distinct\": false,\n"
      + "          \"type\": {\n"
      + "            \"type\": \"BIGINT\",\n"
      + "            \"nullable\": false\n"
      + "          },\n"
      + "          \"window\": {\n"
      + "            \"partition\": [\n"
      + "              {\n"
      + "                \"input\": 2,\n"
      + "                \"name\": \"$2\"\n"
      + "              }\n"
      + "            ],\n"
      + "            \"order\": [\n"
      + "              {\n"
      + "                \"expr\": {\n"
      + "                  \"input\": 1,\n"
      + "                  \"name\": \"$1\"\n"
      + "                },\n"
      + "                \"direction\": \"ASCENDING\",\n"
      + "                \"null-direction\": \"LAST\"\n"
      + "              }\n"
      + "            ],\n"
      + "            \"range-lower\": {\n"
      + "              \"type\": \"CURRENT_ROW\"\n"
      + "            },\n"
      + "            \"range-upper\": {\n"
      + "              \"type\": \"FOLLOWING\",\n"
      + "              \"offset\": {\n"
      + "                \"literal\": 1,\n"
      + "                \"type\": {\n"
      + "                  \"type\": \"INTEGER\",\n"
      + "                  \"nullable\": false\n"
      + "                }\n"
      + "              }\n"
      + "            }\n"
      + "          }\n"
      + "        }\n"
      + "      ]\n"
      + "    }\n"
      + "  ]\n"
      + "}";

  public static final String XX3 = "{\n"
      + "  \"rels\": [\n"
      + "    {\n"
      + "      \"id\": \"0\",\n"
      + "      \"relOp\": \"LogicalTableScan\",\n"
      + "      \"table\": [\n"
      + "        \"scott\",\n"
      + "        \"EMP\"\n"
      + "      ],\n"
      + "      \"inputs\": []\n"
      + "    },\n"
      + "    {\n"
      + "      \"id\": \"1\",\n"
      + "      \"relOp\": \"LogicalSortExchange\",\n"
      + "      \"distribution\": {\n"
      + "        \"type\": \"HASH_DISTRIBUTED\",\n"
      + "        \"keys\": [\n"
      + "          0\n"
      + "        ]\n"
      + "      },\n"
      + "      \"collation\": [\n"
      + "        {\n"
      + "          \"field\": 0,\n"
      + "          \"direction\": \"ASCENDING\",\n"
      + "          \"nulls\": \"LAST\"\n"
      + "        }\n"
      + "      ]\n"
      + "    }\n"
      + "  ]\n"
      + "}";

  public static final String HASH_DIST_WITHOUT_KEYS = "{\n"
      + "  \"rels\": [\n"
      + "    {\n"
      + "      \"id\": \"0\",\n"
      + "      \"relOp\": \"LogicalTableScan\",\n"
      + "      \"table\": [\n"
      + "        \"scott\",\n"
      + "        \"EMP\"\n"
      + "      ],\n"
      + "      \"inputs\": []\n"
      + "    },\n"
      + "    {\n"
      + "      \"id\": \"1\",\n"
      + "      \"relOp\": \"LogicalSortExchange\",\n"
      + "      \"distribution\": {\n"
      + "        \"type\": \"HASH_DISTRIBUTED\"\n"
      + "      },\n"
      + "      \"collation\": [\n"
      + "        {\n"
      + "          \"field\": 0,\n"
      + "          \"direction\": \"ASCENDING\",\n"
      + "          \"nulls\": \"LAST\"\n"
      + "        }\n"
      + "      ]\n"
      + "    }\n"
      + "  ]\n"
      + "}";

  static Stream<SqlExplainFormat> explainFormats() {
    return Stream.of(SqlExplainFormat.TEXT, SqlExplainFormat.DOT);
  }

  /** Creates a fixture. */
  private static Fixture relFn(Function<RelBuilder, RelNode> relFn) {
    return new Fixture(relFn, false, SqlExplainFormat.TEXT);
  }

  /** Unit test for {@link RelJson#toJson(Object)} for an object of type
   * {@link RelDataType}. */
  @Test void testTypeJson() {
    int i = Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
      final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
      final RelDataType type = typeFactory.builder()
          .add("i", typeFactory.createSqlType(SqlTypeName.INTEGER))
          .nullable(false)
          .add("v", typeFactory.createSqlType(SqlTypeName.VARCHAR, 9))
          .nullable(true)
          .add("r", typeFactory.builder()
              .add("d", typeFactory.createSqlType(SqlTypeName.DATE))
              .nullable(false)
              .build())
          .nullableRecord(false)
          .build();
      final JsonBuilder jsonBuilder = new JsonBuilder();
      final RelJson json = new RelJson(jsonBuilder);
      final Object o = json.toJson(type);
      assertThat(o, notNullValue());
      final String s = jsonBuilder.toJsonString(o);
      final String expectedJson = "{\n"
          + "  \"fields\": [\n"
          + "    {\n"
          + "      \"type\": \"INTEGER\",\n"
          + "      \"nullable\": false,\n"
          + "      \"name\": \"i\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"type\": \"VARCHAR\",\n"
          + "      \"nullable\": true,\n"
          + "      \"precision\": 9,\n"
          + "      \"name\": \"v\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"fields\": {\n"
          + "        \"fields\": [\n"
          + "          {\n"
          + "            \"type\": \"DATE\",\n"
          + "            \"nullable\": false,\n"
          + "            \"name\": \"d\"\n"
          + "          }\n"
          + "        ],\n"
          + "        \"nullable\": false\n"
          + "      },\n"
          + "      \"nullable\": false,\n"
          + "      \"name\": \"r\"\n"
          + "    }\n"
          + "  ],\n"
          + "  \"nullable\": false\n"
          + "}";
      assertThat(s, is(expectedJson));
      final RelDataType type2 = json.toType(typeFactory, o);
      assertThat(type2, is(type));
      return 0;
    });
    assertThat(i, is(0));
  }

  /**
   * Unit test for {@link org.apache.calcite.rel.externalize.RelJsonWriter} on
   * a simple tree of relational expressions, consisting of a table and a
   * project including window expressions.
   */
  @Test void testWriter() {
    String s =
        Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
          rootSchema.add("hr",
              new ReflectiveSchema(new HrSchema()));
          LogicalTableScan scan =
              LogicalTableScan.create(cluster,
                  relOptSchema.getTableForMember(
                      Arrays.asList("hr", "emps")),
                  ImmutableList.of());
          final RexBuilder rexBuilder = cluster.getRexBuilder();
          LogicalFilter filter =
              LogicalFilter.create(scan,
                  rexBuilder.makeCall(
                      SqlStdOperatorTable.EQUALS,
                      rexBuilder.makeFieldAccess(
                          rexBuilder.makeRangeReference(scan),
                          "deptno", true),
                      rexBuilder.makeExactLiteral(BigDecimal.TEN)));
          final RelJsonWriter writer = new RelJsonWriter();
          final RelDataType bigIntType =
              cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
          LogicalAggregate aggregate =
              LogicalAggregate.create(filter,
                  ImmutableList.of(),
                  ImmutableBitSet.of(0),
                  null,
                  ImmutableList.of(
                      AggregateCall.create(SqlStdOperatorTable.COUNT,
                          true, false, false, ImmutableList.of(1), -1, null,
                          RelCollations.EMPTY, bigIntType, "c"),
                      AggregateCall.create(SqlStdOperatorTable.COUNT,
                          false, false, false, ImmutableList.of(), -1, null,
                          RelCollations.EMPTY, bigIntType, "d")));
          aggregate.explain(writer);
          return writer.asString();
        });
    assertThat(s, is(XX));
  }

  /**
   * Unit test for {@link org.apache.calcite.rel.externalize.RelJsonWriter} on
   * a simple tree of relational expressions, consisting of a table, a filter
   * and an aggregate node.
   */
  @Test void testWriter2() {
    String s =
        Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
          rootSchema.add("hr",
              new ReflectiveSchema(new HrSchema()));
          LogicalTableScan scan =
              LogicalTableScan.create(cluster,
                  relOptSchema.getTableForMember(
                      Arrays.asList("hr", "emps")),
                  ImmutableList.of());
          final RexBuilder rexBuilder = cluster.getRexBuilder();
          final RelDataType bigIntType =
              cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
          LogicalProject project =
              LogicalProject.create(scan,
                  ImmutableList.of(),
                  ImmutableList.of(
                      rexBuilder.makeInputRef(scan, 0),
                      rexBuilder.makeOver(bigIntType,
                          SqlStdOperatorTable.COUNT,
                          ImmutableList.of(rexBuilder.makeInputRef(scan, 0)),
                          ImmutableList.of(rexBuilder.makeInputRef(scan, 2)),
                          ImmutableList.of(
                              new RexFieldCollation(
                                  rexBuilder.makeInputRef(scan, 1), ImmutableSet.of())),
                          RexWindowBounds.UNBOUNDED_PRECEDING,
                          RexWindowBounds.CURRENT_ROW,
                          true, true, false, false, false),
                      rexBuilder.makeOver(bigIntType,
                          SqlStdOperatorTable.SUM,
                          ImmutableList.of(rexBuilder.makeInputRef(scan, 0)),
                          ImmutableList.of(rexBuilder.makeInputRef(scan, 2)),
                          ImmutableList.of(
                              new RexFieldCollation(
                                  rexBuilder.makeInputRef(scan, 1), ImmutableSet.of())),
                          RexWindowBounds.CURRENT_ROW,
                          RexWindowBounds.following(
                              rexBuilder.makeExactLiteral(BigDecimal.ONE)),
                          false, true, false, false, false)),
                  ImmutableList.of("field0", "field1", "field2"),
                  ImmutableSet.of());
          final RelJsonWriter writer = new RelJsonWriter();
          project.explain(writer);
          return writer.asString();
        });
    assertThat(s, is(XX2));
  }

  @Test void testExchange() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .exchange(RelDistributions.hash(ImmutableList.of(0, 1)))
            .build();
    final String expected = ""
        + "LogicalExchange(distribution=[hash[0, 1]])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  @Test public void testExchangeWithDistributionTraitDef() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .exchange(RelDistributions.hash(ImmutableList.of(0, 1)))
            .build();
    final String expected = ""
        + "LogicalExchange(distribution=[hash[0, 1]])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .withDistribution(true)
        .assertThatPlan(isLinux(expected));
  }

  /**
   * Unit test for {@link org.apache.calcite.rel.externalize.RelJsonReader}.
   */
  @Test void testReader() {
    String s =
        Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
          SchemaPlus schema =
              rootSchema.add("hr",
                  new ReflectiveSchema(new HrSchema()));
          final RelJsonReader reader =
              new RelJsonReader(cluster, relOptSchema, schema);
          RelNode node;
          try {
            node = reader.read(XX);
          } catch (IOException e) {
            throw TestUtil.rethrow(e);
          }
          return RelOptUtil.dumpPlan("", node, SqlExplainFormat.TEXT,
              SqlExplainLevel.EXPPLAN_ATTRIBUTES);
        });

    assertThat(s,
        isLinux("LogicalAggregate(group=[{0}], c=[COUNT(DISTINCT $1)], d=[COUNT()])\n"
            + "  LogicalFilter(condition=[=($1, 10)])\n"
            + "    LogicalTableScan(table=[[hr, emps]])\n"));
  }

  /**
   * Unit test for {@link org.apache.calcite.rel.externalize.RelJsonReader}.
   */
  @Test void testReader2() {
    String s =
        Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
          SchemaPlus schema =
              rootSchema.add("hr",
                  new ReflectiveSchema(new HrSchema()));
          final RelJsonReader reader =
              new RelJsonReader(cluster, relOptSchema, schema);
          RelNode node;
          try {
            node = reader.read(XX2);
          } catch (IOException e) {
            throw TestUtil.rethrow(e);
          }
          return RelOptUtil.dumpPlan("", node, SqlExplainFormat.TEXT,
              SqlExplainLevel.EXPPLAN_ATTRIBUTES);
        });

    assertThat(s,
        isLinux("LogicalProject(field0=[$0],"
            + " field1=[COUNT($0) OVER (PARTITION BY $2 ORDER BY $1 NULLS LAST "
            + "ROWS UNBOUNDED PRECEDING)],"
            + " field2=[SUM($0) OVER (PARTITION BY $2 ORDER BY $1 NULLS LAST "
            + "RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING)])\n"
            + "  LogicalTableScan(table=[[hr, emps]])\n"));
  }

  /**
   * Unit test for {@link org.apache.calcite.rel.externalize.RelJsonReader}.
   */
  @Test void testReaderNull() {
    String s =
        Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
          SchemaPlus schema =
              rootSchema.add("hr",
                  new ReflectiveSchema(new HrSchema()));
          final RelJsonReader reader =
              new RelJsonReader(cluster, relOptSchema, schema);
          RelNode node;
          try {
            node = reader.read(XXNULL);
          } catch (IOException e) {
            throw TestUtil.rethrow(e);
          }
          return RelOptUtil.dumpPlan("", node, SqlExplainFormat.TEXT,
              SqlExplainLevel.EXPPLAN_ATTRIBUTES);
        });

    assertThat(s,
        isLinux("LogicalAggregate(group=[{0}], agg#0=[COUNT(DISTINCT $1)], agg#1=[COUNT()])\n"
            + "  LogicalFilter(condition=[=($1, null:INTEGER)])\n"
            + "    LogicalTableScan(table=[[hr, emps]])\n"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4893">[CALCITE-4893]
   * JsonParseException happens when externalizing expressions with escape
   * character from JSON</a>. */
  @Test void testEscapeCharacter() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .project(
            b.call(new MockSqlOperatorTable.SplitFunction(),
                b.field("ENAME"), b.literal("\r")))
        .build();
    final String expected = ""
        + "LogicalProject($f0=[SPLIT($1, '\r')])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  @Test void testJsonToRex() {
    // Test simple literal without inputs
    final String jsonString1 = "{\n"
        + "  \"literal\": 10,\n"
        + "  \"type\": {\n"
        + "    \"type\": \"INTEGER\",\n"
        + "    \"nullable\": false\n"
        + "  }\n"
        + "}\n";

    assertThatReadExpressionResult(jsonString1, is("10"));

    // Test binary operator ('+') with an input and a literal
    final String jsonString2 = "{ \"op\": \n"
        + "  { \"name\": \"+\",\n"
        + "    \"kind\": \"PLUS\",\n"
        + "    \"syntax\": \"BINARY\"\n"
        + "  },\n"
        + "  \"operands\": [\n"
        + "    {\n"
        + "      \"input\": 1,\n"
        + "      \"name\": \"$1\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"literal\": 2,\n"
        + "      \"type\": { \"type\": \"INTEGER\", \"nullable\": false }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    assertThatReadExpressionResult(jsonString2, is("+(1001, 2)"));
  }

  private void assertThatReadExpressionResult(String json, Matcher<String> matcher) {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    final RelOptCluster cluster = builder.getCluster();
    final ObjectMapper mapper = new ObjectMapper();
    final TypeReference<LinkedHashMap<String, Object>> typeRef =
        new TypeReference<LinkedHashMap<String, Object>>() {
    };
    final Map<String, Object> o;
    try {
      o = mapper
          .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
          .readValue(json, typeRef);
    } catch (JsonProcessingException e) {
      throw TestUtil.rethrow(e);
    }
    RexNode e =
        RelJson.readExpression(cluster, RelWriterTest::translateInput, o);
    assertThat(e.toString(), is(matcher));
  }

  /** Intended as an instance of {@link RelJson.InputTranslator},
   * translates input {@code input} into an INTEGER literal
   * "{@code 1000 + input}". */
  private static RexNode translateInput(RelJson relJson, int input,
      Map<String, @Nullable Object> map, RelInput relInput) {
    final RexBuilder rexBuilder = relInput.getCluster().getRexBuilder();
    return rexBuilder.makeExactLiteral(BigDecimal.valueOf(1000 + input));
  }

  @Test void testTrim() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .project(
                b.alias(
                    b.call(SqlStdOperatorTable.TRIM,
                        b.literal(SqlTrimFunction.Flag.BOTH),
                        b.literal(" "),
                        b.field("ENAME")),
                    "trimmed_ename"))
            .build();
    final String expected = ""
        + "LogicalProject(trimmed_ename=[TRIM(FLAG(BOTH), ' ', $1)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  @Test void testPlusOperator() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .project(
                b.call(SqlStdOperatorTable.PLUS,
                    b.field("SAL"),
                    b.literal(10)))
            .build();
    final String expected = ""
        + "LogicalProject($f0=[+($5, 10)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  @ParameterizedTest
  @MethodSource("explainFormats")
  void testAggregateWithAlias(SqlExplainFormat format) {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    // The rel node stands for sql: SELECT max(SAL) as max_sal from EMP group by JOB;
    final RelNode rel = builder
        .scan("EMP")
        .project(
            builder.field("JOB"),
            builder.field("SAL"))
        .aggregate(
            builder.groupKey("JOB"),
            builder.max("max_sal", builder.field("SAL")))
        .project(
            builder.field("max_sal"))
        .build();
    final RelJsonWriter jsonWriter = new RelJsonWriter();
    rel.explain(jsonWriter);
    final String relJson = jsonWriter.asString();
    String s = deserializeAndDump(getSchema(rel), relJson, format);
    String expected = null;
    switch (format) {
    case TEXT:
      expected = ""
          + "LogicalProject(max_sal=[$1])\n"
          + "  LogicalAggregate(group=[{0}], max_sal=[MAX($1)])\n"
          + "    LogicalProject(JOB=[$2], SAL=[$5])\n"
          + "      LogicalTableScan(table=[[scott, EMP]])\n";
      break;
    case DOT:
      expected = "digraph {\n"
          + "\"LogicalAggregate\\ngroup = {0}\\nmax_sal = MAX($1)\\n\" -> "
          + "\"LogicalProject\\nmax_sal = $1\\n\" [label=\"0\"]\n"
          + "\"LogicalProject\\nJOB = $2\\nSAL = $5\\n\" -> \"LogicalAggregate\\ngroup = "
          + "{0}\\nmax_sal = MAX($1)\\n\" [label=\"0\"]\n"
          + "\"LogicalTableScan\\ntable = [scott, EMP]\\n\" -> \"LogicalProject\\nJOB = $2\\nSAL = "
          + "$5\\n\" [label=\"0\"]\n"
          + "}\n";
      break;
    }
    assertThat(s, isLinux(expected));
  }

  @Test void testDeserializeInvalidOperatorName() {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    final RelNode rel = builder
        .scan("EMP")
        .project(
            builder.field("JOB"),
            builder.field("SAL"))
        .aggregate(
            builder.groupKey("JOB"),
            builder.max("max_sal", builder.field("SAL")),
            builder.min("min_sal", builder.field("SAL")))
        .project(
            builder.field("max_sal"),
            builder.field("min_sal"))
        .build();
    final RelJsonWriter jsonWriter = new RelJsonWriter();
    rel.explain(jsonWriter);
    // mock a non exist SqlOperator
    String relJson = jsonWriter.asString().replace("\"name\": \"MAX\"", "\"name\": \"MAXS\"");
    assertThrows(RuntimeException.class,
        () -> deserializeAndDumpToTextFormat(getSchema(rel), relJson),
        "org.apache.calcite.runtime.CalciteException: "
            + "No operator for 'MAXS' with kind: 'MAX', syntax: 'FUNCTION' during JSON deserialization");
  }

  @Test void testAggregateWithoutAlias() {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    // The rel node stands for sql: SELECT max(SAL) from EMP group by JOB;
    final RelNode rel = builder
        .scan("EMP")
        .project(
            builder.field("JOB"),
            builder.field("SAL"))
        .aggregate(
            builder.groupKey("JOB"),
            builder.max(builder.field("SAL")))
        .project(
            builder.field(1))
        .build();
    final RelJsonWriter jsonWriter = new RelJsonWriter();
    rel.explain(jsonWriter);
    final String relJson = jsonWriter.asString();
    String s = deserializeAndDumpToTextFormat(getSchema(rel), relJson);
    final String expected = ""
        + "LogicalProject($f1=[$1])\n"
        + "  LogicalAggregate(group=[{0}], agg#0=[MAX($1)])\n"
        + "    LogicalProject(JOB=[$2], SAL=[$5])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";

    assertThat(s, isLinux(expected));
  }

  @Test void testCalc() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .let(b2 -> {
              final RexBuilder rexBuilder = b2.getRexBuilder();
              final RelNode scan = b2.build();
              final RelDataType rowType = scan.getRowType();
              final RexProgramBuilder programBuilder =
                  new RexProgramBuilder(rowType, rexBuilder);
              final RelDataTypeField field =
                  rowType.getField("SAL", false, false);
              assertThat(field, notNullValue());
              programBuilder.addIdentity();
              programBuilder.addCondition(
                  rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
                      new RexInputRef(field.getIndex(), field.getType()),
                      b2.literal(10)));
              return LogicalCalc.create(scan, programBuilder.getProgram());
            });
    final String expected = ""
        + "LogicalCalc(expr#0..7=[{inputs}], expr#8=[10], expr#9=[>($t5, $t8)],"
        + " proj#0..7=[{exprs}], $condition=[$t9])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  @ParameterizedTest
  @MethodSource("explainFormats")
  void testCorrelateQuery(SqlExplainFormat format) {
    final Holder<RexCorrelVariable> v = Holder.empty();
    final Function<RelBuilder, RelNode> relFn = b -> b.scan("EMP")
        .variable(v)
        .scan("DEPT")
        .filter(b.equals(b.field(0), b.field(v.get(), "DEPTNO")))
        .correlate(JoinRelType.INNER, v.get().id, b.field(2, 0, "DEPTNO"))
        .build();
    final String expected;
    switch (format) {
    case TEXT:
      expected = ""
          + "LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{7}])\n"
          + "  LogicalTableScan(table=[[scott, EMP]])\n"
          + "  LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
          + "    LogicalTableScan(table=[[scott, DEPT]])\n";
      break;
    case DOT:
      expected = "digraph {\n"
          + "\"LogicalTableScan\\ntable = [scott, EMP]\\n\" -> \"LogicalCorrelate\\ncorrelation = "
          + "$cor0\\njoinType = inner\\nrequiredColumns = {7\\n}\\n\" [label=\"0\"]\n"
          + "\"LogicalFilter\\ncondition = =($0, $c\\nor0.DEPTNO)\\n\" -> "
          + "\"LogicalCorrelate\\ncorrelation = $cor0\\njoinType = inner\\nrequiredColumns = "
          + "{7\\n}\\n\" [label=\"1\"]\n"
          + "\"LogicalTableScan\\ntable = [scott, DEPT\\n]\\n\" -> \"LogicalFilter\\ncondition = ="
          + "($0, $c\\nor0.DEPTNO)\\n\" [label=\"0\"]\n"
          + "}\n";
      break;
    default:
      throw new AssertionError(format);
    }
    relFn(relFn)
        .withFormat(format)
        .assertThatPlan(isLinux(expected));
  }

  @Test void testOverWithoutPartition() {
    // Equivalent SQL:
    //   SELECT count(*) OVER (ORDER BY deptno) FROM emp
    final Function<RelBuilder, RelNode> relFn = b ->
        mockCountOver(b, "EMP", ImmutableList.of(), ImmutableList.of("DEPTNO"));
    final String expected = ""
        + "LogicalProject($f0=[COUNT() OVER (ORDER BY $7 NULLS LAST "
        + "ROWS UNBOUNDED PRECEDING)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  @Test void testProjectionWithCorrelationVariables() {
    final Function<RelBuilder, RelNode> relFn = b -> b.scan("EMP")
        .project(
            ImmutableList.of(b.field("ENAME")),
            ImmutableList.of("ename"),
            true,
            ImmutableSet.of(b.getCluster().createCorrel()))
        .build();

    final String expected = "LogicalProject(variablesSet=[[$cor0]], ename=[$1])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  @Test void testOverWithoutOrderKey() {
    // Equivalent SQL:
    //   SELECT count(*) OVER (PARTITION BY deptno) FROM emp
    final Function<RelBuilder, RelNode> relFn = b ->
        mockCountOver(b, "EMP", ImmutableList.of("DEPTNO"), ImmutableList.of());
    final String expected = ""
        + "LogicalProject($f0=[COUNT() OVER (PARTITION BY $7)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  @Test void testInterval() {
    SqlIntervalQualifier sqlIntervalQualifier =
        new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.DAY, SqlParserPos.ZERO);
    BigDecimal value = new BigDecimal(86400000);
    final Function<RelBuilder, RelNode> relFn = b -> b.scan("EMP")
        .project(
            b.call(SqlStdOperatorTable.TUMBLE_END,
                b.field("HIREDATE"),
                b.getRexBuilder()
                    .makeIntervalLiteral(value, sqlIntervalQualifier)))
        .build();
    final String expected = ""
        + "LogicalProject($f0=[TUMBLE_END($4, 86400000:INTERVAL DAY)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  @Test void testUdf() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .project(
                b.call(new MockSqlOperatorTable.MyFunction(),
                    b.field("EMPNO")))
            .build();
    final String expected = ""
        + "LogicalProject($f0=[MYFUN($0)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  @ParameterizedTest
  @MethodSource("explainFormats")
  void testUDAF(SqlExplainFormat format) {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .project(b.field("ENAME"), b.field("DEPTNO"))
            .aggregate(b.groupKey("ENAME"),
                b.aggregateCall(new MockSqlOperatorTable.MyAggFunc(),
                    b.field("DEPTNO")))
        .build();
    final String expected;
    switch (format) {
    case TEXT:
      expected = ""
          + "LogicalAggregate(group=[{0}], agg#0=[myAggFunc($1)])\n"
          + "  LogicalProject(ENAME=[$1], DEPTNO=[$7])\n"
          + "    LogicalTableScan(table=[[scott, EMP]])\n";
      break;
    case DOT:
      expected = "digraph {\n"
          + "\"LogicalProject\\nENAME = $1\\nDEPTNO = $7\\n\" -> \"LogicalAggregate\\ngroup = "
          + "{0}\\nagg#0 = myAggFunc($1\\n)\\n\" [label=\"0\"]\n"
          + "\"LogicalTableScan\\ntable = [scott, EMP]\\n\" -> \"LogicalProject\\nENAME = "
          + "$1\\nDEPTNO = $7\\n\" [label=\"0\"]\n"
          + "}\n";
      break;
    default:
      throw new AssertionError(format);
    }
    relFn(relFn)
        .withFormat(format)
        .assertThatPlan(isLinux(expected));
  }

  @Test void testArrayType() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .project(
                b.call(new MockSqlOperatorTable.SplitFunction(),
                    b.field("ENAME"), b.literal(",")))
            .build();
    final String expected = ""
        + "LogicalProject($f0=[SPLIT($1, ',')])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  @Test void testMapType() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .project(
                b.call(new MockSqlOperatorTable.MapFunction(),
                    b.literal("key"), b.literal("value")))
            .build();
    final String expected = ""
        + "LogicalProject($f0=[MAP('key', 'value')])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  /** Returns the schema of a {@link org.apache.calcite.rel.core.TableScan}
   * in this plan, or null if there are no scans. */
  private static RelOptSchema getSchema(RelNode rel) {
    final Holder<@Nullable RelOptSchema> schemaHolder = Holder.empty();
    rel.accept(
        new RelShuttleImpl() {
          @Override public RelNode visit(TableScan scan) {
            schemaHolder.set(scan.getTable().getRelOptSchema());
            return super.visit(scan);
          }
        });
    return schemaHolder.get();
  }

  /**
   * Deserialize a relnode from the json string by {@link RelJsonReader},
   * and dump it to the given format.
   */
  private static String deserializeAndDump(RelOptSchema schema, String relJson,
      SqlExplainFormat format) {
    return Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
      final RelJsonReader reader =
          new RelJsonReader(cluster, schema, rootSchema);
      RelNode node;
      try {
        node = reader.read(relJson);
      } catch (IOException e) {
        throw TestUtil.rethrow(e);
      }
      return RelOptUtil.dumpPlan("", node, format,
          SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    });
  }

  private static String deserializeAndDump(RelOptCluster cluster,
      RelOptSchema schema, String relJson, SqlExplainFormat format) {
    final RelJsonReader reader = new RelJsonReader(cluster, schema, null);
    RelNode node;
    try {
      node = reader.read(relJson);
    } catch (IOException e) {
      throw TestUtil.rethrow(e);
    }
    return RelOptUtil.dumpPlan("", node, format, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
  }

  /**
   * Deserialize a relnode from the json string by {@link RelJsonReader},
   * and dump it to text format.
   */
  private static String deserializeAndDumpToTextFormat(RelOptSchema schema,
      String relJson) {
    return deserializeAndDump(schema, relJson, SqlExplainFormat.TEXT);
  }

  /**
   * Creates a mock {@link RelNode} that contains OVER. The SQL is as follows:
   *
   * <blockquote>
   * select count(*) over (partition by {@code partitionKeyNames}<br>
   * order by {@code orderKeyNames}) from {@code table}
   * </blockquote>
   *
   * @param table Table name
   * @param partitionKeyNames Partition by column names, may empty, can not be
   * null
   * @param orderKeyNames Order by column names, may empty, can not be null
   * @return RelNode for the SQL
   */
  private RelNode mockCountOver(RelBuilder builder, String table,
      List<String> partitionKeyNames, List<String> orderKeyNames) {
    final RexBuilder rexBuilder = builder.getRexBuilder();
    final RelDataType type = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    List<RexNode> partitionKeys = new ArrayList<>(partitionKeyNames.size());
    builder.scan(table);
    for (String partitionkeyName: partitionKeyNames) {
      partitionKeys.add(builder.field(partitionkeyName));
    }
    List<RexFieldCollation> orderKeys = new ArrayList<>(orderKeyNames.size());
    for (String orderKeyName: orderKeyNames) {
      orderKeys.add(new RexFieldCollation(builder.field(orderKeyName), ImmutableSet.of()));
    }
    final RelNode rel = builder
        .project(
            rexBuilder.makeOver(
                type,
                SqlStdOperatorTable.COUNT,
                ImmutableList.of(),
                partitionKeys,
                ImmutableList.copyOf(orderKeys),
                RexWindowBounds.UNBOUNDED_PRECEDING,
                RexWindowBounds.CURRENT_ROW,
                true, true, false, false, false))
        .build();
    return rel;
  }

  @Test void testHashDistributionWithoutKeys() {
    final Function<RelBuilder, RelNode> relFn = b ->
        createSortPlan(b, RelDistributions.hash(Collections.emptyList()));
    final String expected =
        "LogicalSortExchange(distribution=[hash], collation=[[0]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatJson(is(HASH_DIST_WITHOUT_KEYS))
        .assertThatPlan(isLinux(expected));
  }

  @Test void testWriteSortExchangeWithHashDistribution() {
    final Function<RelBuilder, RelNode> relFn = b ->
        createSortPlan(b, RelDistributions.hash(Lists.newArrayList(0)));
    final String expected = ""
        + "LogicalSortExchange(distribution=[hash[0]], collation=[[0]])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatJson(is(XX3))
        .assertThatPlan(isLinux(expected));
  }

  @Test void testWriteSortExchangeWithRandomDistribution() {
    final Function<RelBuilder, RelNode> relFn = b ->
        createSortPlan(b, RelDistributions.RANDOM_DISTRIBUTED);
    final String expected = ""
        + "LogicalSortExchange(distribution=[random], collation=[[0]])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  @Test void testTableModifyInsert() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
        .project(b.fields(), ImmutableList.of(), true)
            .let(b2 -> {
              final RelNode input = b2.build();
              final RelOptTable table = input.getInput(0).getTable();
              final LogicalTableModify modify =
                  LogicalTableModify.create(table,
                      (Prepare.CatalogReader) table.getRelOptSchema(),
                      input,
                      TableModify.Operation.INSERT,
                      null,
                      null,
                      false);
              return b2.push(modify);
            })
        .build();
    final String expected = ""
        + "LogicalTableModify(table=[[scott, EMP]], operation=[INSERT], flattened=[false])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], "
        + "COMM=[$6], DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  @Test void testTableModifyUpdate() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .filter(
                b.equals(b.field("JOB"), b.literal("c")))
            .let(b2 -> {
              final RelNode filter = b2.build();
              final RelOptTable table = filter.getInput(0).getTable();
              final LogicalTableModify modify =
                  LogicalTableModify.create(table,
                      (Prepare.CatalogReader) table.getRelOptSchema(),
                      filter,
                      TableModify.Operation.UPDATE,
                      ImmutableList.of("ENAME"),
                      ImmutableList.of(b2.literal("a")),
                      false);
              return b2.push(modify);
            })
            .build();
    final String expected = ""
        + "LogicalTableModify(table=[[scott, EMP]], operation=[UPDATE], updateColumnList=[[ENAME]],"
        + " sourceExpressionList=[['a']], flattened=[false])\n"
        + "  LogicalFilter(condition=[=($2, 'c')])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  @Test void testTableModifyDelete() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .filter(b.equals(b.field("JOB"), b.literal("c")))
            .let(b2 -> {
              final RelNode filter = b2.build();
              final RelOptTable table = filter.getInput(0).getTable();
              LogicalTableModify modify =
                  LogicalTableModify.create(table,
                      (Prepare.CatalogReader) table.getRelOptSchema(),
                      filter,
                      TableModify.Operation.DELETE,
                      null,
                      null,
                      false);
              return b2.push(modify);
            })
            .build();
    final String expected = ""
        + "LogicalTableModify(table=[[scott, EMP]], operation=[DELETE], flattened=[false])\n"
        + "  LogicalFilter(condition=[=($2, 'c')])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  @Test void testTableModifyMerge() {
    final Holder<RelOptTable> emp = Holder.empty();
    final Holder<RelOptTable> dept = Holder.empty();
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("DEPT")
            .let(b2 -> {
              dept.set(requireNonNull(b2.peek().getTable()));
              return b2;
            })
            .scan("EMP")
            .let(b2 -> {
              emp.set(requireNonNull(b2.peek().getTable()));
              return b2;
            })
            .join(JoinRelType.LEFT,
                b.equals(b.field(2, 0, "DEPTNO"), b.field(2, 1, "DEPTNO")))
            .project(b.literal(0),
                b.literal("x"),
                b.literal("x"),
                b.literal(0),
                b.literal("20200501 10:00:00"),
                b.literal(0),
                b.literal(0),
                b.literal(0),
                b.literal("false"),
                b.field(1, 0, 2),
                b.field(1, 0, 3),
                b.field(1, 0, 4),
                b.field(1, 0, 5),
                b.field(1, 0, 6),
                b.field(1, 0, 7),
                b.field(1, 0, 8),
                b.field(1, 0, 9),
                b.field(1, 0, 10),
                b.literal("a"))
            .let(b2 -> {
              // For SQL:
              //   MERGE INTO emp USING dept ON emp.deptno = dept.deptno
              //   WHEN MATCHED THEN
              //     UPDATE SET job = 'a'
              //   WHEN NOT MATCHED THEN
              //     INSERT VALUES (0, 'x', 'x', 0, '20200501 10:00:00',
              //         0, 0, 0, 0)
              final RelNode project = b.build();
              LogicalTableModify modify =
                  LogicalTableModify.create(emp.get(),
                      (Prepare.CatalogReader) emp.get().getRelOptSchema(),
                      project,
                      TableModify.Operation.MERGE,
                      ImmutableList.of("ENAME"),
                      null,
                      false);
              return b2.push(modify);
            })
            .build();
    final String expected = ""
        + "LogicalTableModify(table=[[scott, EMP]], operation=[MERGE], "
        + "updateColumnList=[[ENAME]], flattened=[false])\n"
        + "  LogicalProject($f0=[0], $f1=['x'], $f2=['x'], $f3=[0], $f4=['20200501 10:00:00'], "
        + "$f5=[0], $f6=[0], $f7=[0], $f8=['false'], LOC=[$2], EMPNO=[$3], ENAME=[$4], JOB=[$5], "
        + "MGR=[$6], HIREDATE=[$7], SAL=[$8], COMM=[$9], DEPTNO=[$10], $f18=['a'])\n"
        + "    LogicalJoin(condition=[=($0, $10)], joinType=[left])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    relFn(relFn)
        .assertThatPlan(isLinux(expected));
  }

  private RelNode createSortPlan(RelBuilder builder, RelDistribution distribution) {
    return builder.scan("EMP")
            .sortExchange(distribution,
                RelCollations.of(0))
            .build();
  }

  /** Test fixture. */
  static class Fixture {
    final Function<RelBuilder, RelNode> relFn;
    final boolean distribution;
    final SqlExplainFormat format;

    Fixture(Function<RelBuilder, RelNode> relFn, boolean distribution,
        SqlExplainFormat format) {
      this.relFn = relFn;
      this.distribution = distribution;
      this.format = format;
    }

    Fixture withDistribution(boolean distribution) {
      if (distribution == this.distribution) {
        return this;
      }
      return new Fixture(relFn, distribution, format);
    }

    Fixture withFormat(SqlExplainFormat format) {
      if (format == this.format) {
        return this;
      }
      return new Fixture(relFn, distribution, format);
    }

    Fixture assertThatJson(Matcher<String> matcher) {
      final FrameworkConfig config = RelBuilderTest.config().build();
      final RelBuilder b = RelBuilder.create(config);
      RelNode rel = relFn.apply(b);
      final String relJson = RelOptUtil.dumpPlan("", rel,
          SqlExplainFormat.JSON, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
      assertThat(relJson, matcher);
      return this;
    }

    Fixture assertThatPlan(Matcher<String> matcher) {
      final FrameworkConfig config = RelBuilderTest.config().build();
      final RelBuilder b = RelBuilder.create(config);
      RelNode rel = relFn.apply(b);
      final String relJson = RelOptUtil.dumpPlan("", rel,
          SqlExplainFormat.JSON, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
      final String plan;
      if (distribution) {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
        RelOptCluster cluster =
            RelOptCluster.create(planner, b.getRexBuilder());
        plan = deserializeAndDump(cluster, getSchema(rel), relJson, format);
      } else {
        plan = deserializeAndDump(getSchema(rel), relJson, format);
      }
      assertThat(plan, matcher);
      return this;
    }
  }
}
