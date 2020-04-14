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
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
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
import org.apache.calcite.test.JdbcTest;
import org.apache.calcite.test.MockSqlOperatorTable;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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

  /**
   * Unit test for {@link org.apache.calcite.rel.externalize.RelJsonWriter} on
   * a simple tree of relational expressions, consisting of a table and a
   * project including window expressions.
   */
  @Test void testWriter() {
    String s =
        Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
          rootSchema.add("hr",
              new ReflectiveSchema(new JdbcTest.HrSchema()));
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
                          true, false, false, ImmutableList.of(1), -1,
                          RelCollations.EMPTY, bigIntType, "c"),
                      AggregateCall.create(SqlStdOperatorTable.COUNT,
                          false, false, false, ImmutableList.of(), -1,
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
              new ReflectiveSchema(new JdbcTest.HrSchema()));
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
                  ImmutableList.of("field0", "field1", "field2"));
          final RelJsonWriter writer = new RelJsonWriter();
          project.explain(writer);
          return writer.asString();
        });
    assertThat(s, is(XX2));
  }

  /**
   * Unit test for {@link org.apache.calcite.rel.externalize.RelJsonReader}.
   */
  @Test void testReader() {
    String s =
        Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
          SchemaPlus schema =
              rootSchema.add("hr",
                  new ReflectiveSchema(new JdbcTest.HrSchema()));
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
                  new ReflectiveSchema(new JdbcTest.HrSchema()));
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
                  new ReflectiveSchema(new JdbcTest.HrSchema()));
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

  @Test void testTrim() {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder b = RelBuilder.create(config);
    final RelNode rel =
        b.scan("EMP")
            .project(
                b.alias(
                    b.call(SqlStdOperatorTable.TRIM,
                        b.literal(SqlTrimFunction.Flag.BOTH),
                        b.literal(" "),
                        b.field("ENAME")),
                    "trimmed_ename"))
            .build();

    RelJsonWriter jsonWriter = new RelJsonWriter();
    rel.explain(jsonWriter);
    String relJson = jsonWriter.asString();
    final RelOptSchema schema = getSchema(rel);
    final String s = deserializeAndDumpToTextFormat(schema, relJson);
    final String expected = ""
        + "LogicalProject(trimmed_ename=[TRIM(FLAG(BOTH), ' ', $1)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(s, isLinux(expected));
  }

  @Test void testPlusOperator() {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    final RelNode rel = builder
        .scan("EMP")
        .project(
            builder.call(SqlStdOperatorTable.PLUS,
                builder.field("SAL"),
                builder.literal(10)))
        .build();
    RelJsonWriter jsonWriter = new RelJsonWriter();
    rel.explain(jsonWriter);
    String relJson = jsonWriter.asString();
    String s = deserializeAndDumpToTextFormat(getSchema(rel), relJson);
    final String expected = ""
        + "LogicalProject($f0=[+($5, 10)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(s, isLinux(expected));
  }

  @Test void testAggregateWithAlias() {
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
    String s = deserializeAndDumpToTextFormat(getSchema(rel), relJson);
    final String expected = ""
        + "LogicalProject(max_sal=[$1])\n"
        + "  LogicalAggregate(group=[{0}], max_sal=[MAX($1)])\n"
        + "    LogicalProject(JOB=[$2], SAL=[$5])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";

    assertThat(s, isLinux(expected));
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
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    final RexBuilder rexBuilder = builder.getRexBuilder();
    final LogicalTableScan scan = (LogicalTableScan) builder.scan("EMP").build();
    final RexProgramBuilder programBuilder =
        new RexProgramBuilder(scan.getRowType(), rexBuilder);
    final RelDataTypeField field = scan.getRowType().getField("SAL", false, false);
    programBuilder.addIdentity();
    programBuilder.addCondition(
        rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
            new RexInputRef(field.getIndex(), field.getType()),
            builder.literal(10)));
    final LogicalCalc calc = LogicalCalc.create(scan, programBuilder.getProgram());
    String relJson = RelOptUtil.dumpPlan("", calc,
        SqlExplainFormat.JSON, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    String s =
        Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
          final RelJsonReader reader = new RelJsonReader(
              cluster, getSchema(calc), rootSchema);
          RelNode node;
          try {
            node = reader.read(relJson);
          } catch (IOException e) {
            throw TestUtil.rethrow(e);
          }
          return RelOptUtil.dumpPlan("", node, SqlExplainFormat.TEXT,
              SqlExplainLevel.EXPPLAN_ATTRIBUTES);
        });
    final String expected =
        "LogicalCalc(expr#0..7=[{inputs}], expr#8=[10], expr#9=[>($t5, $t8)],"
            + " proj#0..7=[{exprs}], $condition=[$t9])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(s, isLinux(expected));
  }

  @Test void testCorrelateQuery() {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    final Holder<RexCorrelVariable> v = Holder.of(null);
    RelNode relNode = builder.scan("EMP")
        .variable(v)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field(0), builder.field(v.get(), "DEPTNO")))
        .correlate(
            JoinRelType.INNER, v.get().id, builder.field(2, 0, "DEPTNO"))
        .build();
    RelJsonWriter jsonWriter = new RelJsonWriter();
    relNode.explain(jsonWriter);
    final String relJson = jsonWriter.asString();
    String s = deserializeAndDumpToTextFormat(getSchema(relNode), relJson);
    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";

    assertThat(s, isLinux(expected));
  }

  @Test void testOverWithoutPartition() {
    // The rel stands for the sql of "select count(*) over (order by deptno) from EMP"
    final RelNode rel = mockCountOver("EMP", ImmutableList.of(), ImmutableList.of("DEPTNO"));
    String relJson = RelOptUtil.dumpPlan("", rel, SqlExplainFormat.JSON,
        SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    String s = deserializeAndDumpToTextFormat(getSchema(rel), relJson);
    final String expected = ""
        + "LogicalProject($f0=[COUNT() OVER (ORDER BY $7 NULLS LAST "
        + "ROWS UNBOUNDED PRECEDING)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(s, isLinux(expected));
  }

  @Test void testOverWithoutOrderKey() {
    // The rel stands for the sql of "select count(*) over (partition by DEPTNO) from EMP"
    final RelNode rel = mockCountOver("EMP", ImmutableList.of("DEPTNO"), ImmutableList.of());
    String relJson = RelOptUtil.dumpPlan("", rel, SqlExplainFormat.JSON,
        SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    String s = deserializeAndDumpToTextFormat(getSchema(rel), relJson);
    final String expected = ""
        + "LogicalProject($f0=[COUNT() OVER (PARTITION BY $7)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(s, isLinux(expected));
  }

  @Test void testInterval() {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    SqlIntervalQualifier sqlIntervalQualifier =
        new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.DAY, SqlParserPos.ZERO);
    BigDecimal value = new BigDecimal(86400000);
    RexLiteral intervalLiteral = builder.getRexBuilder()
        .makeIntervalLiteral(value, sqlIntervalQualifier);
    final RelNode rel = builder
        .scan("EMP")
        .project(
            builder.call(
                SqlStdOperatorTable.TUMBLE_END,
                builder.field("HIREDATE"),
                intervalLiteral))
        .build();
    RelJsonWriter jsonWriter = new RelJsonWriter();
    rel.explain(jsonWriter);
    String relJson = jsonWriter.asString();
    String s = deserializeAndDumpToTextFormat(getSchema(rel), relJson);
    final String expected = ""
        + "LogicalProject($f0=[TUMBLE_END($4, 86400000:INTERVAL DAY)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(s, isLinux(expected));
  }

  @Test void testUdf() {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    final RelNode rel = builder
        .scan("EMP")
        .project(
            builder.call(new MockSqlOperatorTable.MyFunction(),
                builder.field("EMPNO")))
        .build();
    String relJson = RelOptUtil.dumpPlan("", rel,
        SqlExplainFormat.JSON, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    String s = deserializeAndDumpToTextFormat(getSchema(rel), relJson);
    final String expected = ""
        + "LogicalProject($f0=[MYFUN($0)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(s, isLinux(expected));
  }

  /** Returns the schema of a {@link org.apache.calcite.rel.core.TableScan}
   * in this plan, or null if there are no scans. */
  private RelOptSchema getSchema(RelNode rel) {
    final Holder<RelOptSchema> schemaHolder = Holder.of(null);
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
   * and dump it to text format.
   */
  private String deserializeAndDumpToTextFormat(RelOptSchema schema, String relJson) {
    String s =
        Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
          final RelJsonReader reader = new RelJsonReader(
              cluster, schema, rootSchema);
          RelNode node;
          try {
            node = reader.read(relJson);
          } catch (IOException e) {
            throw TestUtil.rethrow(e);
          }
          return RelOptUtil.dumpPlan("", node, SqlExplainFormat.TEXT,
              SqlExplainLevel.EXPPLAN_ATTRIBUTES);
        });
    return s;
  }

  /**
   * Mock a {@link RelNode} for sql:
   * select count(*) over (partition by {@code partitionKeyNames}
   * order by {@code orderKeyNames}) from {@code table}
   * @param table Table name
   * @param partitionKeyNames Partition by column names, may empty, can not be null
   * @param orderKeyNames Order by column names, may empty, can not be null
   * @return RelNode for the sql
   */
  private RelNode mockCountOver(String table,
      List<String> partitionKeyNames, List<String> orderKeyNames) {

    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
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

  @Test void testWriteSortExchangeWithHashDistribution() {
    final RelNode root = createSortPlan(RelDistributions.hash(Lists.newArrayList(0)));
    final RelJsonWriter writer = new RelJsonWriter();
    root.explain(writer);
    final String json = writer.asString();
    assertThat(json, is(XX3));

    final String s = deserializeAndDumpToTextFormat(getSchema(root), json);
    final String expected =
        "LogicalSortExchange(distribution=[hash[0]], collation=[[0]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(s, isLinux(expected));
  }

  @Test void testWriteSortExchangeWithRandomDistribution() {
    final RelNode root = createSortPlan(RelDistributions.RANDOM_DISTRIBUTED);
    final RelJsonWriter writer = new RelJsonWriter();
    root.explain(writer);
    final String json = writer.asString();
    final String s = deserializeAndDumpToTextFormat(getSchema(root), json);
    final String expected =
        "LogicalSortExchange(distribution=[random], collation=[[0]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(s, isLinux(expected));
  }

  @Test void testTableModifyInsert() {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    RelNode project = builder
        .scan("EMP")
        .project(builder.fields(), ImmutableList.of(), true)
        .build();
    LogicalTableModify modify = LogicalTableModify.create(
        project.getInput(0).getTable(),
        (Prepare.CatalogReader) project.getInput(0).getTable().getRelOptSchema(),
        project,
        TableModify.Operation.INSERT,
        null,
        null,
        false);
    String relJson = RelOptUtil.dumpPlan("", modify,
        SqlExplainFormat.JSON, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    String s = deserializeAndDumpToTextFormat(getSchema(modify), relJson);
    final String expected = ""
        + "LogicalTableModify(table=[[scott, EMP]], operation=[INSERT], flattened=[false])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], "
        + "COMM=[$6], DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(s, isLinux(expected));
  }

  @Test void testTableModifyUpdate() {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    RelNode filter = builder
        .scan("EMP")
        .filter(
            builder.call(
                SqlStdOperatorTable.EQUALS,
                builder.field("JOB"),
                builder.literal("c")))
        .build();
    LogicalTableModify modify = LogicalTableModify.create(
        filter.getInput(0).getTable(),
        (Prepare.CatalogReader) filter.getInput(0).getTable().getRelOptSchema(),
        filter,
        TableModify.Operation.UPDATE,
        ImmutableList.of("ENAME"),
        ImmutableList.of(builder.literal("a")),
        false);
    String relJson = RelOptUtil.dumpPlan("", modify,
        SqlExplainFormat.JSON, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    String s = deserializeAndDumpToTextFormat(getSchema(modify), relJson);
    final String expected = ""
        + "LogicalTableModify(table=[[scott, EMP]], operation=[UPDATE], updateColumnList=[[ENAME]],"
        + " sourceExpressionList=[['a']], flattened=[false])\n"
        + "  LogicalFilter(condition=[=($2, 'c')])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(s, isLinux(expected));
  }

  @Test void testTableModifyDelete() {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    RelNode filter = builder
        .scan("EMP")
        .filter(
            builder.call(
                SqlStdOperatorTable.EQUALS,
                builder.field("JOB"),
                builder.literal("c")))
        .build();
    LogicalTableModify modify = LogicalTableModify.create(
        filter.getInput(0).getTable(),
        (Prepare.CatalogReader) filter.getInput(0).getTable().getRelOptSchema(),
        filter,
        TableModify.Operation.DELETE,
        null,
        null,
        false);
    String relJson = RelOptUtil.dumpPlan("", modify,
        SqlExplainFormat.JSON, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    String s = deserializeAndDumpToTextFormat(getSchema(modify), relJson);
    final String expected = ""
        + "LogicalTableModify(table=[[scott, EMP]], operation=[DELETE], flattened=[false])\n"
        + "  LogicalFilter(condition=[=($2, 'c')])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(s, isLinux(expected));
  }

  @Test void testTableModifyMerge() {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    RelNode deptScan = builder.scan("DEPT").build();
    RelNode empScan = builder.scan("EMP").build();
    builder.push(deptScan);
    builder.push(empScan);
    RelNode project = builder
        .join(JoinRelType.LEFT,
            builder.call(
                SqlStdOperatorTable.EQUALS,
                builder.field(2, 0, "DEPTNO"),
                builder.field(2, 1, "DEPTNO")))
        .project(
            builder.literal(0),
            builder.literal("x"),
            builder.literal("x"),
            builder.literal(0),
            builder.literal("20200501 10:00:00"),
            builder.literal(0),
            builder.literal(0),
            builder.literal(0),
            builder.literal("false"),
            builder.field(1, 0, 2),
            builder.field(1, 0, 3),
            builder.field(1, 0, 4),
            builder.field(1, 0, 5),
            builder.field(1, 0, 6),
            builder.field(1, 0, 7),
            builder.field(1, 0, 8),
            builder.field(1, 0, 9),
            builder.field(1, 0, 10),
            builder.literal("a"))
        .build();
    // for sql:
    // merge into emp using dept on emp.deptno = dept.deptno
    // when matched then update set job = 'a'
    // when not matched then insert values(0, 'x', 'x', 0, '20200501 10:00:00', 0, 0, 0, 0)
    LogicalTableModify modify = LogicalTableModify.create(
        empScan.getTable(),
        (Prepare.CatalogReader) empScan.getTable().getRelOptSchema(),
        project,
        TableModify.Operation.MERGE,
        ImmutableList.of("ENAME"),
        null,
        false);
    String relJson = RelOptUtil.dumpPlan("", modify,
        SqlExplainFormat.JSON, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    String s = deserializeAndDumpToTextFormat(getSchema(modify), relJson);
    final String expected = ""
        + "LogicalTableModify(table=[[scott, EMP]], operation=[MERGE], "
        + "updateColumnList=[[ENAME]], flattened=[false])\n"
        + "  LogicalProject($f0=[0], $f1=['x'], $f2=['x'], $f3=[0], $f4=['20200501 10:00:00'], "
        + "$f5=[0], $f6=[0], $f7=[0], $f8=['false'], LOC=[$2], EMPNO=[$3], ENAME=[$4], JOB=[$5], "
        + "MGR=[$6], HIREDATE=[$7], SAL=[$8], COMM=[$9], DEPTNO=[$10], $f18=['a'])\n"
        + "    LogicalJoin(condition=[=($0, $10)], joinType=[left])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(s, isLinux(expected));
  }

  private RelNode createSortPlan(RelDistribution distribution) {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    return builder.scan("EMP")
            .sortExchange(distribution,
                RelCollations.of(0))
            .build();
  }
}
