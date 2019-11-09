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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
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
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.JdbcTest;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for {@link org.apache.calcite.rel.externalize.RelJson}.
 */
public class RelWriterTest {
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

  /**
   * Unit test for {@link org.apache.calcite.rel.externalize.RelJsonWriter} on
   * a simple tree of relational expressions, consisting of a table and a
   * project including window expressions.
   */
  @Test public void testWriter() {
    String s =
        Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
          rootSchema.add("hr",
              new ReflectiveSchema(new JdbcTest.HrSchema()));
          LogicalTableScan scan =
              LogicalTableScan.create(cluster,
                  relOptSchema.getTableForMember(
                      Arrays.asList("hr", "emps")));
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
              LogicalAggregate.create(filter, ImmutableBitSet.of(0), null,
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
  @Test public void testWriter2() {
    String s =
        Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
          rootSchema.add("hr",
              new ReflectiveSchema(new JdbcTest.HrSchema()));
          LogicalTableScan scan =
              LogicalTableScan.create(cluster,
                  relOptSchema.getTableForMember(
                      Arrays.asList("hr", "emps")));
          final RexBuilder rexBuilder = cluster.getRexBuilder();
          final RelDataType bigIntType =
              cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
          LogicalProject project =
              LogicalProject.create(scan,
                  ImmutableList.of(
                      rexBuilder.makeInputRef(scan, 0),
                      rexBuilder.makeOver(bigIntType,
                          SqlStdOperatorTable.COUNT,
                          ImmutableList.of(rexBuilder.makeInputRef(scan, 0)),
                          ImmutableList.of(rexBuilder.makeInputRef(scan, 2)),
                          ImmutableList.of(
                              new RexFieldCollation(
                                  rexBuilder.makeInputRef(scan, 1), ImmutableSet.of())),
                          RexWindowBound.create(
                              SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null),
                          RexWindowBound.create(
                              SqlWindow.createCurrentRow(SqlParserPos.ZERO), null),
                          true, true, false, false, false),
                      rexBuilder.makeOver(bigIntType,
                          SqlStdOperatorTable.SUM,
                          ImmutableList.of(rexBuilder.makeInputRef(scan, 0)),
                          ImmutableList.of(rexBuilder.makeInputRef(scan, 2)),
                          ImmutableList.of(
                              new RexFieldCollation(
                                  rexBuilder.makeInputRef(scan, 1), ImmutableSet.of())),
                          RexWindowBound.create(
                              SqlWindow.createCurrentRow(SqlParserPos.ZERO), null),
                          RexWindowBound.create(null,
                              rexBuilder.makeCall(
                                  SqlWindow.FOLLOWING_OPERATOR,
                                  rexBuilder.makeExactLiteral(BigDecimal.ONE))),
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
  @Test public void testReader() {
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
  @Test public void testReader2() {
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
            + " field1=[COUNT($0) OVER (PARTITION BY $2 ORDER BY $1 NULLS LAST ROWS BETWEEN"
            + " UNBOUNDED PRECEDING AND CURRENT ROW)],"
            + " field2=[SUM($0) OVER (PARTITION BY $2 ORDER BY $1 NULLS LAST RANGE BETWEEN"
            + " CURRENT ROW AND 1 FOLLOWING)])\n"
            + "  LogicalTableScan(table=[[hr, emps]])\n"));
  }

  /**
   * Unit test for {@link org.apache.calcite.rel.externalize.RelJsonReader}.
   */
  @Test public void testReaderNull() {
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

  @Test public void testTrim() {
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

  @Test public void testPlusOperator() {
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

  @Test public void testAggregateWithAlias() {
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

  @Test public void testAggregateWithoutAlias() {
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

  @Test public void testCalc() {
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

  @Test public void testCorrelateQuery() {
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

  @Test public void testOverWithoutPartition() {
    // The rel stands for the sql of "select count(*) over (order by deptno) from EMP"
    final RelNode rel = mockCountOver("EMP", ImmutableList.of(), ImmutableList.of("DEPTNO"));
    String relJson = RelOptUtil.dumpPlan("", rel, SqlExplainFormat.JSON,
        SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    String s = deserializeAndDumpToTextFormat(getSchema(rel), relJson);
    final String expected = ""
        + "LogicalProject($f0=[COUNT() OVER (ORDER BY $7 NULLS LAST ROWS"
        + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(s, isLinux(expected));
  }

  @Test public void testOverWithoutOrderKey() {
    // The rel stands for the sql of "select count(*) over (partition by DEPTNO) from EMP"
    final RelNode rel = mockCountOver("EMP", ImmutableList.of("DEPTNO"), ImmutableList.of());
    String relJson = RelOptUtil.dumpPlan("", rel, SqlExplainFormat.JSON,
        SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    String s = deserializeAndDumpToTextFormat(getSchema(rel), relJson);
    final String expected = ""
        + "LogicalProject($f0=[COUNT() OVER"
        + " (PARTITION BY $7 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(s, isLinux(expected));
  }

  @Test public void testInterval() {
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
                RexWindowBound.create(
                    SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null),
                RexWindowBound.create(
                    SqlWindow.createCurrentRow(SqlParserPos.ZERO), null),
                true, true, false, false, false))
        .build();
    return rel;
  }
}

// End RelWriterTest.java
