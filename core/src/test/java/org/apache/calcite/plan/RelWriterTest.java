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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.JdbcTest;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;

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
      + "        \"op\": \"=\",\n"
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
      + "          \"agg\": \"COUNT\",\n"
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
      + "          \"agg\": \"COUNT\",\n"
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
      + "        \"op\": \"=\",\n"
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
      + "          \"agg\": \"COUNT\",\n"
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
      + "          \"agg\": \"COUNT\",\n"
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
      + "          \"op\": \"COUNT\",\n"
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
      + "          \"op\": \"SUM\",\n"
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
        isLinux("LogicalAggregate(group=[{0}], agg#0=[COUNT(DISTINCT $1)], agg#1=[COUNT()])\n"
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
            + " field1=[COUNT($0) OVER (PARTITION BY $2 ORDER BY $1 NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)],"
            + " field2=[SUM($0) OVER (PARTITION BY $2 ORDER BY $1 NULLS LAST RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING)])\n"
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
}

// End RelWriterTest.java
