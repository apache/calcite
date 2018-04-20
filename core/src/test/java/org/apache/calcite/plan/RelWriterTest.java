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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.JdbcTest;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

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
      + "          10\n"
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

  /**
   * Unit test for {@link org.apache.calcite.rel.externalize.RelJsonWriter} on
   * a simple tree of relational expressions, consisting of a table, a filter
   * and an aggregate node.
   */
  @Test public void testWriter() {
    String s =
        Frameworks.withPlanner(
            new Frameworks.PlannerAction<String>() {
              public String apply(RelOptCluster cluster,
                  RelOptSchema relOptSchema, SchemaPlus rootSchema) {
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
                                true, false, ImmutableList.of(1), -1,
                                bigIntType, "c"),
                            AggregateCall.create(SqlStdOperatorTable.COUNT,
                                false, false, ImmutableList.<Integer>of(), -1,
                                bigIntType, "d")));
                aggregate.explain(writer);
                return writer.asString();
              }
            });
    assertThat(s, is(XX));
  }

  /**
   * Unit test for {@link org.apache.calcite.rel.externalize.RelJsonReader}.
   */
  @Test public void testReader() {
    String s =
        Frameworks.withPlanner(
            new Frameworks.PlannerAction<String>() {
              public String apply(RelOptCluster cluster,
                  RelOptSchema relOptSchema, SchemaPlus rootSchema) {
                SchemaPlus schema =
                    rootSchema.add("hr",
                        new ReflectiveSchema(new JdbcTest.HrSchema()));
                final RelJsonReader reader =
                    new RelJsonReader(cluster, relOptSchema, schema);
                RelNode node;
                try {
                  node = reader.read(XX);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
                return RelOptUtil.dumpPlan("", node, SqlExplainFormat.TEXT,
                    SqlExplainLevel.EXPPLAN_ATTRIBUTES);
              }
            });

    assertThat(s,
        isLinux("LogicalAggregate(group=[{0}], agg#0=[COUNT(DISTINCT $1)], agg#1=[COUNT()])\n"
            + "  LogicalFilter(condition=[=($1, 10)])\n"
            + "    LogicalTableScan(table=[[hr, emps]])\n"));
  }
}

// End RelWriterTest.java
