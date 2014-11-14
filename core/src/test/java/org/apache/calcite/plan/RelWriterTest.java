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
package org.eigenbase.relopt;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.sql.SqlExplainLevel;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Util;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.test.JdbcTest;
import net.hydromatic.optiq.tools.Frameworks;
import net.hydromatic.optiq.util.BitSets;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

/**
 * Unit test for {@link org.eigenbase.rel.RelJson}.
 */
public class RelWriterTest {
  public static final String XX =
      "{\n"
      + "  rels: [\n"
      + "    {\n"
      + "      id: \"0\",\n"
      + "      relOp: \"TableAccessRel\",\n"
      + "      table: [\n"
      + "        \"hr\",\n"
      + "        \"emps\"\n"
      + "      ],\n"
      + "      inputs: []\n"
      + "    },\n"
      + "    {\n"
      + "      id: \"1\",\n"
      + "      relOp: \"FilterRel\",\n"
      + "      condition: {\n"
      + "        op: \"=\",\n"
      + "        operands: [\n"
      + "          {\n"
      + "            input: 1\n"
      + "          },\n"
      + "          10\n"
      + "        ]\n"
      + "      }\n"
      + "    },\n"
      + "    {\n"
      + "      id: \"2\",\n"
      + "      relOp: \"AggregateRel\",\n"
      + "      group: [\n"
      + "        0\n"
      + "      ],\n"
      + "      aggs: [\n"
      + "        {\n"
      + "          agg: \"COUNT\",\n"
      + "          type: {\n"
      + "            type: \"BIGINT\",\n"
      + "            nullable: false\n"
      + "          },\n"
      + "          distinct: true,\n"
      + "          operands: [\n"
      + "            1\n"
      + "          ]\n"
      + "        },\n"
      + "        {\n"
      + "          agg: \"COUNT\",\n"
      + "          type: {\n"
      + "            type: \"BIGINT\",\n"
      + "            nullable: false\n"
      + "          },\n"
      + "          distinct: false,\n"
      + "          operands: []\n"
      + "        }\n"
      + "      ]\n"
      + "    }\n"
      + "  ]\n"
      + "}";

  /**
   * Unit test for {@link org.eigenbase.rel.RelJsonWriter} on
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
                TableAccessRel table =
                    new TableAccessRel(cluster,
                        relOptSchema.getTableForMember(
                            Arrays.asList("hr", "emps")));
                final RexBuilder rexBuilder = cluster.getRexBuilder();
                FilterRel filter =
                    new FilterRel(cluster, table,
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.EQUALS,
                            rexBuilder.makeFieldAccess(
                                rexBuilder.makeRangeReference(table),
                                "deptno", true),
                            rexBuilder.makeExactLiteral(BigDecimal.TEN)));
                final RelJsonWriter writer = new RelJsonWriter();
                final RelDataType intType =
                    cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
                final RelDataType bigIntType =
                    cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
                AggregateRel aggregate =
                    new AggregateRel(cluster, filter, BitSets.of(0),
                        ImmutableList.of(
                            new AggregateCall(SqlStdOperatorTable.COUNT,
                                true, ImmutableList.of(1), bigIntType, "c"),
                            new AggregateCall(SqlStdOperatorTable.COUNT,
                                false, ImmutableList.<Integer>of(), bigIntType,
                                "d")));
                aggregate.explain(writer);
                return writer.asString();
              }
            });
    assertThat(s, is(XX));
  }

  /**
   * Unit test for {@link org.eigenbase.rel.RelJsonReader}.
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
                return RelOptUtil.dumpPlan(
                    "",
                    node,
                    false,
                    SqlExplainLevel.EXPPLAN_ATTRIBUTES);
              }
            });

    assertThat(Util.toLinux(s), is(
        "AggregateRel(group=[{0}], agg#0=[COUNT(DISTINCT $1)], agg#1=[COUNT()])\n"
        + "  FilterRel(condition=[=($1, 10)])\n"
        + "    TableAccessRel(table=[[hr, emps]])\n"));
  }
}

// End RelWriterTest.java
