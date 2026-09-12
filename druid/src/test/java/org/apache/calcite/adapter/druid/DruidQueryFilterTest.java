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
package org.apache.calcite.adapter.druid;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Tests generating Druid filters.
 */
class DruidQueryFilterTest {

  private DruidQuery druidQuery;

  @BeforeEach void testSetup() {
    druidQuery = Mockito.mock(DruidQuery.class);
    final CalciteConnectionConfig connectionConfigMock = Mockito
        .mock(CalciteConnectionConfig.class);
    Mockito.when(connectionConfigMock.timeZone()).thenReturn("UTC");
    Mockito.when(druidQuery.getConnectionConfig()).thenReturn(connectionConfigMock);
    Mockito.when(druidQuery.getDruidTable())
        .thenReturn(
            new DruidTable(Mockito.mock(DruidSchema.class), "dataSource", null,
                ImmutableSet.of(), "timestamp", null, null,
                null));
  }

  @Test void testInFilter() throws IOException {
    final Fixture f = new Fixture();
    final List<? extends RexNode> listRexNodes =
        ImmutableList.of(f.rexBuilder.makeInputRef(f.varcharRowType, 0),
            f.rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)),
            f.rexBuilder.makeExactLiteral(BigDecimal.valueOf(5)),
            f.rexBuilder.makeLiteral("value1"));

    RexNode inRexNode =
        f.rexBuilder.makeCall(SqlInternalOperators.DRUID_IN, listRexNodes);
    DruidJsonFilter returnValue = DruidJsonFilter
        .toDruidFilters(inRexNode, f.varcharRowType, druidQuery, f.rexBuilder);
    assertThat("Filter is null", returnValue, notNullValue());
    JsonFactory jsonFactory = new JsonFactory();
    final StringWriter sw = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createGenerator(sw);
    returnValue.write(jsonGenerator);
    jsonGenerator.close();

    assertThat(sw,
        hasToString("{\"type\":\"in\",\"dimension\":\"dimensionName\","
            + "\"values\":[\"1\",\"5\",\"value1\"]}"));
  }

  @Test void testNotInFilter() throws IOException {
    final Fixture f = new Fixture();
    final List<? extends RexNode> listRexNodes =
        ImmutableList.of(f.rexBuilder.makeInputRef(f.varcharRowType, 0),
            f.rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)),
            f.rexBuilder.makeExactLiteral(BigDecimal.valueOf(5)),
            f.rexBuilder.makeLiteral("value1"));

    RexNode notInRexNode =
        f.rexBuilder.makeCall(SqlInternalOperators.DRUID_NOT_IN, listRexNodes);
    DruidJsonFilter returnValue = DruidJsonFilter
        .toDruidFilters(notInRexNode, f.varcharRowType, druidQuery, f.rexBuilder);
    assertThat("Filter is null", returnValue, notNullValue());
    JsonFactory jsonFactory = new JsonFactory();
    final StringWriter sw = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createGenerator(sw);
    returnValue.write(jsonGenerator);
    jsonGenerator.close();

    assertThat(sw,
        hasToString("{\"type\":\"not\",\"field\":{\"type\":\"in\",\"dimension\":"
            + "\"dimensionName\",\"values\":[\"1\",\"5\",\"value1\"]}}"));
  }

  @Test void testBetweenFilterStringCase() throws IOException {
    final Fixture f = new Fixture();
    final List<RexNode> listRexNodes =
        ImmutableList.of(f.rexBuilder.makeLiteral(false),
            f.rexBuilder.makeInputRef(f.varcharRowType, 0),
            f.rexBuilder.makeLiteral("lower-bound"),
            f.rexBuilder.makeLiteral("upper-bound"));
    RelDataType relDataType = f.typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    RexNode betweenRexNode =
        f.rexBuilder.makeCall(relDataType,
            SqlInternalOperators.DRUID_BETWEEN, listRexNodes);

    DruidJsonFilter returnValue = DruidJsonFilter
        .toDruidFilters(betweenRexNode, f.varcharRowType, druidQuery, f.rexBuilder);
    assertThat("Filter is null", returnValue, notNullValue());
    JsonFactory jsonFactory = new JsonFactory();
    final StringWriter sw = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createGenerator(sw);
    returnValue.write(jsonGenerator);
    jsonGenerator.close();
    assertThat(sw,
        hasToString("{\"type\":\"bound\",\"dimension\":\"dimensionName\","
            + "\"lower\":\"lower-bound\",\"lowerStrict\":false,"
            + "\"upper\":\"upper-bound\",\"upperStrict\":false,"
            + "\"ordering\":\"lexicographic\"}"));
  }

  @Test void testOrWithExtractRetainsFilter() {
    final Fixture f = new Fixture();
    final HepPlanner planner =
        new HepPlanner(new HepProgramBuilder().addRuleInstance(DruidRules.FILTER).build(),
            Contexts.of(new CalciteConnectionConfigImpl(new Properties())));
    final RelOptCluster cluster = RelOptCluster.create(planner, f.rexBuilder);
    final RelDataType rowType = f.typeFactory.builder()
        .add("timestamp", SqlTypeName.TIMESTAMP)
        .build();
    final DruidTable table =
        new DruidTable(Mockito.mock(DruidSchema.class), "events", factory -> rowType,
            ImmutableSet.of(), "timestamp", null, null, null);
    final RelOptTable relOptTable =
        RelOptTableImpl.create(null, rowType, ImmutableList.of("events"), table,
            clazz -> null);
    final RelNode scan = LogicalTableScan.create(cluster, relOptTable, ImmutableList.of());
    final DruidQuery query =
        DruidQuery.create(cluster, cluster.traitSet().plus(BindableConvention.INSTANCE),
            relOptTable, table, ImmutableList.of(scan));
    final RexNode timestamp = f.rexBuilder.makeInputRef(scan, 0);
    final RexNode condition =
        f.rexBuilder.makeCall(
            SqlStdOperatorTable.OR, f.rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, timestamp,
            f.rexBuilder.makeTimestampLiteral(new TimestampString("2020-01-01 00:00:00"), 0)),
        f.rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            f.rexBuilder.makeCall(SqlStdOperatorTable.EXTRACT,
                f.rexBuilder.makeFlag(TimeUnitRange.DAY), timestamp),
            f.rexBuilder.makeExactLiteral(BigDecimal.valueOf(15))));
    planner.setRoot(LogicalFilter.create(query, condition));

    final DruidQuery result = assertInstanceOf(DruidQuery.class, planner.findBestExp());
    assertThat(result.intervals, is(query.intervals));
    final Filter filter = assertInstanceOf(Filter.class, result.getTopNode());
    assertThat(filter.getCondition(), is(condition));
  }

  /** Everything a test needs for a healthy, active life. */
  static class Fixture {
    final JavaTypeFactoryImpl typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final DruidTable druidTable =
        new DruidTable(Mockito.mock(DruidSchema.class), "dataSource", null,
            ImmutableSet.of(), "timestamp", null, null,
                null);
    final RelDataType varcharType =
        typeFactory.createSqlType(SqlTypeName.VARCHAR);
    final RelDataType varcharRowType = typeFactory.builder()
        .add("dimensionName", varcharType)
        .build();
  }
}
