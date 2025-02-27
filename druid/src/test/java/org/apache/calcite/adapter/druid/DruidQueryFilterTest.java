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

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.type.SqlTypeName;

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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;

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
