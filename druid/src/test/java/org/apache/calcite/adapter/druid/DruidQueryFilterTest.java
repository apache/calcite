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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.List;

import static org.hamcrest.core.Is.is;

/**
 * Tests generating Druid filters.
 */
public class DruidQueryFilterTest {

  @Test public void testInFilter() throws NoSuchMethodException,
      InvocationTargetException, IllegalAccessException, IOException {
    final Fixture f = new Fixture();
    final List<? extends RexNode> listRexNodes =
        ImmutableList.of(f.rexBuilder.makeInputRef(f.varcharRowType, 0),
            f.rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)),
            f.rexBuilder.makeExactLiteral(BigDecimal.valueOf(5)),
            f.rexBuilder.makeLiteral("value1"));

    RexNode inRexNode =
        f.rexBuilder.makeCall(SqlStdOperatorTable.IN, listRexNodes);
    Method translateFilter =
        DruidQuery.Translator.class.getDeclaredMethod("translateFilter",
            RexNode.class);
    translateFilter.setAccessible(true);
    DruidQuery.JsonInFilter returnValue =
        (DruidQuery.JsonInFilter) translateFilter.invoke(f.translatorStringKind,
            inRexNode);
    JsonFactory jsonFactory = new JsonFactory();
    final StringWriter sw = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createGenerator(sw);
    returnValue.write(jsonGenerator);
    jsonGenerator.close();

    Assert.assertThat(sw.toString(),
        is("{\"type\":\"in\",\"dimension\":\"dimensionName\","
            + "\"values\":[\"1\",\"5\",\"value1\"]}"));
  }

  @Test public void testBetweenFilterStringCase() throws NoSuchMethodException,
      InvocationTargetException, IllegalAccessException, IOException {
    final Fixture f = new Fixture();
    final List<RexNode> listRexNodes =
        ImmutableList.of(f.rexBuilder.makeLiteral(false),
            f.rexBuilder.makeInputRef(f.varcharRowType, 0),
            f.rexBuilder.makeLiteral("lower-bound"),
            f.rexBuilder.makeLiteral("upper-bound"));
    RelDataType relDataType = f.typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    RexNode betweenRexNode = f.rexBuilder.makeCall(relDataType,
        SqlStdOperatorTable.BETWEEN, listRexNodes);

    Method translateFilter =
        DruidQuery.Translator.class.getDeclaredMethod("translateFilter",
            RexNode.class);
    translateFilter.setAccessible(true);
    DruidQuery.JsonBound returnValue =
        (DruidQuery.JsonBound) translateFilter.invoke(f.translatorStringKind,
            betweenRexNode);
    JsonFactory jsonFactory = new JsonFactory();
    final StringWriter sw = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createGenerator(sw);
    returnValue.write(jsonGenerator);
    jsonGenerator.close();
    Assert.assertThat(sw.toString(),
        is("{\"type\":\"bound\",\"dimension\":\"dimensionName\",\"lower\":\"lower-bound\","
            + "\"lowerStrict\":false,\"upper\":\"upper-bound\",\"upperStrict\":false,"
            + "\"ordering\":\"lexicographic\"}"));
  }

  /** Everything a test needs for a healthy, active life. */
  static class Fixture {
    final JavaTypeFactoryImpl typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final DruidTable druidTable =
        new DruidTable(Mockito.mock(DruidSchema.class), "dataSource", null,
            ImmutableSet.<String>of(), "timestamp", null, null,
                null);
    final RelDataType varcharType =
        typeFactory.createSqlType(SqlTypeName.VARCHAR);
    final RelDataType varcharRowType = typeFactory.builder()
        .add("dimensionName", varcharType)
        .build();
    final DruidQuery.Translator translatorStringKind =
        new DruidQuery.Translator(druidTable, varcharRowType, "UTC");
  }
}

// End DruidQueryFilterTest.java
