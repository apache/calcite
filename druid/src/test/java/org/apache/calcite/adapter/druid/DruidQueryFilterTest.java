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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.List;

/**
 * Test class for druid filter generation
 */
public class DruidQueryFilterTest {

  private JavaTypeFactory typeFactory;

  private RexBuilder rexBuilder;

  private DruidTable druidTable;

  private RelDataType varCharRowType;

  private DruidQuery.Translator translatorStringKind;

  @Before
  public void setup() {
    typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
    druidTable = new DruidTable(
            new DruidSchema("", "", false),
            "dataSource",
            null,
            ImmutableSet.<String>of(),
            "timestamp",
            null
    );
    final RelDataType varCharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    varCharRowType = typeFactory.builder()
            .add("dimensionName", varCharType)
            .build();
    translatorStringKind = new DruidQuery.Translator(druidTable, varCharRowType);
  }

  @Test
  public void testInFilter()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
          IOException {
    final List<? extends RexNode> listRexNodes = Lists.newArrayList(
            rexBuilder.makeInputRef(varCharRowType, 0),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(5)),
            rexBuilder.makeLiteral("value1")
    );

    RexNode inRexNode = rexBuilder.makeCall(SqlStdOperatorTable.IN, listRexNodes);
    RexNode[] rexNodes = new RexNode[1];
    rexNodes[0] = inRexNode;
    Method translateFilter = DruidQuery.Translator.class.getDeclaredMethod(
            "translateFilter",
            RexNode.class
    );
    translateFilter.setAccessible(true);
    DruidQuery.JsonInFilter returnValue = (DruidQuery.JsonInFilter) translateFilter
            .invoke(translatorStringKind, rexNodes);
    JsonFactory jsonFactory = new JsonFactory();
    final StringWriter sw = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createGenerator(sw);
    returnValue.write(jsonGenerator);
    jsonGenerator.close();
    Assert.assertEquals(
            "{\"type\":\"in\",\"dimension\":\"dimensionName\",\"values\":[\"1\",\"5\",\"value1\"]}",
            sw.toString()
    );
  }

  @Test
  public void testBetweenFilterStringCase()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
          IOException {

    final List<RexNode> listRexNodes = Lists.newArrayList(
            rexBuilder.makeLiteral(false),
            rexBuilder.makeInputRef(varCharRowType, 0),
            rexBuilder.makeLiteral("lower-bound"),
            rexBuilder.makeLiteral("upper-bound")
    );
    RelDataType relDataType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    RexNode betweenRexNode = rexBuilder
            .makeCall(relDataType, SqlStdOperatorTable.BETWEEN, listRexNodes);
    RexNode[] rexNodes = new RexNode[1];
    rexNodes[0] = betweenRexNode;

    Method translateFilter = DruidQuery.Translator.class.getDeclaredMethod(
            "translateFilter",
            RexNode.class
    );
    translateFilter.setAccessible(true);
    DruidQuery.JsonBound returnValue = (DruidQuery.JsonBound) translateFilter
            .invoke(translatorStringKind, rexNodes);
    JsonFactory jsonFactory = new JsonFactory();
    final StringWriter sw = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createGenerator(sw);
    returnValue.write(jsonGenerator);
    jsonGenerator.close();
    Assert.assertEquals(
            "{\"type\":\"bound\",\"dimension\":\"dimensionName\",\"lower\":\"lower-bound\","
                    + "\"lowerStrict\":false,\"upper\":\"upper-bound\",\"upperStrict\":false,"
                    + "\"alphaNumeric\":false}",
            sw.toString()
    );
  }
}

// End DruidQueryFilterTest.java
