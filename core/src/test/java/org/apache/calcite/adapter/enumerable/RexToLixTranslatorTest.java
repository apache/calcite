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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;

/**
 * Test for {@link org.apache.calcite.adapter.enumerable.RexToLixTranslator}
 */
public class RexToLixTranslatorTest {

  @Test public void testRawTranslateRexNode() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder b = new RexBuilder(typeFactory);
    final RexNode rexNode = b.makeCall(
        SqlStdOperatorTable.ABS,
        b.makeBigintLiteral(new BigDecimal(300)));
    BlockBuilder builder = new BlockBuilder();
    RexToLixTranslator translator =
        RexToLixTranslator.createRexToLixTranslator(
            new JavaTypeFactoryImpl()).setBlock(builder);
    Expression expr = translator.rawTranslate(rexNode);
    Expression lambdaExpr = Expressions.lambda(expr);
    FunctionExpression.Invokable invokable = ((FunctionExpression) lambdaExpr).compile();
    Object result = invokable.dynamicInvoke();
    assertEquals(result, 300L);
  }
}

// End RexToLixTranslatorTest.java
