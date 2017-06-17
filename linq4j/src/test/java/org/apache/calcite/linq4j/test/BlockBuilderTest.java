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
package org.apache.calcite.linq4j.test;

import org.apache.calcite.linq4j.tree.BinaryExpression;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.OptimizeShuttle;
import org.apache.calcite.linq4j.tree.Shuttle;

import org.junit.Before;
import org.junit.Test;

import static org.apache.calcite.linq4j.test.BlockBuilderBase.FOUR;
import static org.apache.calcite.linq4j.test.BlockBuilderBase.ONE;
import static org.apache.calcite.linq4j.test.BlockBuilderBase.TWO;

import static org.junit.Assert.assertEquals;

/**
 * Tests BlockBuilder.
 */
public class BlockBuilderTest {
  BlockBuilder b;

  @Before
  public void prepareBuilder() {
    b = new BlockBuilder(true);
  }

  @Test public void testReuseExpressionsFromUpperLevel() {
    Expression x = b.append("x", Expressions.add(ONE, TWO));
    BlockBuilder nested = new BlockBuilder(true, b);
    Expression y = nested.append("y", Expressions.add(ONE, TWO));
    nested.add(Expressions.return_(null, Expressions.add(y, y)));
    b.add(nested.toBlock());
    assertEquals(
        "{\n"
            + "  final int x = 1 + 2;\n"
            + "  {\n"
            + "    return x + x;\n"
            + "  }\n"
            + "}\n",
        b.toBlock().toString());
  }

  @Test public void testTestCustomOptimizer() {
    BlockBuilder b = new BlockBuilder() {
      @Override protected Shuttle createOptimizeShuttle() {
        return new OptimizeShuttle() {
          @Override public Expression visit(BinaryExpression binary,
              Expression expression0, Expression expression1) {
            if (binary.getNodeType() == ExpressionType.Add
                && ONE.equals(expression0) && TWO.equals(expression1)) {
              return FOUR;
            }
            return super.visit(binary, expression0, expression1);
          }
        };
      }
    };
    b.add(Expressions.return_(null, Expressions.add(ONE, TWO)));
    assertEquals("{\n  return 4;\n}\n", b.toBlock().toString());
  }
}

// End BlockBuilderTest.java
