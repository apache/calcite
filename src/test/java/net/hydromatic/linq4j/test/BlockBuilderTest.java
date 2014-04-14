/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.linq4j.test;

import net.hydromatic.linq4j.expressions.BlockBuilder;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests BlockBuilder
 */
public class BlockBuilderTest extends BlockBuilderBase {
  BlockBuilder b;

  @Before
  public void prepareBuilder() {
    b = new BlockBuilder(true);
  }

  @Test
  public void reuseExpressionsFromUpperLevel() {
    Expression x = b.append("x", Expressions.add(ONE, TWO));
    BlockBuilder nested = new BlockBuilder(true, b);
    Expression y = nested.append("y", Expressions.add(ONE, TWO));
    nested.add(Expressions.return_(null, Expressions.add(y, y)));
    b.add(nested.toBlock());
    assertEquals("{\n"
                 + "  final int x = 1 + 2;\n"
                 + "  {\n"
                 + "    return x + x;\n"
                 + "  }\n"
                 + "}\n", b.toBlock().toString());
  }

}
