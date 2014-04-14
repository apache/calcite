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
import net.hydromatic.linq4j.expressions.Statement;

/**
 * Base methods and constant for simplified Expression testing
 */
public abstract class BlockBuilderBase {
  public static final Expression NULL = Expressions.constant(null);
  public static final Expression NULL_INTEGER = Expressions.constant(null,
      Integer.class);
  public static final Expression ONE = Expressions.constant(1);
  public static final Expression TWO = Expressions.constant(2);
  public static final Expression THREE = Expressions.constant(3);
  public static final Expression FOUR = Expressions.constant(4);
  public static final Expression TRUE = Expressions.constant(true);
  public static final Expression FALSE = Expressions.constant(false);

  public static String optimize(Expression expr) {
    return optimize(Expressions.return_(null, expr));
  }

  public static String optimize(Statement statement) {
    BlockBuilder b = new BlockBuilder(true);
    b.add(statement);
    return b.toBlock().toString();
  }
}
