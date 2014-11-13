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
package net.hydromatic.linq4j.expressions;

/**
 * <p>Helper methods concerning {@link BlockStatement}s.</p>
 *
 * @see BlockBuilder
 */
public final class Blocks {
  private Blocks() {
    throw new AssertionError("no blocks for you!");
  }

  private static BlockStatement toFunctionBlock(Node body, boolean function) {
    if (body instanceof BlockStatement) {
      return (BlockStatement) body;
    }
    Statement statement;
    if (body instanceof Statement) {
      statement = (Statement) body;
    } else if (body instanceof Expression) {
      if (((Expression) body).getType() == Void.TYPE && function) {
        statement = Expressions.statement((Expression) body);
      } else {
        statement = Expressions.return_(null, (Expression) body);
      }
    } else {
      throw new AssertionError(
          "block cannot contain node that is neither statement nor "
          + "expression: "
          + body);
    }
    return Expressions.block(statement);
  }

  public static BlockStatement toFunctionBlock(Node body) {
    return toFunctionBlock(body, true);
  }

  public static BlockStatement toBlock(Node body) {
    return toFunctionBlock(body, false);
  }

  /**
   * Prepends a statement to a block.
   */
  public static BlockStatement create(Statement statement,
      BlockStatement block) {
    return Expressions.block(Expressions.list(statement).appendAll(
        block.statements));
  }

  /**
   * Converts a simple "{ return expr; }" block into "expr"; otherwise
   * throws.
   */
  public static Expression simple(BlockStatement block) {
    if (block.statements.size() == 1) {
      Statement statement = block.statements.get(0);
      if (statement instanceof GotoStatement) {
        return ((GotoStatement) statement).expression;
      }
    }
    throw new AssertionError("not a simple block: " + block);
  }
}

// End Blocks.java
