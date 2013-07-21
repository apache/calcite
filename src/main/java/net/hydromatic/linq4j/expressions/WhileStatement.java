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
 * Represents a "while" statement.
 */
public class WhileStatement extends Statement {
  public final Expression condition;
  public final Statement body;

  public WhileStatement(Expression condition, Statement body) {
    super(ExpressionType.While, Void.TYPE);
    this.condition = condition;
    this.body = body;
  }

  @Override
  public Statement accept(Visitor visitor) {
    final Expression condition1 = condition.accept(visitor);
    final Statement body1 = body.accept(visitor);
    return visitor.visit(this, condition1, body1);
  }

  @Override
  void accept0(ExpressionWriter writer) {
    writer.append("while (").append(condition).append(") ").append(
        Blocks.toBlock(body));
  }
}

// End WhileStatement.java
