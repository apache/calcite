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

import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents a block that contains a sequence of expressions where variables
 * can be defined.
 */
public class BlockStatement extends Statement {
  public final List<Statement> statements;
  /**
   * Cache the hash code for the expression
   */
  private int hash;

  BlockStatement(List<Statement> statements, Type type) {
    super(ExpressionType.Block, type);
    assert statements != null : "statements should not be null";
    this.statements = statements;
    assert distinctVariables(true);
  }

  private boolean distinctVariables(boolean fail) {
    Set<String> names = new HashSet<String>();
    for (Statement statement : statements) {
      if (statement instanceof DeclarationStatement) {
        String name = ((DeclarationStatement) statement).parameter.name;
        if (!names.add(name)) {
          assert !fail : "duplicate variable " + name;
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public BlockStatement accept(Visitor visitor) {
    visitor = visitor.preVisit(this);
    List<Statement> newStatements = Expressions.acceptStatements(statements,
        visitor);
    return visitor.visit(this, newStatements);
  }

  @Override
  void accept0(ExpressionWriter writer) {
    if (statements.isEmpty()) {
      writer.append("{}");
      return;
    }
    writer.begin("{\n");
    for (Statement node : statements) {
      node.accept(writer, 0, 0);
    }
    writer.end("}\n");
  }

  @Override
  public Object evaluate(Evaluator evaluator) {
    Object o = null;
    for (Statement statement : statements) {
      o = statement.evaluate(evaluator);
    }
    return o;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    BlockStatement that = (BlockStatement) o;

    if (!statements.equals(that.statements)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = hash;
    if (result == 0) {
      result = super.hashCode();
      result = 31 * result + statements.hashCode();
      if (result == 0) {
        result = 1;
      }
      hash = result;
    }
    return result;
  }
}

// End BlockStatement.java
