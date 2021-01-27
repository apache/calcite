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
package org.apache.calcite.linq4j.tree;

import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a block that contains a sequence of expressions
 * where variables can be defined.
 *
 * 表示 变量可定义的、包含关系表达式的Block.
 */
public class BlockStatement extends Statement {

  public final List<Statement> statements;

  // 缓存 hash值。
  private int hash;

  BlockStatement(List<Statement> statements, Type type) {
    super(ExpressionType.Block, type);
    assert statements != null : "statements should not be null";
    this.statements = statements;
    assert distinctVariables(true);
  }

  private boolean distinctVariables(boolean fail) {
    Set<String> names = new HashSet<>();
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

  @Override public BlockStatement accept(Shuttle shuttle) {
    shuttle = shuttle.preVisit(this);
    List<Statement> newStatements = Expressions.acceptStatements(statements,
        shuttle);
    return shuttle.visit(this, newStatements);
  }

  @Override
  public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override void accept0(ExpressionWriter writer) {
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

  @Override public Object evaluate(Evaluator evaluator) {
    Object o = null;
    for (Statement statement : statements) {
      o = statement.evaluate(evaluator);
    }
    return o;
  }

  @Override public boolean equals(Object o) {
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
      // tag 即使3个值都为null、也不会为0
      //     Objects.hash(null)结果为0
      result = Objects.hash(nodeType, type, statements);

      // todo 这个1毫无道理：如果后续修改了list或者list里边的对象，hashCode会改变。
      if (result == 0) {
        result = 1;
      }
      hash = result;
    }
    return result;
  }
}
