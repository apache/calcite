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

import org.apache.calcite.linq4j.Ord;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents an infinite loop. It can be exited with "break".
 */
public class ForStatement extends Statement {
  public final List<DeclarationStatement> declarations;
  public final @Nullable Expression condition;
  public final @Nullable Expression post;
  public final Statement body;
  /** Cached hash code for the expression. */
  private int hash;

  public ForStatement(List<DeclarationStatement> declarations,
      @Nullable Expression condition, @Nullable Expression post,
      Statement body) {
    super(ExpressionType.For, Void.TYPE);
    this.declarations =
        requireNonNull(declarations, "declarations"); // may be empty
    this.condition = condition;
    this.post = post;
    this.body = requireNonNull(body, "body"); // may be empty block
  }

  @Override public ForStatement accept(Shuttle shuttle) {
    shuttle = shuttle.preVisit(this);
    List<DeclarationStatement> decls1 =
        Expressions.acceptDeclarations(declarations, shuttle);
    final Expression condition1 =
        condition == null ? null : condition.accept(shuttle);
    final Expression post1 = post == null ? null : post.accept(shuttle);
    final Statement body1 = body.accept(shuttle);
    return shuttle.visit(this, decls1, condition1, post1, body1);
  }

  @Override public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override void accept0(ExpressionWriter writer) {
    writer.append("for (");
    for (Ord<DeclarationStatement> declaration : Ord.zip(declarations)) {
      declaration.e.accept2(writer, declaration.i == 0);
    }
    writer.append("; ");
    if (condition != null) {
      writer.append(condition);
    }
    writer.append("; ");
    if (post != null) {
      writer.append(post);
    }
    writer.append(") ").append(Blocks.toBlock(body));
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ForStatement that = (ForStatement) o;
    return body.equals(that.body)
        && Objects.equals(condition, that.condition)
        && declarations.equals(that.declarations)
        && Objects.equals(post, that.post);
  }

  @Override public int hashCode() {
    int result = hash;
    if (result == 0) {
      result =
          Objects.hash(nodeType, type, declarations, condition, post, body);
      if (result == 0) {
        result = 1;
      }
      hash = result;
    }
    return result;
  }
}
