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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Abstract implementation of {@link Node}.
 */
public abstract class AbstractNode implements Node {
  public final ExpressionType nodeType;
  public final Type type;

  AbstractNode(ExpressionType nodeType, Type type) {
    this.type = requireNonNull(type, "type");
    this.nodeType = requireNonNull(nodeType, "nodeType");
  }

  /**
   * Gets the node type of this Expression.
   */
  public ExpressionType getNodeType() {
    return nodeType;
  }

  /**
   * Gets the static type of the expression that this Expression
   * represents.
   */
  public Type getType() {
    return type;
  }

  @Override public String toString() {
    ExpressionWriter writer = new ExpressionWriter(true);
    accept(writer, 0, 0);
    return writer.toString();
  }

  @Override public void accept(ExpressionWriter writer) {
    accept(writer, 0, 0);
  }

  void accept0(ExpressionWriter writer) {
    accept(writer, 0, 0);
  }

  void accept(ExpressionWriter writer, int lprec, int rprec) {
    throw new RuntimeException(
        "un-parse not supported: " + getClass() + ":" + nodeType);
  }

  @Override public Node accept(Shuttle shuttle) {
    throw new RuntimeException(
        "visit not supported: " + getClass() + ":" + nodeType);
  }

  public @Nullable Object evaluate(Evaluator evaluator) {
    throw new RuntimeException(
        "evaluation not supported: " + getClass() + ":" + nodeType);
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AbstractNode that = (AbstractNode) o;
    return nodeType == that.nodeType
        && type.equals(that.type);
  }

  @Override public int hashCode() {
    return Objects.hash(nodeType, type);
  }
}
