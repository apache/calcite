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

import java.util.Objects;

/**
 * Represents a label, which can be put in any {@link Expression} context. If it
 * is jumped to, it will get the value provided by the corresponding
 * {@link GotoStatement}. Otherwise, it receives the value in
 * {@link #defaultValue}. If the Type equals {@link Void}, no value should be
 * provided.
 */
public class LabelStatement extends Statement {
  public final Expression defaultValue;

  public LabelStatement(Expression defaultValue, ExpressionType nodeType) {
    super(nodeType, Void.TYPE);
    this.defaultValue = defaultValue;
  }

  @Override public LabelStatement accept(Shuttle shuttle) {
    return shuttle.visit(this);
  }

  public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
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

    LabelStatement that = (LabelStatement) o;

    if (defaultValue != null ? !defaultValue.equals(that.defaultValue) : that
        .defaultValue != null) {
      return false;
    }

    return true;
  }

  @Override public int hashCode() {
    return Objects.hash(nodeType, type, defaultValue);
  }
}

// End LabelStatement.java
