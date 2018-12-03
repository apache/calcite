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
package org.apache.calcite.adapter.pig;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Relational expression that uses the Pig calling convention.
 */
public interface PigRel extends RelNode {

  /**
   * Converts this node to a Pig Latin statement.
   */
  void implement(Implementor implementor);

  // String getPigRelationAlias();
  //
  // String getFieldName(int index);

  /** Calling convention for relational operations that occur in Pig. */
  Convention CONVENTION = new Convention.Impl("PIG", PigRel.class);

  /**
   * Callback for the implementation process that converts a tree of
   * {@link PigRel} nodes into complete Pig Latin script.
   */
  class Implementor {

    /**
     * An ordered list of Pig Latin statements.
     *
     * <p>See
     * <a href="https://pig.apache.org/docs/r0.13.0/start.html#pl-statements">
     * Pig Latin reference</a>.
     */
    private final List<String> statements = new ArrayList<>();

    public String getTableName(RelNode input) {
      final List<String> qualifiedName = input.getTable().getQualifiedName();
      return qualifiedName.get(qualifiedName.size() - 1);
    }

    public String getPigRelationAlias(RelNode input) {
      return getTableName(input);
    }

    public String getFieldName(RelNode input, int index) {
      return input.getRowType().getFieldList().get(index).getName();
    }

    public void addStatement(String statement) {
      statements.add(statement);
    }

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((PigRel) input).implement(this);
    }

    public List<String> getStatements() {
      return statements;
    }

    public String getScript() {
      return String.join("\n", statements);
    }
  }
}

// End PigRel.java
