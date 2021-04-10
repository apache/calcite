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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Relational expression that uses Arrow calling convention.
 */
public interface ArrowRel extends RelNode {
  void implement(Implementor implementor);

  /** Calling convention for relational operations that occur in Arrow. */
  Convention CONVENTION = new Convention.Impl("ARROW", ArrowRel.class);

  /** Callback for the implementation process that converts a tree of
   * {@link ArrowRel} nodes into a SQL query. */
  class Implementor {
    int @Nullable[] selectFields;
    final List<String> whereClause = new ArrayList<>();
    @Nullable RelOptTable table;
    @Nullable ArrowTable arrowTable;

    /** Adds newly projected fields and restricted predicates.
     *
     * @param fields New fields to be projected from a query
     * @param predicates Predicates
     */
    void add(int @Nullable[] fields, @Nullable List<String> predicates) {
      if (fields != null) {
        selectFields = fields.clone();
      }
      if (predicates != null) {
        whereClause.addAll(predicates);
      }
    }

    public void visitInput(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((ArrowRel) input).implement(this);
    }
  }
}
