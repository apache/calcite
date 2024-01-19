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
import org.apache.calcite.util.ImmutableIntList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Relational expression that uses the Arrow calling convention.
 */
public interface ArrowRel extends RelNode {
  void implement(Implementor implementor);

  /** Calling convention for relational operations that occur in Arrow. */
  Convention CONVENTION = new Convention.Impl("ARROW", ArrowRel.class);

  /** Callback for the implementation process that converts a tree of
   * {@link ArrowRel} nodes into a SQL query. */
  class Implementor {
    @Nullable List<Integer> selectFields;
    final List<String> whereClause = new ArrayList<>();
    @Nullable RelOptTable table;
    @Nullable ArrowTable arrowTable;

    /** Adds new predicates.
     *
     * @param predicates Predicates
     */
    void addFilters(List<String> predicates) {
      whereClause.addAll(predicates);
    }

    /** Adds newly projected fields.
     *
     * @param fields New fields to be projected from a query
     */
    void addProjectFields(List<Integer> fields) {
      selectFields = ImmutableIntList.copyOf(fields);
    }

    public void visitInput(int ordinal, RelNode input) {
      checkArgument(ordinal == 0);
      ((ArrowRel) input).implement(this);
    }
  }
}
