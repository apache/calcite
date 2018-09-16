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
package org.apache.calcite.adapter.cassandra;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Relational expression that uses Cassandra calling convention.
 */
public interface CassandraRel extends RelNode {
  void implement(Implementor implementor);

  /** Calling convention for relational operations that occur in Cassandra. */
  Convention CONVENTION = new Convention.Impl("CASSANDRA", CassandraRel.class);

  /** Callback for the implementation process that converts a tree of
   * {@link CassandraRel} nodes into a CQL query. */
  class Implementor {
    final Map<String, String> selectFields = new LinkedHashMap<>();
    final List<String> whereClause = new ArrayList<>();
    int offset = 0;
    int fetch = -1;
    final List<String> order = new ArrayList<>();

    RelOptTable table;
    CassandraTable cassandraTable;

    /** Adds newly projected fields and restricted predicates.
     *
     * @param fields New fields to be projected from a query
     * @param predicates New predicates to be applied to the query
     */
    public void add(Map<String, String> fields, List<String> predicates) {
      if (fields != null) {
        selectFields.putAll(fields);
      }
      if (predicates != null) {
        whereClause.addAll(predicates);
      }
    }

    public void addOrder(List<String> newOrder) {
      order.addAll(newOrder);
    }

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((CassandraRel) input).implement(this);
    }
  }
}

// End CassandraRel.java
