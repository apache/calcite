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
package org.apache.calcite.adapter.geode.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Relational expression that uses Geode calling convention.
 */
public interface GeodeRel extends RelNode {

  /**
   * Calling convention for relational operations that occur in Geode.
   */
  Convention CONVENTION = new Convention.Impl("GEODE", GeodeRel.class);

  /**
   * Callback for the implementation process that collects the context from the
   * {@link GeodeRel} required to convert the relational tree into physical such.
   *
   * @param geodeImplementContext Context class that collects the feedback from the
   *                              call back method calls
   */
  void implement(GeodeImplementContext geodeImplementContext);

  /**
   * Shared context used by the {@link GeodeRel} relations.
   *
   * <p>Callback context class for the implementation process that converts a
   * tree of {@code GeodeRel} nodes into an OQL query.
   */
  class GeodeImplementContext {
    final Map<String, String> selectFields = new LinkedHashMap<>();

    final List<String> whereClause = new ArrayList<>();

    final List<String> orderByFields = new ArrayList<>();

    final List<String> groupByFields = new ArrayList<>();

    final Map<String, String> oqlAggregateFunctions = new LinkedHashMap<>();

    Long limitValue;

    RelOptTable table;

    GeodeTable geodeTable;

    /**
     * Adds new projected fields.
     *
     * @param fields New fields to be projected from a query
     */
    public void addSelectFields(Map<String, String> fields) {
      if (fields != null) {
        selectFields.putAll(fields);
      }
    }

    /**
     * Adds new  restricted predicates.
     *
     * @param predicates New predicates to be applied to the query
     */
    public void addPredicates(List<String> predicates) {
      if (predicates != null) {
        whereClause.addAll(predicates);
      }
    }

    public void addOrderByFields(List<String> orderByFieldLists) {
      orderByFields.addAll(orderByFieldLists);
    }

    public void setLimit(long limit) {
      limitValue = limit;
    }

    public void addGroupBy(List<String> groupByFields) {
      this.groupByFields.addAll(groupByFields);
    }

    public void addAggregateFunctions(Map<String, String> oqlAggregateFunctions) {
      this.oqlAggregateFunctions.putAll(oqlAggregateFunctions);
    }

    void visitChild(RelNode input) {
      ((GeodeRel) input).implement(this);
    }

    @Override public String toString() {
      return "GeodeImplementContext{"
          + "selectFields=" + selectFields
          + ", whereClause=" + whereClause
          + ", orderByFields=" + orderByFields
          + ", limitValue='" + limitValue + '\''
          + ", groupByFields=" + groupByFields
          + ", table=" + table
          + ", geodeTable=" + geodeTable
          + '}';
    }
  }
}

// End GeodeRel.java
