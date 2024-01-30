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
package org.apache.calcite.adapter.innodb;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Relational expression that uses InnoDB calling convention.
 */
public interface InnodbRel extends RelNode {
  void implement(Implementor implementor);

  /** Calling convention for relational operations that occur in InnoDB. */
  Convention CONVENTION = new Convention.Impl("INNODB", InnodbRel.class);

  /** Callback for the implementation process that converts a tree of
   * {@link InnodbRel} nodes into an InnoDB direct call query. */
  class Implementor {
    final Map<String, String> selectFields = new LinkedHashMap<>();
    IndexCondition indexCondition = IndexCondition.EMPTY_CONDITION;
    boolean ascOrder = true;

    RelOptTable table;
    InnodbTable innodbTable;

    public void addSelectFields(Map<String, String> fields) {
      if (fields != null) {
        selectFields.putAll(fields);
      }
    }

    public void setIndexCondition(IndexCondition indexCondition) {
      this.indexCondition = indexCondition;
    }

    public void setAscOrder(boolean ascOrder) {
      this.ascOrder = ascOrder;
    }

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((InnodbRel) input).implement(this);
    }
  }
}
