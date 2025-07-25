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
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

/**
 * Relational expression that uses Salesforce calling convention.
 */
public interface SalesforceRel extends RelNode {

  Convention CONVENTION = new Convention.Impl("SALESFORCE", SalesforceRel.class);

  /**
   * Callback for the implementation process.
   */
  void implement(Implementor implementor);

  /**
   * Shared context for implementing a Salesforce relational expression.
   */
  class Implementor {
    SalesforceTable salesforceTable;
    RelOptTable table;
    String sObjectType;

    // Query components
    String selectClause;
    String fromClause;
    String whereClause;
    String orderByClause;
    Integer limitValue;
    Integer offsetValue;

    void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((SalesforceRel) input).implement(this);
    }
  }
}
