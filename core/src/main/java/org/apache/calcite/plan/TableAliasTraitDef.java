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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;

/**
 * TableAliasTraitDef is used to identify if a given rel has a TableAliasTrait scope.
 */
public class TableAliasTraitDef extends RelTraitDef<TableAliasTrait> {


  public static TableAliasTraitDef instance = new TableAliasTraitDef();

  @Override public Class<TableAliasTrait> getTraitClass() {
    return TableAliasTrait.class;
  }

  @Override public String getSimpleName() {
    return TableAliasTrait.class.getSimpleName();
  }

  @Override public RelNode convert(RelOptPlanner planner, RelNode rel, TableAliasTrait toTrait,
      boolean allowInfiniteCostConverters) {
    throw new UnsupportedOperationException("Method implementation not supported for "
        +
        "TableAliasTrait");
  }

  @Override public boolean canConvert(RelOptPlanner planner, TableAliasTrait fromTrait,
      TableAliasTrait toTrait) {
    return false;
  }

  @Override public TableAliasTrait getDefault() {
    throw new UnsupportedOperationException("Default implementation not supported for "
        +
        "TableAliasTrait");
  }
}
