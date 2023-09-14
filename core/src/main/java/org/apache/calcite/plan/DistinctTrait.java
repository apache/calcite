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

/***
 * Distinct type of queries are handled via "Group By" by Calcite.
 * In order to generate Distinct keyword in RelToSql phase DistinctTrait is used.
 * This model keeps the info for Aggregate Rel
 * i.e, if a given aggregate rel is for "group by" or for "distinct"
 */
public class DistinctTrait implements RelTrait {
  private final boolean distinctQuery;
  private boolean evaluatedStruct;

  public DistinctTrait(boolean distinctQuery) {
    this.distinctQuery = distinctQuery;
    this.evaluatedStruct = false;
  }

  public final boolean getTableAlias() {
    return distinctQuery;
  }

  public boolean isDistinct() {
    return distinctQuery;
  }

  public boolean isEvaluated() {
    return evaluatedStruct;
  }

  @Override public RelTraitDef<DistinctTrait> getTraitDef() {
    return DistinctTraitDef.instance;
  }

  @Override public boolean satisfies(RelTrait trait) {
    throw new UnsupportedOperationException("Method not implemented for TableAliasTrait");
  }

  @Override public void register(RelOptPlanner planner) {
    throw new UnsupportedOperationException("Registration not supported for TableAliasTrait");
  }

  public void setEvaluatedStruct(boolean evaluated) {
    this.evaluatedStruct = evaluated;
  }
}
