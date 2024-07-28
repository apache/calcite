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

/**
 * SubQueryAliasTrait is used to maintain alias of subquery.
 */
public class SubQueryAliasTrait implements RelTrait {

  private final String subQueryAlias;

  public SubQueryAliasTrait(String subQueryAlias) {
    this.subQueryAlias = subQueryAlias;
  }

  public String getSubQueryAlias() {
    return this.subQueryAlias;
  }

  @Override public RelTraitDef<SubQueryAliasTrait> getTraitDef() {
    return SubQueryAliasTraitDef.instance;
  }

  @Override public boolean satisfies(RelTrait trait) {
    throw new UnsupportedOperationException("Method not implemented for SubQueryAliasTrait");
  }

  @Override public void register(RelOptPlanner planner) {
    throw new UnsupportedOperationException("Registration not supported for SubQueryAliasTrait");
  }
}
