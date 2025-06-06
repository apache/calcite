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

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * SubQueryAliasTraitDef is used to maintain alias of subquery.
 */
public class SubQueryAliasTraitDef extends RelTraitDef<SubQueryAliasTrait> {

  public static SubQueryAliasTraitDef instance = new SubQueryAliasTraitDef();

  @Override public Class<SubQueryAliasTrait> getTraitClass() {
    return SubQueryAliasTrait.class;
  }

  @Override public String getSimpleName() {
    return SubQueryAliasTrait.class.getSimpleName();
  }

  @Override public @Nullable RelNode convert(RelOptPlanner planner, RelNode rel,
      SubQueryAliasTrait toTrait, boolean allowInfiniteCostConverters) {
    throw new UnsupportedOperationException("Method implementation not supported for "
        + "SubQueryAliasTrait");
  }

  @Override public boolean canConvert(RelOptPlanner planner, SubQueryAliasTrait fromTrait,
      SubQueryAliasTrait toTrait) {
    return false;
  }

  @Override public SubQueryAliasTrait getDefault() {
    throw new UnsupportedOperationException("Default implementation not supported for "
        + "SubQueryAliasTrait");
  }
}
