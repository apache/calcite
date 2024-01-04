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
 * CTEDefinationTrait is used to identify if a given rel has a CTEDefinationTraitDef.
 */
public class CTEDefinationTraitDef extends RelTraitDef<CTEDefinationTrait> {

  public static CTEDefinationTraitDef instance = new CTEDefinationTraitDef();

  @Override public Class<CTEDefinationTrait> getTraitClass() {
    return CTEDefinationTrait.class;
  }

  @Override public String getSimpleName() {
    return CTEDefinationTrait.class.getSimpleName();
  }

  @Override public RelNode convert(RelOptPlanner planner, RelNode rel,
      CTEDefinationTrait toTrait, boolean allowInfiniteCostConverters) {
    throw new UnsupportedOperationException("Method implementation"
        + " not supported for CTEDefinationTrait");
  }

  @Override public boolean canConvert(RelOptPlanner planner, CTEDefinationTrait fromTrait,
      CTEDefinationTrait toTrait) {
    return false;
  }

  @Override public CTEDefinationTrait getDefault() {
    throw new UnsupportedOperationException("Default implementation not "
        + "supported for CTEDefinationTrait");
  }
}
