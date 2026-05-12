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
 * CTEDefinationTrait is used to identify if a given rel has a CTE Definition.
 */
public class CTEDefinationTrait implements RelTrait {

  private final boolean isCTEDefination;
  private final String cteName;
  /** Whether the CTE was declared with an explicit column list (e.g., {@code WITH cte(col1, col2) AS (...)}). */
  private final boolean hasExplicitColumns;

  public CTEDefinationTrait(boolean isCTEDefination, String cteName, boolean hasExplicitColumns) {
    this.isCTEDefination = isCTEDefination;
    this.cteName = cteName;
    this.hasExplicitColumns = hasExplicitColumns;
  }

  public boolean isCTEDefination() {
    return this.isCTEDefination;
  }

  public boolean hasExplicitColumns() {
    return this.hasExplicitColumns;
  }

  public String getCteName() {
    return this.cteName;
  }

  @Override public RelTraitDef<CTEDefinationTrait> getTraitDef() {
    return CTEDefinationTraitDef.instance;
  }

  @Override public boolean satisfies(RelTrait trait) {
    throw new UnsupportedOperationException("Method not implemented for CTEDefinationTrait");
  }

  @Override public void register(RelOptPlanner planner) {
    throw new UnsupportedOperationException("Registration not supported for CTEDefinationTrait");
  }
}
