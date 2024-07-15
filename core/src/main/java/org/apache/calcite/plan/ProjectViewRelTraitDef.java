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
 * ProjectViewRelTraitDef wraps the ProjectViewRelTrait class
 * and provides the default implementation for the trait.
 */

public class ProjectViewRelTraitDef extends RelTraitDef<ProjectViewRelTrait> {

  public static ProjectViewRelTraitDef instance = new ProjectViewRelTraitDef();

  @Override public Class<ProjectViewRelTrait> getTraitClass() {
    return ProjectViewRelTrait.class;
  }

  @Override public String getSimpleName() {
    return ProjectViewRelTrait.class.getSimpleName();
  }

  @Override public RelNode convert(
      RelOptPlanner planner, RelNode rel, ProjectViewRelTrait toTrait,
      boolean allowInfiniteCostConverters) {
    throw new UnsupportedOperationException(
        "Method implementation not supported for ProjectViewRelTrait");
  }

  @Override public boolean canConvert(
      RelOptPlanner relOptPlanner,
      ProjectViewRelTrait projectViewRelTrait,
      ProjectViewRelTrait t1) {
    return false;
  }

  @Override public ProjectViewRelTrait getDefault() {
    throw new UnsupportedOperationException(
        "Default implementation not supported for ProjectViewRelTrait");
  }
}
