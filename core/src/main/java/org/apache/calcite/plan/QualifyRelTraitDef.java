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
 * QualifyRelTraitDef wraps the QualifyTrait class
 * and provides the default implementation for the trait.
 */
public class QualifyRelTraitDef extends RelTraitDef<QualifyRelTrait> {
  public static QualifyRelTraitDef instance = new QualifyRelTraitDef();

  public Class<QualifyRelTrait> getTraitClass() {
    return QualifyRelTrait.class;
  }

  public String getSimpleName() {
    return QualifyRelTrait.class.getSimpleName();
  }

  @Override public @Nullable RelNode convert(
      RelOptPlanner planner, RelNode rel, QualifyRelTrait toTrait, boolean allowInfiniteCostConverters) {
    return null;
  }

  @Override public boolean canConvert(RelOptPlanner planner, QualifyRelTrait fromTrait, QualifyRelTrait toTrait) {
    return false;
  }

  public QualifyRelTrait getDefault() {
    throw new UnsupportedOperationException("Default implementation not supported for PivotRelTrait");
  }
}
