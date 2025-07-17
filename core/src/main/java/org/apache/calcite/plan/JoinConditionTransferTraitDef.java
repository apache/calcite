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
 * {@code JoinConditionTransferTraitDef} defines a custom trait for identifying
 * whether join conditions have been moved into filter conditions in a query plan.
 *
 *
 * <p>This trait is useful for optimization or transformation phases where distinguishing
 * between conditions in the join and those in the filter can affect how rules are applied.
 * </p>
 *
 * <p>This class extends {@link RelTraitDef} and provides metadata for the
 * {@link JoinConditionTransferTrait} type, but does not support conversion
 * or a default trait value.</p>
 *
 * <p>All method implementations related to trait conversion and defaults
 * explicitly throw {@link UnsupportedOperationException} since this trait is
 * informational only and not intended for conversion or default inference.</p>
 */
public class JoinConditionTransferTraitDef extends RelTraitDef<JoinConditionTransferTrait> {

  public static JoinConditionTransferTraitDef instance = new JoinConditionTransferTraitDef();

  @Override public Class<JoinConditionTransferTrait> getTraitClass() {
    return JoinConditionTransferTrait.class;
  }

  @Override public String getSimpleName() {
    return JoinConditionTransferTrait.class.getSimpleName();
  }

  @Override public RelNode convert(RelOptPlanner planner, RelNode rel,
      JoinConditionTransferTrait toTrait, boolean allowInfiniteCostConverters) {
    throw new UnsupportedOperationException("Method implementation"
        + " not supported for JoinConditionTransferTrait");

  }

  @Override public boolean canConvert(RelOptPlanner planner, JoinConditionTransferTrait fromTrait,
      JoinConditionTransferTrait toTrait) {
    return false;
  }

  @Override public JoinConditionTransferTrait getDefault() {
/*    throw new UnsupportedOperationException("Default implementation not "
        + "supported for JoinConditionTransferTrait");*/
    return JoinConditionTransferTrait.EMPTY;
  }

  @Override public boolean multiple() {
    return true;
  }
}
