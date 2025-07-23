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
 * {@code JoinConditionTransferTrait} is a custom {@link RelTrait} used to indicate
 * whether join conditions in a relational expression have been pushed down
 * and moved into a filter clause.
 *
 * <p>The {@code joinConditionMovedToFilter} flag is the key property, which determines
 * if the join condition has been relocated to a filter.</p>
 *
 * <p>Methods like {@code satisfies} and {@code register} are intentionally unsupported,
 * as this trait is not meant to participate in trait conversion or planner registration.</p>
 */
public class JoinConditionTransferTrait implements RelTrait {

  private final boolean joinConditionMovedToFilter;

  public static final JoinConditionTransferTrait EMPTY = new JoinConditionTransferTrait(false);

  public JoinConditionTransferTrait(boolean joinConditionMovedToFilter) {
    this.joinConditionMovedToFilter = joinConditionMovedToFilter;
  }

  public boolean isJoinConditionMovedToFilter() {
    return joinConditionMovedToFilter;
  }

  @Override public RelTraitDef<JoinConditionTransferTrait> getTraitDef() {
    return JoinConditionTransferTraitDef.INSTANCE;
  }

  @Override public boolean satisfies(RelTrait trait) {
    return this == trait;
  }

  @Override public void register(RelOptPlanner planner) {
    throw new UnsupportedOperationException("Registration not supported for "
        + "JoinConditionTransferTrait");
  }
}
