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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/** Planner rule that converts a
 * {@link org.apache.calcite.rel.logical.LogicalTableFunctionScan}
 * relational expression
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableTableFunctionScanRule extends ConverterRule {
  @Deprecated // to be removed before 2.0
  public EnumerableTableFunctionScanRule() {
    this(RelFactories.LOGICAL_BUILDER);
  }

  /**
   * Creates an EnumerableTableFunctionScanRule.
   *
   * @param relBuilderFactory Builder for relational expressions
   */
  public EnumerableTableFunctionScanRule(RelBuilderFactory relBuilderFactory) {
    super(LogicalTableFunctionScan.class, (Predicate<RelNode>) r -> true,
        Convention.NONE, EnumerableConvention.INSTANCE, relBuilderFactory,
        "EnumerableTableFunctionScanRule");
  }

  @Override public RelNode convert(RelNode rel) {
    final RelTraitSet traitSet =
        rel.getTraitSet().replace(EnumerableConvention.INSTANCE);
    LogicalTableFunctionScan tbl = (LogicalTableFunctionScan) rel;
    return new EnumerableTableFunctionScan(rel.getCluster(), traitSet,
        tbl.getInputs(), tbl.getElementType(), tbl.getRowType(),
        tbl.getCall(), tbl.getColumnMappings());
  }
}

// End EnumerableTableFunctionScanRule.java
