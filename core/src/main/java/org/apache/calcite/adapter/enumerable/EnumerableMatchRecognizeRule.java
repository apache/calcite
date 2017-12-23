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
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.base.Predicates;

/**
 * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalMatch} to an
 * {@link EnumerableProject}.
 */
public class EnumerableMatchRecognizeRule extends ConverterRule {

  public EnumerableMatchRecognizeRule() {
    this(RelFactories.LOGICAL_BUILDER);
  }

  /**
   * Create an EnumerableMatchRecognizeRule
   * @param relBuilderFactory Builder for relation expressions
   */
  public EnumerableMatchRecognizeRule(RelBuilderFactory relBuilderFactory) {
    super(LogicalMatch.class, Predicates.alwaysTrue(),
      Convention.NONE, EnumerableConvention.INSTANCE, relBuilderFactory,
      "EnumerableMatchRecognizeRule");
  }

  @Override public RelNode convert(RelNode rel) {
    final RelTraitSet traitSet = rel.getTraitSet().replace(EnumerableConvention.INSTANCE);
    LogicalMatch mr = (LogicalMatch) rel;
    return new EnumerableMatchRecognize(rel.getCluster(), traitSet,
      mr.getInput(), mr.getRowType(), mr.getPattern(), mr.isStrictStart(), mr.isStrictEnd(),
      mr.getPatternDefinitions(), mr.getMeasures(), mr.getAfter(), mr.getSubsets(), mr.isAllRows(),
      mr.getPartitionKeys(), mr.getOrderKeys(), mr.getInterval());
  }
}

// End EnumerableMatchRecognizeRule.java
