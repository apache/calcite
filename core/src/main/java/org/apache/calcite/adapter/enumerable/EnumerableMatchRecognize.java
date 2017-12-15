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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;

/** Implementation of {@link org.apache.calcite.rel.core.Match} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableMatchRecognize extends LogicalMatch implements EnumerableRel {

  public EnumerableMatchRecognize(RelOptCluster cluster,
      RelTraitSet traits, RelNode input, RelDataType rowType, RexNode pattern,
      boolean strictStart, boolean strictEnd,
      Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures,
      RexNode after, Map<String, ? extends SortedSet<String>> subsets,
      boolean allRows, List<RexNode> partitionKeys, RelCollation orderKeys,
      RexNode interval) {
    super(cluster, traits, input, rowType, pattern, strictStart, strictEnd, patternDefinitions,
      measures, after, subsets, allRows, partitionKeys, orderKeys, interval);
  }

  @Override public Match copy(RelTraitSet traits, RelNode input, RelDataType rowType,
      RexNode pattern, boolean strictStart, boolean strictEnd,
      Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures,
      RexNode after, Map<String, ? extends SortedSet<String>> subsets,
      boolean allRows, List<RexNode> partitionKeys, RelCollation orderKeys,
      RexNode interval) {
    return new EnumerableMatchRecognize(getCluster(), traits,
      input,
      rowType,
      pattern, strictStart, strictEnd, patternDefinitions, measures,
      after, subsets, allRows, partitionKeys, orderKeys,
      interval);
  }


  public EnumerableRel.Result implement(EnumerableRelImplementor implementor,
      EnumerableRel.Prefer pref) {
    throw new RuntimeException("Test");
  }

}

// End EnumerableMatchRecognize.java
