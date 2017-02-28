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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.MatchRecognize;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.Map;

/**
 * sub class of {@link org.apache.calcite.rel.core.MatchRecognize}
 * not targeted at any particular engine or calling convention
 */
public class LogicalMatchRecognize extends MatchRecognize {

  /**
   * Creates a MatchRecognize
   * @param cluster cluster
   * @param traits Trait set
   * @param input Input to MatchRecognize
   * @param pattern Regular Expression defining pattern variables
   * @param isStrictStarts Whether it is a strict start pattern
   * @param isStrictEnds Whether it is a strict end pattern
   * @param defns pattern defiintions
   * @param rowType rowtype
   */
  public LogicalMatchRecognize(
    RelOptCluster cluster,
    RelTraitSet traits,
    RelNode input,
    RexNode pattern,
    boolean isStrictStarts,
    boolean isStrictEnds,
    Map<String, RexNode> defns,
    RelDataType rowType) {
    super(cluster, traits, input, pattern, isStrictStarts, isStrictEnds, defns, rowType);
  }

  /**
   * Create a logicalMatchRecoginize
   */
  public static LogicalMatchRecognize create(
    RelNode input,
    RexNode pattern,
    boolean isStrictStarts,
    boolean isStrictEnds,
    Map<String, RexNode> defns,
    RelDataType rowType) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalMatchRecognize(cluster, traitSet, input, pattern,
      isStrictStarts, isStrictEnds, defns, rowType);
  }

  //~ Methods ------------------------------------------------------

  @Override public MatchRecognize copy(
    RelNode input,
    RexNode pattern,
    boolean isStrictStarts,
    boolean isStrictEnds,
    Map<String, RexNode> defns,
    RelDataType rowType) {
    return new LogicalMatchRecognize(getCluster(), getCluster().traitSetOf(Convention.NONE),
      input, pattern, isStrictStarts, isStrictEnds, defns, rowType);
  }
}

// End LogicalMatchRecognize.java
