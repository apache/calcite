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
package org.eigenbase.relopt;

import org.eigenbase.rex.RexNode;

import com.google.common.collect.ImmutableList;

/**
 * Predicates that are known to hold in the output of a particular relational
 * expression.
 */
public class RelOptPredicateList {
  private static final ImmutableList<RexNode> EMPTY_LIST = ImmutableList.of();
  public static final RelOptPredicateList EMPTY =
      new RelOptPredicateList(EMPTY_LIST, EMPTY_LIST, EMPTY_LIST);

  public final ImmutableList<RexNode> pulledUpPredicates;
  public final ImmutableList<RexNode> leftInferredPredicates;
  public final ImmutableList<RexNode> rightInferredPredicates;

  private RelOptPredicateList(Iterable<RexNode> pulledUpPredicates,
      Iterable<RexNode> leftInferredPredicates,
      Iterable<RexNode> rightInferredPredicates) {
    this.pulledUpPredicates = ImmutableList.copyOf(pulledUpPredicates);
    this.leftInferredPredicates = ImmutableList.copyOf(leftInferredPredicates);
    this.rightInferredPredicates =
        ImmutableList.copyOf(rightInferredPredicates);
  }

  public static RelOptPredicateList of(Iterable<RexNode> pulledUpPredicates) {
    ImmutableList<RexNode> pulledUpPredicatesList =
        ImmutableList.copyOf(pulledUpPredicates);
    if (pulledUpPredicatesList.isEmpty()) {
      return EMPTY;
    }
    return new RelOptPredicateList(pulledUpPredicatesList, EMPTY_LIST,
        EMPTY_LIST);
  }

  public static RelOptPredicateList of(Iterable<RexNode> pulledUpPredicates,
      Iterable<RexNode> leftInferredPredicates,
      Iterable<RexNode> rightInferredPredicates) {
    final ImmutableList<RexNode> pulledUpPredicatesList =
        ImmutableList.copyOf(pulledUpPredicates);
    final ImmutableList<RexNode> leftInferredPredicateList =
        ImmutableList.copyOf(leftInferredPredicates);
    final ImmutableList<RexNode> rightInferredPredicatesList =
        ImmutableList.copyOf(rightInferredPredicates);
    if (pulledUpPredicatesList.isEmpty()
        && leftInferredPredicateList.isEmpty()
        && rightInferredPredicatesList.isEmpty()) {
      return EMPTY;
    }
    return new RelOptPredicateList(pulledUpPredicatesList,
        leftInferredPredicateList, rightInferredPredicatesList);
  }
}

// End RelOptPulledUpPredicates.java
