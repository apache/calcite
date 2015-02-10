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
package org.apache.calcite.rel.core;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldPartitioning;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelPartitioning;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;

/**
 * Relational expression that shuffles its input without otherwise
 * changing its content.
 */
public abstract class Shuffle extends SingleRel {

//~ Instance fields --------------------------------------------------------
  protected final RelCollation collation;

  protected final RelPartitioning partitioning;

  protected final ImmutableList<RexNode> sortExps;

  protected final ImmutableList<RexNode> partitioningExps;
  /**
   * Creates a Shuffle.
   *
   * @param cluster      Cluster this relational expression belongs to
   * @param traits       Traits
   * @param child        input relational expression
   * @param collation    array of sort specifications
   * @param partitioning array of partitioning specifications
   */

  public Shuffle(RelOptCluster cluster, RelTraitSet traits, RelNode input,
    RelCollation collation, RelPartitioning partitioning) {
    super(cluster, traits, input);
    this.collation = collation;
    this.partitioning = partitioning;

    // If partitioning is not needed, than Sort may serve your purpose better.
    assert this.partitioning != null;

    ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    for (RelFieldCollation field : collation.getFieldCollations()) {
      int index = field.getFieldIndex();
      builder.add(cluster.getRexBuilder().makeInputRef(input, index));
    }
    sortExps = builder.build();

    builder = ImmutableList.builder();
    for (RelFieldPartitioning field : partitioning.getPartitioningFields()) {
      int index = field.getFieldIndex();
      builder.add(cluster.getRexBuilder().makeInputRef(input, index));
    }
    partitioningExps = builder.build();
  }

  /**
   * Creates a Shuffle.
   *
   * @param cluster      Cluster this relational expression belongs to
   * @param traits       Traits
   * @param child        input relational expression
   * @param partitioning array of partitioning specifications
   */

  public Shuffle(RelOptCluster cluster, RelTraitSet traits, RelNode input,
     RelPartitioning partitioning) {
    // its ok to have partitioning only and no sorting.
    this(cluster, traits, input, RelCollations.EMPTY, partitioning);
  }

  @Override public List<RexNode> getChildExps() {
    return partitioningExps;
  }

  public List<RexNode> getSortExps() {
    return sortExps;
  }
}
