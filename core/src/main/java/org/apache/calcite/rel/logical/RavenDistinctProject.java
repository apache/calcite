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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Supplier;

/**
 * RavenDistinctProject is an extension of Project which represent distinct by a boolean value.
 */


public class RavenDistinctProject extends Project {

  boolean distinct;

  public boolean isDistinct() {
    return distinct;
  }

  public void setDistinct(boolean distinct) {
    this.distinct = distinct;
  }

  public RavenDistinctProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType) {
    super(cluster, traitSet, ImmutableList.of(), input, projects, rowType);
    this.distinct = false;
  }

  public RavenDistinctProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType, boolean distinct) {
    super(cluster, traitSet, ImmutableList.of(), input, projects, rowType);
    this.distinct = distinct;
  }

  public static RavenDistinctProject create(
      final RelNode input, final List<? extends RexNode> projects, List<String> fieldNames) {
    RelDataType rowType = RexUtil.
        createStructType(input.getCluster().getTypeFactory(), projects, fieldNames,
            SqlValidatorUtil.F_SUGGESTER);
    return create(input, projects, rowType);
  }

  /**
   * Creates a LogicalProject, specifying row type rather than field names.
   */
  public static RavenDistinctProject create(final RelNode input,
      final List<? extends RexNode> projects, RelDataType rowType) {
    RelOptCluster cluster = input.getCluster();
    RelMetadataQuery mq = cluster.getMetadataQuery();
    Supplier<List<RelCollation>> traitSupplier = () -> RelMdCollation.project(mq, input, projects);
    RelTraitSet traitSet = cluster.traitSet().replace(Convention.NONE).
        replaceIfs(RelCollationTraitDef.INSTANCE, traitSupplier);
    return new RavenDistinctProject(cluster, traitSet, input, projects, rowType, true);
  }

  @Override public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects,
      RelDataType rowType) {
    return new RavenDistinctProject(getCluster(), traitSet, input, exps, rowType, distinct);
  }

}
