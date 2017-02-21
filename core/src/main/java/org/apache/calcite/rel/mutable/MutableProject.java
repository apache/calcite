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
package org.apache.calcite.rel.mutable;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;

import java.util.List;
import java.util.Objects;

/** Mutable equivalent of
 * {@link org.apache.calcite.rel.logical.LogicalProject}. */
public class MutableProject extends MutableSingleRel {
  public final List<RexNode> projects;

  private MutableProject(RelDataType rowType, MutableRel input,
      List<RexNode> projects) {
    super(MutableRelType.PROJECT, rowType, input);
    this.projects = projects;
    assert RexUtil.compatibleTypes(projects, rowType, Litmus.THROW);
  }

  public static MutableProject of(RelDataType rowType, MutableRel input,
      List<RexNode> projects) {
    return new MutableProject(rowType, input, projects);
  }

  /** Equivalent to
   * {@link RelOptUtil#createProject(org.apache.calcite.rel.RelNode, java.util.List, java.util.List)}
   * for {@link MutableRel}. */
  public static MutableRel of(MutableRel child, List<RexNode> exprList,
      List<String> fieldNameList) {
    final RelDataType rowType =
        RexUtil.createStructType(child.cluster.getTypeFactory(), exprList,
            fieldNameList, SqlValidatorUtil.F_SUGGESTER);
    return of(rowType, child, exprList);
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof MutableProject
        && MutableRel.PAIRWISE_STRING_EQUIVALENCE.equivalent(
            projects, ((MutableProject) obj).projects)
        && input.equals(((MutableProject) obj).input);
  }

  @Override public int hashCode() {
    return Objects.hash(input,
        MutableRel.PAIRWISE_STRING_EQUIVALENCE.hash(projects));
  }

  @Override public StringBuilder digest(StringBuilder buf) {
    return buf.append("Project(projects: ").append(projects).append(")");
  }

  /** Returns a list of (expression, name) pairs. */
  public final List<Pair<RexNode, String>> getNamedProjects() {
    return Pair.zip(projects, getRowType().getFieldNames());
  }

  public Mappings.TargetMapping getMapping() {
    return Project.getMapping(
        input.getRowType().getFieldCount(), projects);
  }

  @Override public MutableRel clone() {
    return MutableProject.of(rowType, input.clone(), projects);
  }
}

// End MutableProject.java
