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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;

/**
 * Metadata provider to determine which input fields are used by a RelNode.
 */
public class RelMdInputFieldsUsed
    implements MetadataHandler<BuiltInMetadata.InputFieldsUsed> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new RelMdInputFieldsUsed(), BuiltInMetadata.InputFieldsUsed.Handler.class);

  @Override public MetadataDef<BuiltInMetadata.InputFieldsUsed> getDef() {
    return BuiltInMetadata.InputFieldsUsed.DEF;
  }

  public ImmutableList<ImmutableBitSet> getInputFieldsUsed(RelNode rel,
      RelMetadataQuery mq) {
    ImmutableList.Builder<ImmutableBitSet> builder = ImmutableList.builder();
    rel.getInputs().forEach(input -> {
      builder.addAll(mq.getInputFieldsUsed(input));
    });
    return builder.build();
  }

  public ImmutableList<ImmutableBitSet> getInputFieldsUsed(TableScan scan,
      RelMetadataQuery mq) {
    final BuiltInMetadata.InputFieldsUsed.Handler handler =
        scan.getTable().unwrap(BuiltInMetadata.InputFieldsUsed.Handler.class);
    if (handler != null) {
      return handler.getInputFieldsUsed(scan, mq);
    }
    final int fieldCount = scan.getRowType().getFieldCount();
    return ImmutableList.of(ImmutableBitSet.range(fieldCount));
  }

  public ImmutableList<ImmutableBitSet> getInputFieldsUsed(Project project,
      RelMetadataQuery mq) {
    final ImmutableBitSet bits = RelOptUtil.InputFinder.bits(project.getProjects(), null);
    return ImmutableList.of(bits);
  }

  public ImmutableList<ImmutableBitSet> getInputFieldsUsed(Filter filter,
      RelMetadataQuery mq) {
    return mq.getInputFieldsUsed(filter.getInput());
  }

  public ImmutableList<ImmutableBitSet> getInputFieldsUsed(Calc calc,
      RelMetadataQuery mq) {
    final RexProgram program = calc.getProgram();
    final List<RexNode> expandedProjects = program.expandList(program.getProjectList());
    final RexNode cond = program.getCondition() == null
        ? null
        : program.expandLocalRef(program.getCondition());
    final ImmutableBitSet bits = RelOptUtil.InputFinder.bits(expandedProjects, cond);
    return ImmutableList.of(bits);
  }

  public ImmutableList<ImmutableBitSet> getInputFieldsUsed(Join join,
      RelMetadataQuery mq) {
    List<ImmutableBitSet> leftInputFieldsUsed = mq.getInputFieldsUsed(join.getLeft());
    List<ImmutableBitSet> rightInputFieldsUsed = mq.getInputFieldsUsed(join.getRight());
    assert leftInputFieldsUsed.size() == 1 && rightInputFieldsUsed.size() == 1;

    ImmutableBitSet rightUsedBits = rightInputFieldsUsed.get(0);
    if (join.getJoinType() == JoinRelType.SEMI
        ||  join.getJoinType() == JoinRelType.ANTI) {
      rightUsedBits = ImmutableBitSet.of();
    }

    return ImmutableList.of(leftInputFieldsUsed.get(0), rightUsedBits);
  }

  public ImmutableList<ImmutableBitSet> getInputFieldsUsed(SetOp setOp,
      RelMetadataQuery mq) {
    final ImmutableList.Builder<ImmutableBitSet> builder = ImmutableList.builder();
    for (RelNode input : setOp.getInputs()) {
      ImmutableList<ImmutableBitSet> inputFieldsBits = mq.getInputFieldsUsed(input);
      assert inputFieldsBits.size() == 1;
      builder.add(inputFieldsBits.get(0));
    }
    return builder.build();
  }

  public ImmutableList<ImmutableBitSet> getInputFieldsUsed(Aggregate agg,
      RelMetadataQuery mq) {
    Set<Integer> fields =  RelOptUtil.getAllFields(agg);
    return ImmutableList.of(ImmutableBitSet.of(fields));
  }
}
