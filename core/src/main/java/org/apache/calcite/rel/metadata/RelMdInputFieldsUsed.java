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
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Set;

/**
 * Metadata provider to determine which input fields are used by a RelNode.
 *
 * <p>A field is considered "used" if it is referenced by the relational
 * expression. The result is an {@link ImmutableBitSet} where bits correspond to
 * input column ordinals.
 *
 * <p>Examples:
 * <ul>
 *   <li>For an {@link Aggregate}, "used" fields are those in the group set or
 *   referenced in aggregate functions. see {@link RelOptUtil#getAllFields}</li>
 *   <li>For a {@link Join}, it is the union of "used" fields from both inputs
 *   (shifted appropriately for the right input). For SEMI and ANTI joins, fields
 *   from the right input are not considered "used" as they are not projected to
 *   the output</li>
 * </ul>
 *
 * @see BuiltInMetadata.InputFieldsUsed
 * @see RelMetadataQuery#getInputFieldsUsed(RelNode)
 */
public class RelMdInputFieldsUsed
    implements MetadataHandler<BuiltInMetadata.InputFieldsUsed> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new RelMdInputFieldsUsed(), BuiltInMetadata.InputFieldsUsed.Handler.class);

  @Override public MetadataDef<BuiltInMetadata.InputFieldsUsed> getDef() {
    return BuiltInMetadata.InputFieldsUsed.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.InputFieldsUsed#getInputFieldsUsed()},
   * invoked using reflection.
   *
   * @see org.apache.calcite.rel.metadata.RelMetadataQuery#getInputFieldsUsed(RelNode)
   */
  public ImmutableBitSet getInputFieldsUsed(RelNode rel, RelMetadataQuery mq) {
    // By default, a RelNode uses all of its input fields.
    return getAllInputFieldsUsed(rel);
  }

  public ImmutableBitSet getInputFieldsUsed(TableScan scan, RelMetadataQuery mq) {
    final BuiltInMetadata.InputFieldsUsed.Handler handler =
        scan.getTable().unwrap(BuiltInMetadata.InputFieldsUsed.Handler.class);
    if (handler != null) {
      return handler.getInputFieldsUsed(scan, mq);
    }
    final int fieldCount = scan.getRowType().getFieldCount();
    return ImmutableBitSet.range(fieldCount);
  }

  public ImmutableBitSet getInputFieldsUsed(Project project, RelMetadataQuery mq) {
    // Project involves column trimming, returning only the columns that are used.
    return RelOptUtil.InputFinder.bits(project.getProjects(), null);
  }

  public ImmutableBitSet getInputFieldsUsed(Filter filter, RelMetadataQuery mq) {
    return getAllFieldsUsed(filter);
  }

  public ImmutableBitSet getInputFieldsUsed(Sort sort, RelMetadataQuery mq) {
    return getAllFieldsUsed(sort);
  }

  public ImmutableBitSet getInputFieldsUsed(Window window, RelMetadataQuery mq) {
    return getAllFieldsUsed(window);
  }

  public ImmutableBitSet getInputFieldsUsed(Calc calc, RelMetadataQuery mq) {
    final RexProgram program = calc.getProgram();
    final List<RexNode> expandedProjects = program.expandList(program.getProjectList());
    final RexNode cond = program.getCondition() == null
        ? null
        : program.expandLocalRef(program.getCondition());

    // Same as Project.
    return RelOptUtil.InputFinder.bits(expandedProjects, cond);
  }

  public ImmutableBitSet getInputFieldsUsed(Join join, RelMetadataQuery mq) {
    // Computes the union of fields used by both inputs. For SEMI and ANTI joins,
    // fields from the right input are excluded as they are not projected to the output.
    final ImmutableBitSet leftInputFieldsUsed = getAllFieldsUsed(join.getLeft());
    if (join.getJoinType() == JoinRelType.SEMI
        || join.getJoinType() == JoinRelType.ANTI) {
      return leftInputFieldsUsed;
    }

    final ImmutableBitSet rightInputFieldsUsedShifted =
        getAllFieldsUsed(join.getRight(),
            join.getLeft().getRowType().getFieldCount());
    return leftInputFieldsUsed.union(rightInputFieldsUsedShifted);
  }

  public ImmutableBitSet getInputFieldsUsed(SetOp setOp, RelMetadataQuery mq) {
    return getAllInputFieldsUsed(setOp);
  }

  public ImmutableBitSet getInputFieldsUsed(Aggregate agg, RelMetadataQuery mq) {
    Set<Integer> fields = RelOptUtil.getAllFields(agg);
    return ImmutableBitSet.of(fields);
  }

  public ImmutableBitSet getInputFieldsUsed(Correlate correlate, RelMetadataQuery mq) {
    // Computes the union of fields referenced by both inputs. For SEMI and ANTI
    // correlates, fields from the right input are excluded from the projection.
    final ImmutableBitSet leftInputFieldsUsed = getAllFieldsUsed(correlate.getLeft());
    if (correlate.getJoinType() == JoinRelType.SEMI
        || correlate.getJoinType() == JoinRelType.ANTI) {
      return leftInputFieldsUsed;
    }

    final ImmutableBitSet rightInputFieldsUsedShifted =
        getAllFieldsUsed(correlate.getRight(),
            correlate.getLeft().getRowType().getFieldCount());
    return leftInputFieldsUsed.union(rightInputFieldsUsedShifted);
  }

  // ~ Private helper methods ------------------------------------------------

  /**
   * Returns a bitset of all fields used by all inputs of a {@link RelNode},
   * shifted by the cumulative field count of preceding inputs.
   */
  private static ImmutableBitSet getAllInputFieldsUsed(RelNode rel) {
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    int offset = 0;
    for (RelNode input : rel.getInputs()) {
      builder.addAll(getAllFieldsUsed(input, offset));
      offset += input.getRowType().getFieldCount();
    }
    return builder.build();
  }

  private static ImmutableBitSet getAllFieldsUsed(RelNode rel, int offset) {
    return ImmutableBitSet.range(rel.getRowType().getFieldCount()).shift(offset);
  }

  private static ImmutableBitSet getAllFieldsUsed(RelNode rel) {
    return getAllFieldsUsed(rel, 0);
  }
}
