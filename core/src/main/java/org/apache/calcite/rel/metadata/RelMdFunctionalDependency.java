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

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Default implementation of
 * {@link RelMetadataQuery#determines(RelNode, int, int)}
 * for the standard logical algebra.
 *
 * <p>The goal of this provider is to determine whether
 * key is functionally dependent on column.
 *
 * <p>If the functional dependency cannot be determined, we return false.
 */
public class RelMdFunctionalDependency
    implements MetadataHandler<BuiltInMetadata.FunctionalDependency> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new RelMdFunctionalDependency(), BuiltInMetadata.FunctionalDependency.Handler.class);

  //~ Constructors -----------------------------------------------------------

  protected RelMdFunctionalDependency() {}

  //~ Methods ----------------------------------------------------------------

  @Override public MetadataDef<BuiltInMetadata.FunctionalDependency> getDef() {
    return BuiltInMetadata.FunctionalDependency.DEF;
  }

  public @Nullable Boolean determines(RelNode rel, RelMetadataQuery mq,
      int key, int column) {
    return determinesImpl2(rel, mq, key, column);
  }

  public @Nullable Boolean determines(SetOp rel, RelMetadataQuery mq,
      int key, int column) {
    return determinesImpl2(rel, mq, key, column);
  }

  public @Nullable Boolean determines(Join rel, RelMetadataQuery mq,
      int key, int column) {
    return determinesImpl2(rel, mq, key, column);
  }

  public @Nullable Boolean determines(Correlate rel, RelMetadataQuery mq,
      int key, int column) {
    return determinesImpl2(rel, mq, key, column);
  }

  public @Nullable Boolean determines(Aggregate rel, RelMetadataQuery mq,
      int key, int column) {
    return determinesImpl(rel, mq, key, column);
  }

  public @Nullable Boolean determines(Calc rel, RelMetadataQuery mq,
      int key, int column) {
    return determinesImpl(rel, mq, key, column);
  }

  public @Nullable Boolean determines(Project rel, RelMetadataQuery mq,
      int key, int column) {
    return determinesImpl(rel, mq, key, column);
  }

  /**
   * Checks if a column is functionally determined by a key column through expression analysis.
   *
   * @param rel The input relation
   * @param mq Metadata query instance
   * @param key Index of the determinant expression
   * @param column Index of the dependent expression
   * @return TRUE if column is determined by key,
   *         FALSE if not determined,
   *         NULL if undetermined
   */
  private static @Nullable Boolean determinesImpl(RelNode rel, RelMetadataQuery mq,
      int key, int column) {
    if (preCheck(rel, key, column)) {
      return true;
    }

    ImmutableBitSet keyInputIndices = null;
    ImmutableBitSet columnInputIndices = null;
    if (rel instanceof Project || rel instanceof Calc) {
      List<RexNode> exprs = null;
      if (rel instanceof Project) {
        Project project = (Project) rel;
        exprs = project.getProjects();
      } else {
        Calc calc = (Calc) rel;
        final RexProgram program = calc.getProgram();
        exprs = program.expandList(program.getProjectList());
      }

      // TODO: Supports dependency analysis for all types of expressions
      if (!(exprs.get(column) instanceof RexInputRef)) {
        return false;
      }

      RexNode keyExpr = exprs.get(key);
      RexNode columnExpr = exprs.get(column);

      // Identical expressions imply functional dependency
      if (keyExpr.equals(columnExpr)) {
        return true;
      }

      keyInputIndices = extractDeterministicRefs(keyExpr);
      columnInputIndices = extractDeterministicRefs(columnExpr);
    } else if (rel instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) rel;

      int groupByCnt = aggregate.getGroupCount();
      if (key < groupByCnt && column >= groupByCnt) {
        return false;
      }

      keyInputIndices = extractDeterministicRefs(aggregate, key);
      columnInputIndices = extractDeterministicRefs(aggregate, column);
    } else {
      throw new UnsupportedOperationException("Unsupported RelNode type: "
          + rel.getClass().getSimpleName());
    }

    // Early return if invalid cases
    if (keyInputIndices.isEmpty()
        || columnInputIndices.isEmpty()) {
      return false;
    }

    // Currently only supports multiple (keyInputIndices) to one (columnInputIndices)
    // dependency detection
    for (Integer keyRef : keyInputIndices) {
      if (Boolean.FALSE.equals(
          mq.determines(rel.getInput(0), keyRef,
          columnInputIndices.nextSetBit(0)))) {
        return false;
      }
    }

    return true;
  }

  /**
   * determinesImpl2is similar to determinesImpl, but it doesn't need to handle the
   * mapping between output and input columns.
   */
  private static @Nullable Boolean determinesImpl2(RelNode rel, RelMetadataQuery mq,
      int key, int column) {
    if (preCheck(rel, key, column)) {
      return true;
    }

    if (rel instanceof TableScan) {
      TableScan tableScan = (TableScan) rel;
      RelOptTable table = tableScan.getTable();
      List<ImmutableBitSet> keys = table.getKeys();
      return keys != null
          && keys.size() == 1
          && keys.get(0).equals(ImmutableBitSet.of(column));
    } else if (rel instanceof Join) {
      Join join = (Join) rel;
      // TODO Considering column mapping based on equality conditions in join
      int leftFieldCnt = join.getLeft().getRowType().getFieldCount();
      if (key < leftFieldCnt && column < leftFieldCnt) {
        return mq.determines(join.getLeft(), key, column);
      } else if (key >= leftFieldCnt && column >= leftFieldCnt) {
        return mq.determines(join.getRight(), key - leftFieldCnt, column - leftFieldCnt);
      }
      return false;
    } else if (rel instanceof Correlate) {
      // TODO Support Correlate.
      return false;
    } else if (rel instanceof SetOp) {
      // TODO Support SetOp
      return false;
    }

    return mq.determines(rel.getInput(0), key, column);
  }

  private static Boolean preCheck(RelNode rel, int key, int column) {
    verifyIndex(rel, key, column);

    // Equal index values indicate the same expression reference
    if (key == column) {
      return true;
    }

    return false;
  }

  private static void verifyIndex(RelNode rel, int... indices) {
    for (int index : indices) {
      if (index < 0 || index >= rel.getRowType().getFieldCount()) {
        throw new IndexOutOfBoundsException(
            "Column index " + index + " is out of bounds. "
                + "Valid range is [0, " + rel.getRowType().getFieldCount() + ")");
      }
    }
  }

  /**
   * Extracts input indices referenced by an output column in an Aggregate.
   * For group-by columns, returns the column index itself since they directly
   * reference input columns. For aggregate function columns, returns the input
   * column indices used by the aggregate call.
   *
   * @param aggregate The Aggregate relational expression to analyze
   * @param index Index of the output column in the Aggregate (0-based)
   * @return ImmutableBitSet of input column indices referenced by the output column.
   *         For group-by columns, returns a singleton set of the column index.
   *         For aggregate columns, returns the argument indices of the aggregate call.
   */
  private static ImmutableBitSet extractDeterministicRefs(Aggregate aggregate, int index) {
    int groupByCnt = aggregate.getGroupCount();
    if (index < groupByCnt) {
      return ImmutableBitSet.of(index);
    }

    List<AggregateCall> aggCalls = aggregate.getAggCallList();
    AggregateCall call = aggCalls.get(index - groupByCnt);
    return ImmutableBitSet.of(call.getArgList());
  }

  /**
   * Extracts input indices referenced by a deterministic RexNode expression.
   *
   * @param rex The expression to analyze
   * @return referenced input indices if deterministic
   */
  private static ImmutableBitSet extractDeterministicRefs(RexNode rex) {
    if (rex instanceof RexCall && !RexUtil.isDeterministic(rex)) {
      return ImmutableBitSet.of();
    }
    return RelOptUtil.InputFinder.bits(rex);
  }
}
