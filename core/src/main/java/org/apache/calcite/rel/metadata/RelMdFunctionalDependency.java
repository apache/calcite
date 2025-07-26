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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

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
    return preCheck(rel, mq, key, column);
  }

  public @Nullable Boolean determines(SingleRel rel, RelMetadataQuery mq,
      int key, int column) {
    if (preCheck(rel, mq, key, column)) {
      return true;
    }
    return mq.determines(rel.getInput(), key, column);
  }

  public @Nullable Boolean determines(SetOp rel, RelMetadataQuery mq,
      int key, int column) {
    if (preCheck(rel, mq, key, column)) {
      return true;
    }
    return mq.determines(rel.getInput(0), key, column);
  }

  public @Nullable Boolean determines(Project rel, RelMetadataQuery mq,
      int key, int column) {
    if (preCheck(rel, mq, key, column)) {
      return true;
    }

    List<RexNode> projects = rel.getProjects();
    RexNode keyExpr = projects.get(key);
    RexNode columnExpr = projects.get(column);
    // Identical expressions imply deterministic dependency
    if (keyExpr.equals(columnExpr)) {
      return true;
    }

    // Maps expression indices to their corresponding inputs
    Set<Integer> keyInputIndices = extractDeterministicRefs(keyExpr);
    Set<Integer> columnInputIndices = extractDeterministicRefs(columnExpr);

    if (keyInputIndices == null || columnInputIndices == null) {
      return false;
    }

    for (Integer columnRef : columnInputIndices) {
      for (Integer keyRef : keyInputIndices) {
        if (Boolean.FALSE.equals(mq.determines(rel.getInput(), keyRef, columnRef))) {
          return false;
        }
      }
    }

    return true;
  }

  private static Boolean preCheck(RelNode rel, RelMetadataQuery mq,
      int key, int column) {
    verifyIndex(rel, key, column);

    // Equal index values indicate the same expression reference
    if (key == column) {
      return true;
    }

    // Unique column index implies deterministic dependency
    if (isUnique(rel, mq, column)) {
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
   * Extracts input indices referenced by a RexNode expression if it's deterministic.
   * For deterministic expressions, returns all referenced indices.
   * For non-deterministic expressions or unsupported node types, returns null.
   *
   * @param rex The expression to analyze
   * @return Set of input indices if deterministic, null otherwise
   */
  private static @Nullable Set<Integer> extractDeterministicRefs(RexNode rex) {
    if (rex instanceof RexInputRef) {
      return ImmutableSet.of(((RexInputRef) rex).getIndex());
    } else if (rex instanceof RexCall) {
      Set<Integer> inputRefs = new HashSet<>();
      if (isDeterministic(rex, inputRefs)) {
        return inputRefs;
      }
      // The presence of any non-deterministic index invalidates the input mapping
      return null;
    }
    // TODO Processes all other types of RexNode expressions not previously handled
    return null;
  }

  /**
   * Checks if an expression is deterministic and collects referenced indices.
   *
   * @param node expression node to check
   * @param inputRefs set to store referenced indices
   * @return true if the expression is deterministic, false otherwise
   */
  private static boolean isDeterministic(RexNode node, Set<Integer> inputRefs) {
    if (node instanceof RexInputRef) {
      inputRefs.add(((RexInputRef) node).getIndex());
      return true;
    }

    if (node instanceof RexCall) {
      RexCall call = (RexCall) node;
      SqlOperator operator = call.getOperator();

      if (!operator.isDeterministic()) {
        return false;
      }

      for (RexNode operand : call.getOperands()) {
        if (!isDeterministic(operand, inputRefs)) {
          return false;
        }
      }
      return true;
    }

    return true;
  }

  /**
   * Checks if the specified column is unique in the relation.
   *
   * @param rel the relation node
   * @param mq metadata query instance
   * @param index column index to check
   * @return true if the column values are unique, false otherwise
   */
  private static Boolean isUnique(RelNode rel, RelMetadataQuery mq, int index) {
    final ImmutableBitSet columnBitSet = ImmutableBitSet.builder()
        .set(index)
        .build();
    return requireNonNull(mq.areColumnsUnique(rel, columnBitSet, false));
  }

}
