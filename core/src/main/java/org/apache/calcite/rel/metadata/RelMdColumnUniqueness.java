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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.convert.Converter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * RelMdColumnUniqueness supplies a default implementation of
 * {@link RelMetadataQuery#areColumnsUnique} for the standard logical algebra.
 */
public class RelMdColumnUniqueness
    implements MetadataHandler<BuiltInMetadata.ColumnUniqueness> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new RelMdColumnUniqueness(), BuiltInMetadata.ColumnUniqueness.Handler.class);

  //~ Constructors -----------------------------------------------------------

  private RelMdColumnUniqueness() {}

  //~ Methods ----------------------------------------------------------------

  @Override public MetadataDef<BuiltInMetadata.ColumnUniqueness> getDef() {
    return BuiltInMetadata.ColumnUniqueness.DEF;
  }

  public Boolean areColumnsUnique(TableScan scan, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    final BuiltInMetadata.ColumnUniqueness.Handler handler =
        scan.getTable().unwrap(BuiltInMetadata.ColumnUniqueness.Handler.class);
    if (handler != null) {
      return handler.areColumnsUnique(scan, mq, columns, ignoreNulls);
    }
    return scan.getTable().isKey(columns);
  }

  public @Nullable Boolean areColumnsUnique(Filter rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq);
    return mq.areColumnsUnique(rel.getInput(), columns, ignoreNulls);
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.ColumnUniqueness#areColumnsUnique(ImmutableBitSet, boolean)},
   * invoked using reflection, for any relational expression not
   * handled by a more specific method.
   *
   * @param rel Relational expression
   * @param mq Metadata query
   * @param columns column mask representing the subset of columns for which
   *                uniqueness will be determined
   * @param ignoreNulls if true, ignore null values when determining column
   *                    uniqueness
   * @return whether the columns are unique, or
   * null if not enough information is available to make that determination
   *
   * @see org.apache.calcite.rel.metadata.RelMetadataQuery#areColumnsUnique(RelNode, ImmutableBitSet, boolean)
   */
  public @Nullable Boolean areColumnsUnique(RelNode rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    // no information available
    return null;
  }

  public Boolean areColumnsUnique(SetOp rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq);
    // If not ALL then the rows are distinct.
    // Therefore the set of all columns is a key.
    return !rel.all
        && columns.nextClearBit(0) >= rel.getRowType().getFieldCount();
  }

  public Boolean areColumnsUnique(Intersect rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq);
    if (areColumnsUnique((SetOp) rel, mq, columns, ignoreNulls)) {
      return true;
    }
    for (RelNode input : rel.getInputs()) {
      Boolean b = mq.areColumnsUnique(input, columns, ignoreNulls);
      if (b != null && b) {
        return true;
      }
    }
    return false;
  }

  public @Nullable Boolean areColumnsUnique(Minus rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq);
    if (areColumnsUnique((SetOp) rel, mq, columns, ignoreNulls)) {
      return true;
    }
    return mq.areColumnsUnique(rel.getInput(0), columns, ignoreNulls);
  }

  public @Nullable Boolean areColumnsUnique(Sort rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq);
    return mq.areColumnsUnique(rel.getInput(), columns, ignoreNulls);
  }

  public @Nullable Boolean areColumnsUnique(TableModify rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq);
    return mq.areColumnsUnique(rel.getInput(), columns, ignoreNulls);
  }

  public @Nullable Boolean areColumnsUnique(Exchange rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq);
    return mq.areColumnsUnique(rel.getInput(), columns, ignoreNulls);
  }

  public @Nullable Boolean areColumnsUnique(Correlate rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq);
    switch (rel.getJoinType()) {
    case ANTI:
    case SEMI:
      return mq.areColumnsUnique(rel.getLeft(), columns, ignoreNulls);
    case LEFT:
    case INNER:
      final Pair<ImmutableBitSet, ImmutableBitSet> leftAndRightColumns =
          splitLeftAndRightColumns(rel.getLeft().getRowType().getFieldCount(),
              columns);
      final ImmutableBitSet leftColumns = leftAndRightColumns.left;
      final ImmutableBitSet rightColumns = leftAndRightColumns.right;
      final RelNode left = rel.getLeft();
      final RelNode right = rel.getRight();

      if (leftColumns.cardinality() > 0
          && rightColumns.cardinality() > 0) {
        Boolean leftUnique =
            mq.areColumnsUnique(left, leftColumns, ignoreNulls);
        Boolean rightUnique =
            mq.areColumnsUnique(right, rightColumns, ignoreNulls);
        if (leftUnique == null || rightUnique == null) {
          return null;
        } else {
          return leftUnique && rightUnique;
        }
      } else {
        return null;
      }
    default:
      throw new IllegalStateException("Unknown join type " + rel.getJoinType()
          + " for correlate relation " + rel);
    }
  }

  public @Nullable Boolean areColumnsUnique(Project rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq);
    // LogicalProject maps a set of rows to a different set;
    // Without knowledge of the mapping function(whether it
    // preserves uniqueness), it is only safe to derive uniqueness
    // info from the child of a project when the mapping is f(a) => a.
    //
    // Also need to map the input column set to the corresponding child
    // references

    return areProjectColumnsUnique(rel, mq, columns, ignoreNulls, rel.getProjects());
  }

  public @Nullable Boolean areColumnsUnique(Calc rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq);
    RexProgram program = rel.getProgram();

    return areProjectColumnsUnique(rel, mq, columns, ignoreNulls,
        Util.transform(program.getProjectList(), program::expandLocalRef));
  }

  private static @Nullable Boolean areProjectColumnsUnique(
      SingleRel rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls, List<RexNode> projExprs) {
    RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
    ImmutableBitSet.Builder childColumns = ImmutableBitSet.builder();
    for (int bit : columns) {
      RexNode projExpr = projExprs.get(bit);
      if (projExpr instanceof RexInputRef) {
        childColumns.set(((RexInputRef) projExpr).getIndex());
      } else if (projExpr instanceof RexCall && ignoreNulls) {
        // If the expression is a cast such that the types are the same
        // except for the nullability, then if we're ignoring nulls,
        // it doesn't matter whether the underlying column reference
        // is nullable.  Check that the types are the same by making a
        // nullable copy of both types and then comparing them.
        RexCall call = (RexCall) projExpr;
        if (call.getOperator() != SqlStdOperatorTable.CAST) {
          continue;
        }
        RexNode castOperand = call.getOperands().get(0);
        if (!(castOperand instanceof RexInputRef)) {
          continue;
        }
        RelDataType castType =
            typeFactory.createTypeWithNullability(
                projExpr.getType(), true);
        RelDataType origType = typeFactory.createTypeWithNullability(
            castOperand.getType(),
            true);
        if (castType.equals(origType)) {
          childColumns.set(((RexInputRef) castOperand).getIndex());
        }
      } else {
        // If the expression will not influence uniqueness of the
        // projection, then skip it.
        continue;
      }
    }

    // If no columns can affect uniqueness, then return unknown
    if (childColumns.cardinality() == 0) {
      return null;
    }

    return mq.areColumnsUnique(rel.getInput(), childColumns.build(),
        ignoreNulls);
  }

  public @Nullable Boolean areColumnsUnique(Join rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq);
    if (columns.cardinality() == 0) {
      return false;
    }

    final RelNode left = rel.getLeft();
    final RelNode right = rel.getRight();

    // Semi or anti join should ignore uniqueness of the right input.
    if (!rel.getJoinType().projectsRight()) {
      return mq.areColumnsUnique(left, columns, ignoreNulls);
    }

    // Divide up the input column mask into column masks for the left and
    // right sides of the join
    final Pair<ImmutableBitSet, ImmutableBitSet> leftAndRightColumns =
        splitLeftAndRightColumns(rel.getLeft().getRowType().getFieldCount(),
            columns);
    final ImmutableBitSet leftColumns = leftAndRightColumns.left;
    final ImmutableBitSet rightColumns = leftAndRightColumns.right;

    // for FULL OUTER JOIN if columns contain column from both inputs it is not
    // guaranteed that the result will be unique
    if (!ignoreNulls && rel.getJoinType() == JoinRelType.FULL
        && leftColumns.cardinality() > 0 && rightColumns.cardinality() > 0) {
      return false;
    }

    // If the original column mask contains columns from both the left and
    // right hand side, then the columns are unique if and only if they're
    // unique for their respective join inputs
    Boolean leftUnique = mq.areColumnsUnique(left, leftColumns, ignoreNulls);
    Boolean rightUnique = mq.areColumnsUnique(right, rightColumns, ignoreNulls);
    if ((leftColumns.cardinality() > 0)
        && (rightColumns.cardinality() > 0)) {
      if ((leftUnique == null) || (rightUnique == null)) {
        return null;
      } else {
        return leftUnique && rightUnique;
      }
    }

    // If we're only trying to determine uniqueness for columns that
    // originate from one join input, then determine if the equijoin
    // columns from the other join input are unique.  If they are, then
    // the columns are unique for the entire join if they're unique for
    // the corresponding join input, provided that input is not null
    // generating.
    final JoinInfo joinInfo = rel.analyzeCondition();
    if (leftColumns.cardinality() > 0) {
      if (rel.getJoinType().generatesNullsOnLeft()) {
        return false;
      }
      Boolean rightJoinColsUnique =
          mq.areColumnsUnique(right, joinInfo.rightSet(), ignoreNulls);
      if ((rightJoinColsUnique == null) || (leftUnique == null)) {
        return null;
      }
      return rightJoinColsUnique && leftUnique;
    } else if (rightColumns.cardinality() > 0) {
      if (rel.getJoinType().generatesNullsOnRight()) {
        return false;
      }
      Boolean leftJoinColsUnique =
          mq.areColumnsUnique(left, joinInfo.leftSet(), ignoreNulls);
      if ((leftJoinColsUnique == null) || (rightUnique == null)) {
        return null;
      }
      return leftJoinColsUnique && rightUnique;
    }

    throw new AssertionError();
  }

  public @Nullable Boolean areColumnsUnique(Aggregate rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    if (Aggregate.isSimple(rel) || ignoreNulls) {
      columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq);
      // group by keys form a unique key
      final ImmutableBitSet groupKey = ImmutableBitSet.range(rel.getGroupCount());
      final boolean contained = columns.contains(groupKey);
      if (contained) {
        return true;
      } else if (!Aggregate.isSimple(rel)) {
        return false;
      }

      final ImmutableBitSet commonKeys = columns.intersect(groupKey);
      if (commonKeys.isEmpty()) {
        return false;
      }
      final ImmutableBitSet.Builder targetColumns = ImmutableBitSet.builder();
      for (int key: commonKeys) {
        targetColumns.set(rel.getGroupSet().nth(key));
      }

      return mq.areColumnsUnique(rel.getInput(), targetColumns.build(), ignoreNulls);
    }
    return null;
  }

  public Boolean areColumnsUnique(Values rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq);
    if (rel.tuples.size() < 2) {
      return true;
    }
    final Set<List<Comparable>> set = new HashSet<>();
    final List<Comparable> values = new ArrayList<>(columns.cardinality());
    for (ImmutableList<RexLiteral> tuple : rel.tuples) {
      for (int column : columns) {
        final RexLiteral literal = tuple.get(column);
        Comparable value = literal.getValueAs(Comparable.class);
        values.add(value == null ? NullSentinel.INSTANCE : value);
      }
      if (!set.add(ImmutableList.copyOf(values))) {
        return false;
      }
      values.clear();
    }
    return true;
  }

  public @Nullable Boolean areColumnsUnique(Converter rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq);
    return mq.areColumnsUnique(rel.getInput(), columns, ignoreNulls);
  }

  public @Nullable Boolean areColumnsUnique(RelSubset rel, RelMetadataQuery mq,
      ImmutableBitSet columns, boolean ignoreNulls) {
    columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq);
    for (RelNode rel2 : rel.getRels()) {
      if (rel2 instanceof Aggregate
          || rel2 instanceof Filter
          || rel2 instanceof Values
          || rel2 instanceof Sort
          || rel2 instanceof TableScan
          || simplyProjects(rel2, columns)) {
        try {
          final Boolean unique = mq.areColumnsUnique(rel2, columns, ignoreNulls);
          if (unique != null) {
            if (unique) {
              return true;
            }
          } else {
            return null;
          }
        } catch (CyclicMetadataException e) {
          // Ignore this relational expression; there will be non-cyclic ones
          // in this set.
        }
      }
    }
    return false;
  }

  private static boolean simplyProjects(RelNode rel, ImmutableBitSet columns) {
    if (!(rel instanceof Project)) {
      return false;
    }
    Project project = (Project) rel;
    final List<RexNode> projects = project.getProjects();
    for (int column : columns) {
      if (column >= projects.size()) {
        return false;
      }
      if (!(projects.get(column) instanceof RexInputRef)) {
        return false;
      }
      final RexInputRef ref = (RexInputRef) projects.get(column);
      if (ref.getIndex() != column) {
        return false;
      }
    }
    return true;
  }

  /** Splits a column set between left and right sets. */
  private static Pair<ImmutableBitSet, ImmutableBitSet>
      splitLeftAndRightColumns(int leftCount, final ImmutableBitSet columns) {
    ImmutableBitSet.Builder leftBuilder = ImmutableBitSet.builder();
    ImmutableBitSet.Builder rightBuilder = ImmutableBitSet.builder();
    for (int bit : columns) {
      if (bit < leftCount) {
        leftBuilder.set(bit);
      } else {
        rightBuilder.set(bit - leftCount);
      }
    }
    return Pair.of(leftBuilder.build(), rightBuilder.build());
  }

  /**
   * Deduce constant columns from predicates of rel and return the union
   * bitsets of checkingColumns and the constant columns.
   */
  private static ImmutableBitSet decorateWithConstantColumnsFromPredicates(
      ImmutableBitSet checkingColumns, RelNode rel, RelMetadataQuery mq) {
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(rel);
    if (!RelOptPredicateList.isEmpty(predicates)) {
      final Set<Integer> constantIndexes = new HashSet<>();
      predicates.constantMap.keySet().forEach(rex -> {
        if (rex instanceof RexInputRef) {
          constantIndexes.add(((RexInputRef) rex).getIndex());
        }
      });
      if (!constantIndexes.isEmpty()) {
        return checkingColumns.union(ImmutableBitSet.of(constantIndexes));
      }
    }
    // If no constant columns deduced, return the original "checkingColumns".
    return checkingColumns;
  }
}
