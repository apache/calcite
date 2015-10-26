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

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.Converter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * RelMdColumnUniqueness supplies a default implementation of
 * {@link RelMetadataQuery#areColumnsUnique} for the standard logical algebra.
 */
public class RelMdColumnUniqueness {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.COLUMN_UNIQUENESS.method, new RelMdColumnUniqueness());

  //~ Constructors -----------------------------------------------------------

  private RelMdColumnUniqueness() {}

  //~ Methods ----------------------------------------------------------------

  public Boolean areColumnsUnique(
      TableScan rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    return rel.getTable().isKey(columns);
  }

  public Boolean areColumnsUnique(
      Filter rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    return RelMetadataQuery.areColumnsUnique(
        rel.getInput(),
        columns,
        ignoreNulls);
  }

  public Boolean areColumnsUnique(
      Sort rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    return RelMetadataQuery.areColumnsUnique(
        rel.getInput(),
        columns,
        ignoreNulls);
  }

  public Boolean areColumnsUnique(
      SetOp rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    // If not ALL then the rows are distinct.
    // Therefore the set of all columns is a key.
    return !rel.all
        && columns.nextClearBit(0) >= rel.getRowType().getFieldCount();
  }

  public Boolean areColumnsUnique(
      Intersect rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    if (areColumnsUnique((SetOp) rel, columns, ignoreNulls)) {
      return true;
    }
    for (RelNode input : rel.getInputs()) {
      Boolean b = RelMetadataQuery.areColumnsUnique(
          input,
          columns,
          ignoreNulls);
      if (b != null && b) {
        return true;
      }
    }
    return false;
  }

  public Boolean areColumnsUnique(
      Minus rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    if (areColumnsUnique((SetOp) rel, columns, ignoreNulls)) {
      return true;
    }
    return RelMetadataQuery.areColumnsUnique(
        rel.getInput(0),
        columns,
        ignoreNulls);
  }

  public Boolean areColumnsUnique(
      Exchange rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    return RelMetadataQuery.areColumnsUnique(
        rel.getInput(),
        columns,
        ignoreNulls);
  }

  public Boolean areColumnsUnique(
      Correlate rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    return RelMetadataQuery.areColumnsUnique(
        rel.getLeft(),
        columns,
        ignoreNulls);
  }

  public Boolean areColumnsUnique(
      Project rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    // LogicalProject maps a set of rows to a different set;
    // Without knowledge of the mapping function(whether it
    // preserves uniqueness), it is only safe to derive uniqueness
    // info from the child of a project when the mapping is f(a) => a.
    //
    // Also need to map the input column set to the corresponding child
    // references

    List<RexNode> projExprs = rel.getProjects();
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
        RelDataTypeFactory typeFactory =
            rel.getCluster().getTypeFactory();
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

    return RelMetadataQuery.areColumnsUnique(
        rel.getInput(),
        childColumns.build(),
        ignoreNulls);
  }

  public Boolean areColumnsUnique(
      Join rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    if (columns.cardinality() == 0) {
      return false;
    }

    final RelNode left = rel.getLeft();
    final RelNode right = rel.getRight();

    // Divide up the input column mask into column masks for the left and
    // right sides of the join
    ImmutableBitSet.Builder leftBuilder = ImmutableBitSet.builder();
    ImmutableBitSet.Builder rightBuilder = ImmutableBitSet.builder();
    int nLeftColumns = left.getRowType().getFieldCount();
    for (int bit : columns) {
      if (bit < nLeftColumns) {
        leftBuilder.set(bit);
      } else {
        rightBuilder.set(bit - nLeftColumns);
      }
    }

    // If the original column mask contains columns from both the left and
    // right hand side, then the columns are unique if and only if they're
    // unique for their respective join inputs
    final ImmutableBitSet leftColumns = leftBuilder.build();
    Boolean leftUnique =
        RelMetadataQuery.areColumnsUnique(left, leftColumns, ignoreNulls);
    final ImmutableBitSet rightColumns = rightBuilder.build();
    Boolean rightUnique =
        RelMetadataQuery.areColumnsUnique(right, rightColumns, ignoreNulls);
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
          RelMetadataQuery.areColumnsUnique(right, joinInfo.rightSet(),
              ignoreNulls);
      if ((rightJoinColsUnique == null) || (leftUnique == null)) {
        return null;
      }
      return rightJoinColsUnique && leftUnique;
    } else if (rightColumns.cardinality() > 0) {
      if (rel.getJoinType().generatesNullsOnRight()) {
        return false;
      }
      Boolean leftJoinColsUnique =
          RelMetadataQuery.areColumnsUnique(left, joinInfo.leftSet(),
              ignoreNulls);
      if ((leftJoinColsUnique == null) || (rightUnique == null)) {
        return null;
      }
      return leftJoinColsUnique && rightUnique;
    }

    throw new AssertionError();
  }

  public Boolean areColumnsUnique(
      SemiJoin rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    // only return the unique keys from the LHS since a semijoin only
    // returns the LHS
    return RelMetadataQuery.areColumnsUnique(
        rel.getLeft(),
        columns,
        ignoreNulls);
  }

  public Boolean areColumnsUnique(
      Aggregate rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    // group by keys form a unique key
    ImmutableBitSet groupKey = ImmutableBitSet.range(rel.getGroupCount());
    return columns.contains(groupKey);
  }

  // Catch-all rule when none of the others apply.
  public Boolean areColumnsUnique(
      RelNode rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    // no information available
    return null;
  }

  public Boolean areColumnsUnique(
      Converter rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    return RelMetadataQuery.areColumnsUnique(
        rel.getInput(),
        columns,
        ignoreNulls);
  }

  public Boolean areColumnsUnique(
      Values rel,
      RelMetadataQuery query,
      boolean ignoreNulls) {
    if (rel.getTuples().size() < 2) {
      return true;
    }
    final Set<List<Comparable>> set = new HashSet<>();
    final List<Comparable> values = new ArrayList<>();
    for (ImmutableList<RexLiteral> tuple : rel.getTuples()) {
      for (RexLiteral literal : tuple) {
        values.add(NullSentinel.mask(literal.getValue()));
      }
      if (!set.add(ImmutableList.copyOf(values))) {
        return false;
      }
      values.clear();
    }
    return true;
  }

  public Boolean areColumnsUnique(
      boolean dummy, // prevent method from being used
      HepRelVertex rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    return RelMetadataQuery.areColumnsUnique(
        rel.getCurrentRel(),
        columns,
        ignoreNulls);
  }

  public Boolean areColumnsUnique(
      RelSubset rel,
      ImmutableBitSet columns,
      boolean ignoreNulls) {
    final RelNode best = rel.getBest();
    if (best == null) {
      return null;
    } else {
      return RelMetadataQuery.areColumnsUnique(best, columns, ignoreNulls);
    }
  }

  /** Aggregate and Calc are "safe" children of a RelSubset to delve into. */
  private static final Predicate<RelNode> SAFE_REL =
      new Predicate<RelNode>() {
        public boolean apply(RelNode r) {
          return r instanceof Aggregate || r instanceof Project;
        }
      };
}

// End RelMdColumnUniqueness.java
