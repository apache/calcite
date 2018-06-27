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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Relational expression whose value is a sequence of zero or more literal row
 * values.
 */
public abstract class Values extends AbstractRelNode {
  /**
   * Lambda that helps render tuples as strings.
   */
  private static final Function<ImmutableList<RexLiteral>, Object> F =
      new Function<ImmutableList<RexLiteral>, Object>() {
        public Object apply(ImmutableList<RexLiteral> tuple) {
          String s = tuple.toString();
          assert s.startsWith("[");
          assert s.endsWith("]");
          return "{ " + s.substring(1, s.length() - 1) + " }";
        }
      };

  /** Predicate, to be used when defining an operand of a {@link RelOptRule},
   * that returns true if a Values contains zero tuples.
   *
   * <p>This is the conventional way to represent an empty relational
   * expression. There are several rules that recognize empty relational
   * expressions and prune away that section of the tree.
   */
  public static final Predicate<? super Values> IS_EMPTY =
      new PredicateImpl<Values>() {
        public boolean test(Values values) {
          return values.getTuples().isEmpty();
        }
      };

  /** Predicate, to be used when defining an operand of a {@link RelOptRule},
   * that returns true if a Values contains one or more tuples.
   *
   * <p>This is the conventional way to represent an empty relational
   * expression. There are several rules that recognize empty relational
   * expressions and prune away that section of the tree.
   */
  public static final Predicate<? super Values> IS_NOT_EMPTY =
      new PredicateImpl<Values>() {
        public boolean test(Values values) {
          return !values.getTuples().isEmpty();
        }
      };

  //~ Instance fields --------------------------------------------------------

  public final ImmutableList<ImmutableList<RexLiteral>> tuples;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new Values.
   *
   * <p>Note that tuples passed in become owned by
   * this rel (without a deep copy), so caller must not modify them after this
   * call, otherwise bad things will happen.
   *
   * @param cluster Cluster that this relational expression belongs to
   * @param rowType Row type for tuples produced by this rel
   * @param tuples  2-dimensional array of tuple values to be produced; outer
   *                list contains tuples; each inner list is one tuple; all
   *                tuples must be of same length, conforming to rowType
   */
  protected Values(
      RelOptCluster cluster,
      RelDataType rowType,
      ImmutableList<ImmutableList<RexLiteral>> tuples,
      RelTraitSet traits) {
    super(cluster, traits);
    this.rowType = rowType;
    this.tuples = tuples;
    assert assertRowType();
  }

  /**
   * Creates a Values by parsing serialized output.
   */
  public Values(RelInput input) {
    this(input.getCluster(), input.getRowType("type"),
        input.getTuples("tuples"), input.getTraitSet());
  }

  //~ Methods ----------------------------------------------------------------

  public ImmutableList<ImmutableList<RexLiteral>> getTuples(RelInput input) {
    return input.getTuples("tuples");
  }

  /** Returns the rows of literals represented by this Values relational
   * expression. */
  public ImmutableList<ImmutableList<RexLiteral>> getTuples() {
    return tuples;
  }

  /** Returns true if all tuples match rowType; otherwise, assert on
   * mismatch. */
  private boolean assertRowType() {
    for (List<RexLiteral> tuple : tuples) {
      assert tuple.size() == rowType.getFieldCount();
      for (Pair<RexLiteral, RelDataTypeField> pair
          : Pair.zip(tuple, rowType.getFieldList())) {
        RexLiteral literal = pair.left;
        RelDataType fieldType = pair.right.getType();

        // TODO jvs 19-Feb-2006: strengthen this a bit.  For example,
        // overflow, rounding, and padding/truncation must already have
        // been dealt with.
        if (!RexLiteral.isNullLiteral(literal)) {
          assert SqlTypeUtil.canAssignFrom(fieldType, literal.getType())
              : "to " + fieldType + " from " + literal;
        }
      }
    }
    return true;
  }

  @Override protected RelDataType deriveRowType() {
    return rowType;
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double dRows = mq.getRowCount(this);

    // Assume CPU is negligible since values are precomputed.
    double dCpu = 1;
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  // implement RelNode
  public double estimateRowCount(RelMetadataQuery mq) {
    return tuples.size();
  }

  // implement RelNode
  public RelWriter explainTerms(RelWriter pw) {
    // A little adapter just to get the tuples to come out
    // with curly brackets instead of square brackets.  Plus
    // more whitespace for readability.
    return super.explainTerms(pw)
        // For rel digest, include the row type since a rendered
        // literal may leave the type ambiguous (e.g. "null").
        .itemIf("type", rowType,
            pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
        .itemIf("type", rowType.getFieldList(), pw.nest())
        .itemIf("tuples", Lists.transform(tuples, F), !pw.nest())
        .itemIf("tuples", tuples, pw.nest());
  }
}

// End Values.java
