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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.Pair;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.Functions;

/**
 * <code>ValuesRelBase</code> is an abstract base class for implementations of
 * {@link ValuesRel}.
 */
public abstract class ValuesRelBase extends AbstractRelNode {
  /**
   * Lambda that helps render tuples as strings.
   */
  private static final Function1<List<RexLiteral>, Object> F =
      new Function1<List<RexLiteral>, Object>() {
        public Object apply(List<RexLiteral> tuple) {
          String s = tuple.toString();
          assert s.startsWith("[");
          assert s.endsWith("]");
          return "{ " + s.substring(1, s.length() - 1) + " }";
        }
      };

  //~ Instance fields --------------------------------------------------------

  protected final List<List<RexLiteral>> tuples;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new ValuesRelBase. Note that tuples passed in become owned by
   * this rel (without a deep copy), so caller must not modify them after this
   * call, otherwise bad things will happen.
   *
   * @param cluster .
   * @param rowType row type for tuples produced by this rel
   * @param tuples  2-dimensional array of tuple values to be produced; outer
   *                list contains tuples; each inner list is one tuple; all
   *                tuples must be of same length, conforming to rowType
   */
  protected ValuesRelBase(
      RelOptCluster cluster,
      RelDataType rowType,
      List<List<RexLiteral>> tuples,
      RelTraitSet traits) {
    super(cluster, traits);
    this.rowType = rowType;
    this.tuples = tuples;
    assert assertRowType();
  }

  /**
   * Creates a ValuesRelBase by parsing serialized output.
   */
  public ValuesRelBase(RelInput input) {
    this(
        input.getCluster(), input.getRowType("type"),
        input.getTuples("tuples"), input.getTraitSet());
  }

  //~ Methods ----------------------------------------------------------------

  public List<List<RexLiteral>> getTuples(RelInput input) {
    return input.getTuples("tuples");
  }

  /**
   * @return rows of literals represented by this rel
   */
  public List<List<RexLiteral>> getTuples() {
    return tuples;
  }

  /**
   * @return true if all tuples match rowType; otherwise, assert on mismatch
   */
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

  // implement RelNode
  protected RelDataType deriveRowType() {
    return rowType;
  }

  // implement RelNode
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    double dRows = RelMetadataQuery.getRowCount(this);

    // Assume CPU is negligible since values are precomputed.
    double dCpu = 1;
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  // implement RelNode
  public double getRows() {
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
        .itemIf(
            "type", rowType,
            pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
        .itemIf(
            "type", rowType.getFieldList(),
            pw.nest())
        .itemIf("tuples", Functions.adapt(tuples, F), !pw.nest())
        .itemIf("tuples", tuples, pw.nest());
  }
}

// End ValuesRelBase.java
