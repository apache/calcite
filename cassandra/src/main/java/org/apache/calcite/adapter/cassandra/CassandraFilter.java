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
package org.apache.calcite.adapter.cassandra;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter}
 * relational expression in Cassandra.
 */
public class CassandraFilter extends Filter implements CassandraRel {
  private final List<String> partitionKeys;
  private Boolean singlePartition;
  private final List<String> clusteringKeys;
  private List<RelFieldCollation> implicitFieldCollations;
  private RelCollation implicitCollation;
  private String match;

  public CassandraFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition,
      List<String> partitionKeys,
      List<String> clusteringKeys,
      List<RelFieldCollation> implicitFieldCollations) {
    super(cluster, traitSet, child, condition);

    this.partitionKeys = partitionKeys;
    this.singlePartition = false;
    this.clusteringKeys = new ArrayList<>(clusteringKeys);
    this.implicitFieldCollations = implicitFieldCollations;

    Translator translator =
        new Translator(getRowType(), partitionKeys, clusteringKeys,
            implicitFieldCollations);
    this.match = translator.translateMatch(condition);
    this.singlePartition = translator.isSinglePartition();
    this.implicitCollation = translator.getImplicitCollation();

    assert getConvention() == CassandraRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  public CassandraFilter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    return new CassandraFilter(getCluster(), traitSet, input, condition,
        partitionKeys, clusteringKeys, implicitFieldCollations);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    implementor.add(null, Collections.singletonList(match));
  }

  /** Check if the filter restricts to a single partition.
   *
   * @return True if the filter will restrict the underlying to a single partition
   */
  public boolean isSinglePartition() {
    return singlePartition;
  }

  /** Get the resulting collation by the clustering keys after filtering.
   *
   * @return The implicit collation based on the natural sorting by clustering keys
   */
  public RelCollation getImplicitCollation() {
    return implicitCollation;
  }

  /** Translates {@link RexNode} expressions into Cassandra expression strings. */
  static class Translator {
    private final RelDataType rowType;
    private final List<String> fieldNames;
    private final Set<String> partitionKeys;
    private final List<String> clusteringKeys;
    private int restrictedClusteringKeys;
    private final List<RelFieldCollation> implicitFieldCollations;

    Translator(RelDataType rowType, List<String> partitionKeys, List<String> clusteringKeys,
        List<RelFieldCollation> implicitFieldCollations) {
      this.rowType = rowType;
      this.fieldNames = CassandraRules.cassandraFieldNames(rowType);
      this.partitionKeys = new HashSet<>(partitionKeys);
      this.clusteringKeys = clusteringKeys;
      this.restrictedClusteringKeys = 0;
      this.implicitFieldCollations = implicitFieldCollations;
    }

    /** Check if the query spans only one partition.
     *
     * @return True if the matches translated so far have resulted in a single partition
     */
    public boolean isSinglePartition() {
      return partitionKeys.isEmpty();
    }

    /** Infer the implicit correlation from the unrestricted clustering keys.
     *
     * @return The collation of the filtered results
     */
    public RelCollation getImplicitCollation() {
      // No collation applies if we aren't restricted to a single partition
      if (!isSinglePartition()) {
        return RelCollations.EMPTY;
      }

      // Pull out the correct fields along with their original collations
      List<RelFieldCollation> fieldCollations = new ArrayList<>();
      for (int i = restrictedClusteringKeys; i < clusteringKeys.size(); i++) {
        int fieldIndex = fieldNames.indexOf(clusteringKeys.get(i));
        RelFieldCollation.Direction direction = implicitFieldCollations.get(i).getDirection();
        fieldCollations.add(new RelFieldCollation(fieldIndex, direction));
      }

      return RelCollations.of(fieldCollations);
    }

    /** Produce the CQL predicate string for the given condition.
     *
     * @param condition Condition to translate
     * @return CQL predicate string
     */
    private String translateMatch(RexNode condition) {
      // CQL does not support disjunctions
      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
      if (disjunctions.size() == 1) {
        return translateAnd(disjunctions.get(0));
      } else {
        throw new AssertionError("cannot translate " + condition);
      }
    }

    /** Convert the value of a literal to a string.
     *
     * @param literal Literal to translate
     * @return String representation of the literal
     */
    private static String literalValue(RexLiteral literal) {
      Object value = literal.getValue2();
      StringBuilder buf = new StringBuilder();
      buf.append(value);
      return buf.toString();
    }

    /** Translate a conjunctive predicate to a CQL string.
     *
     * @param condition A conjunctive predicate
     * @return CQL string for the predicate
     */
    private String translateAnd(RexNode condition) {
      List<String> predicates = new ArrayList<>();
      for (RexNode node : RelOptUtil.conjunctions(condition)) {
        predicates.add(translateMatch2(node));
      }

      return Util.toString(predicates, "", " AND ", "");
    }

    /** Translate a binary relation. */
    private String translateMatch2(RexNode node) {
      // We currently only use equality, but inequalities on clustering keys
      // should be possible in the future
      switch (node.getKind()) {
      case EQUALS:
        return translateBinary("=", "=", (RexCall) node);
      case LESS_THAN:
        return translateBinary("<", ">", (RexCall) node);
      case LESS_THAN_OR_EQUAL:
        return translateBinary("<=", ">=", (RexCall) node);
      case GREATER_THAN:
        return translateBinary(">", "<", (RexCall) node);
      case GREATER_THAN_OR_EQUAL:
        return translateBinary(">=", "<=", (RexCall) node);
      default:
        throw new AssertionError("cannot translate " + node);
      }
    }

    /** Translates a call to a binary operator, reversing arguments if
     * necessary. */
    private String translateBinary(String op, String rop, RexCall call) {
      final RexNode left = call.operands.get(0);
      final RexNode right = call.operands.get(1);
      String expression = translateBinary2(op, left, right);
      if (expression != null) {
        return expression;
      }
      expression = translateBinary2(rop, right, left);
      if (expression != null) {
        return expression;
      }
      throw new AssertionError("cannot translate op " + op + " call " + call);
    }

    /** Translates a call to a binary operator. Returns null on failure. */
    private String translateBinary2(String op, RexNode left, RexNode right) {
      switch (right.getKind()) {
      case LITERAL:
        break;
      default:
        return null;
      }
      final RexLiteral rightLiteral = (RexLiteral) right;
      switch (left.getKind()) {
      case INPUT_REF:
        final RexInputRef left1 = (RexInputRef) left;
        String name = fieldNames.get(left1.getIndex());
        return translateOp2(op, name, rightLiteral);
      case CAST:
        // FIXME This will not work in all cases (for example, we ignore string encoding)
        return translateBinary2(op, ((RexCall) left).operands.get(0), right);
      default:
        return null;
      }
    }

    /** Combines a field name, operator, and literal to produce a predicate string. */
    private String translateOp2(String op, String name, RexLiteral right) {
      // In case this is a key, record that it is now restricted
      if (op.equals("=")) {
        partitionKeys.remove(name);
        if (clusteringKeys.contains(name)) {
          restrictedClusteringKeys++;
        }
      }

      Object value = literalValue(right);
      String valueString = value.toString();
      if (value instanceof String) {
        SqlTypeName typeName = rowType.getField(name, true, false).getType().getSqlTypeName();
        if (typeName != SqlTypeName.CHAR) {
          valueString = "'" + valueString + "'";
        }
      }
      return name + " " + op + " " + valueString;
    }
  }
}

// End CassandraFilter.java
