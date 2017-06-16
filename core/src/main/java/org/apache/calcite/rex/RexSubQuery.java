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
package org.apache.calcite.rex;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Scalar expression that represents an IN, EXISTS or scalar sub-query.
 */
public class RexSubQuery extends RexCall {
  public final RelNode rel;

  private RexSubQuery(RelDataType type, SqlOperator op,
      ImmutableList<RexNode> operands, RelNode rel) {
    super(type, op, operands);
    this.rel = rel;
    this.digest = computeDigest(false);
  }

  /** Creates an IN sub-query. */
  public static RexSubQuery in(RelNode rel, ImmutableList<RexNode> nodes) {
    final RelDataType type = type(rel, nodes);
    return new RexSubQuery(type, SqlStdOperatorTable.IN, nodes, rel);
  }

  /** Creates a SOME sub-query.
   *
   * <p>There is no ALL. For {@code x comparison ALL (sub-query)} use instead
   * {@code NOT (x inverse-comparison SOME (sub-query))}.
   * If {@code comparison} is {@code >}
   * then {@code negated-comparison} is {@code <=}, and so forth. */
  public static RexSubQuery some(RelNode rel, ImmutableList<RexNode> nodes,
      SqlQuantifyOperator op) {
    assert op.kind == SqlKind.SOME;
    final RelDataType type = type(rel, nodes);
    return new RexSubQuery(type, op, nodes, rel);
  }

  static RelDataType type(RelNode rel, ImmutableList<RexNode> nodes) {
    assert rel.getRowType().getFieldCount() == nodes.size();
    final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
    boolean nullable = false;
    for (RexNode node : nodes) {
      if (node.getType().isNullable()) {
        nullable = true;
      }
    }
    for (RelDataTypeField field : rel.getRowType().getFieldList()) {
      if (field.getType().isNullable()) {
        nullable = true;
      }
    }
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.BOOLEAN), nullable);
  }

  /** Creates an EXISTS sub-query. */
  public static RexSubQuery exists(RelNode rel) {
    final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
    final RelDataType type = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    return new RexSubQuery(type, SqlStdOperatorTable.EXISTS,
        ImmutableList.<RexNode>of(), rel);
  }

  /** Creates a scalar sub-query. */
  public static RexSubQuery scalar(RelNode rel) {
    final List<RelDataTypeField> fieldList = rel.getRowType().getFieldList();
    assert fieldList.size() == 1;
    final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
    final RelDataType type =
        typeFactory.createTypeWithNullability(fieldList.get(0).getType(), true);
    return new RexSubQuery(type, SqlStdOperatorTable.SCALAR_QUERY,
        ImmutableList.<RexNode>of(), rel);
  }

  public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitSubQuery(this);
  }

  public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitSubQuery(this, arg);
  }

  @Override protected String computeDigest(boolean withType) {
    StringBuilder sb = new StringBuilder(op.getName());
    sb.append("(");
    for (RexNode operand : operands) {
      sb.append(operand.toString());
      sb.append(", ");
    }
    sb.append("{\n");
    sb.append(RelOptUtil.toString(rel));
    sb.append("})");
    return sb.toString();
  }

  @Override public RexSubQuery clone(RelDataType type, List<RexNode> operands) {
    return new RexSubQuery(type, getOperator(),
        ImmutableList.copyOf(operands), rel);
  }

  public RexSubQuery clone(RelNode rel) {
    return new RexSubQuery(type, getOperator(), operands, rel);
  }
}

// End RexSubQuery.java
