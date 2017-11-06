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

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidatorException;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * <code>RexCallBinding</code> implements {@link SqlOperatorBinding} by
 * referring to an underlying collection of {@link RexNode} operands.
 */
public class RexCallBinding extends SqlOperatorBinding {
  //~ Instance fields --------------------------------------------------------

  private final List<RexNode> operands;

  private final List<RelCollation> inputCollations;

  //~ Constructors -----------------------------------------------------------

  public RexCallBinding(
      RelDataTypeFactory typeFactory,
      SqlOperator sqlOperator,
      List<? extends RexNode> operands,
      List<RelCollation> inputCollations) {
    super(typeFactory, sqlOperator);
    this.operands = ImmutableList.copyOf(operands);
    this.inputCollations = ImmutableList.copyOf(inputCollations);
  }

  /** Creates a binding of the appropriate type. */
  public static RexCallBinding create(RelDataTypeFactory typeFactory,
      RexCall call,
      List<RelCollation> inputCollations) {
    switch (call.getKind()) {
    case CAST:
      return new RexCastCallBinding(typeFactory, call.getOperator(),
          call.getOperands(), call.getType(), inputCollations);
    }
    return new RexCallBinding(typeFactory, call.getOperator(),
        call.getOperands(), inputCollations);
  }

  //~ Methods ----------------------------------------------------------------

  @SuppressWarnings("deprecation")
  @Override public String getStringLiteralOperand(int ordinal) {
    return RexLiteral.stringValue(operands.get(ordinal));
  }

  @SuppressWarnings("deprecation")
  @Override public int getIntLiteralOperand(int ordinal) {
    return RexLiteral.intValue(operands.get(ordinal));
  }

  @Override public <T> T getOperandLiteralValue(int ordinal, Class<T> clazz) {
    final RexNode node = operands.get(ordinal);
    if (node instanceof RexLiteral) {
      return ((RexLiteral) node).getValueAs(clazz);
    }
    return clazz.cast(RexLiteral.value(node));
  }

  @Override public SqlMonotonicity getOperandMonotonicity(int ordinal) {
    RexNode operand = operands.get(ordinal);

    if (operand instanceof RexInputRef) {
      for (RelCollation ic : inputCollations) {
        if (ic.getFieldCollations().isEmpty()) {
          continue;
        }

        for (RelFieldCollation rfc : ic.getFieldCollations()) {
          if (rfc.getFieldIndex() == ((RexInputRef) operand).getIndex()) {
            return rfc.direction.monotonicity();
            // TODO: Is it possible to have more than one RelFieldCollation for a RexInputRef?
          }
        }
      }
    } else if (operand instanceof RexCall) {
      final RexCallBinding binding =
          RexCallBinding.create(typeFactory, (RexCall) operand, inputCollations);
      return ((RexCall) operand).getOperator().getMonotonicity(binding);
    }

    return SqlMonotonicity.NOT_MONOTONIC;
  }

  @Override public boolean isOperandNull(int ordinal, boolean allowCast) {
    return RexUtil.isNullLiteral(operands.get(ordinal), allowCast);
  }

  @Override public boolean isOperandLiteral(int ordinal, boolean allowCast) {
    return RexUtil.isLiteral(operands.get(ordinal), allowCast);
  }

  // implement SqlOperatorBinding
  public int getOperandCount() {
    return operands.size();
  }

  // implement SqlOperatorBinding
  public RelDataType getOperandType(int ordinal) {
    return operands.get(ordinal).getType();
  }

  public CalciteException newError(
      Resources.ExInst<SqlValidatorException> e) {
    return SqlUtil.newContextException(SqlParserPos.ZERO, e);
  }

  /** To be compatible with {@code SqlCall}, CAST needs to pretend that it
   * has two arguments, the second of which is the target type. */
  private static class RexCastCallBinding extends RexCallBinding {
    private final RelDataType type;

    RexCastCallBinding(
        RelDataTypeFactory typeFactory,
        SqlOperator sqlOperator, List<? extends RexNode> operands,
        RelDataType type,
        List<RelCollation> inputCollations) {
      super(typeFactory, sqlOperator, operands, inputCollations);
      this.type = type;
    }

    @Override public RelDataType getOperandType(int ordinal) {
      if (ordinal == 1) {
        return type;
      }
      return super.getOperandType(ordinal);
    }
  }
}

// End RexCallBinding.java
