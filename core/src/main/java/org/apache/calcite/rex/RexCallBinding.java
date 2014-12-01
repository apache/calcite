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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
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

  //~ Constructors -----------------------------------------------------------

  public RexCallBinding(
      RelDataTypeFactory typeFactory,
      SqlOperator sqlOperator,
      List<? extends RexNode> operands) {
    super(typeFactory, sqlOperator);
    this.operands = ImmutableList.copyOf(operands);
  }

  //~ Methods ----------------------------------------------------------------

  // implement SqlOperatorBinding
  public String getStringLiteralOperand(int ordinal) {
    return RexLiteral.stringValue(operands.get(ordinal));
  }

  // implement SqlOperatorBinding
  public int getIntLiteralOperand(int ordinal) {
    return RexLiteral.intValue(operands.get(ordinal));
  }

  // implement SqlOperatorBinding
  public boolean isOperandNull(int ordinal, boolean allowCast) {
    return RexUtil.isNullLiteral(operands.get(ordinal), allowCast);
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
}

// End RexCallBinding.java
