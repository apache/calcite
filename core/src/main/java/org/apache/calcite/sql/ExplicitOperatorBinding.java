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
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorException;

import java.util.List;

/**
 * <code>ExplicitOperatorBinding</code> implements {@link SqlOperatorBinding}
 * via an underlying array of known operand types.
 */
public class ExplicitOperatorBinding extends SqlOperatorBinding {
  //~ Instance fields --------------------------------------------------------

  private final List<RelDataType> types;
  private final SqlOperatorBinding delegate;

  //~ Constructors -----------------------------------------------------------

  public ExplicitOperatorBinding(
      SqlOperatorBinding delegate,
      List<RelDataType> types) {
    this(
        delegate,
        delegate.getTypeFactory(),
        delegate.getOperator(),
        types);
  }

  public ExplicitOperatorBinding(
      RelDataTypeFactory typeFactory,
      SqlOperator operator,
      List<RelDataType> types) {
    this(null, typeFactory, operator, types);
  }

  private ExplicitOperatorBinding(
      SqlOperatorBinding delegate,
      RelDataTypeFactory typeFactory,
      SqlOperator operator,
      List<RelDataType> types) {
    super(typeFactory, operator);
    this.types = types;
    this.delegate = delegate;
  }

  //~ Methods ----------------------------------------------------------------

  // implement SqlOperatorBinding
  public int getOperandCount() {
    return types.size();
  }

  // implement SqlOperatorBinding
  public RelDataType getOperandType(int ordinal) {
    return types.get(ordinal);
  }

  public CalciteException newError(
      Resources.ExInst<SqlValidatorException> e) {
    if (delegate != null) {
      return delegate.newError(e);
    } else {
      return SqlUtil.newContextException(SqlParserPos.ZERO, e);
    }
  }

  public boolean isOperandNull(int ordinal, boolean allowCast) {
    // NOTE jvs 1-May-2006:  This call is only relevant
    // for SQL validation, so anywhere else, just say
    // everything's OK.
    return false;
  }
}

// End ExplicitOperatorBinding.java
