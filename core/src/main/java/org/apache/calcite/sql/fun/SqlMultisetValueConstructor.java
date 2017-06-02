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
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Definition of the SQL:2003 standard MULTISET constructor, <code>MULTISET
 * [&lt;expr&gt;, ...]</code>.
 *
 * <p>Derived classes construct other kinds of collections.</p>
 *
 * @see SqlMultisetQueryConstructor
 */
public class SqlMultisetValueConstructor extends SqlSpecialOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlMultisetValueConstructor() {
    this("MULTISET", SqlKind.MULTISET_VALUE_CONSTRUCTOR);
  }

  protected SqlMultisetValueConstructor(String name, SqlKind kind) {
    super(
        name,
        kind, MDX_PRECEDENCE,
        false,
        ReturnTypes.ARG0,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC);
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    RelDataType type =
        getComponentType(
            opBinding.getTypeFactory(),
            opBinding.collectOperandTypes());
    if (null == type) {
      return null;
    }
    return SqlTypeUtil.createMultisetType(
        opBinding.getTypeFactory(),
        type,
        false);
  }

  protected RelDataType getComponentType(
      RelDataTypeFactory typeFactory,
      List<RelDataType> argTypes) {
    return typeFactory.leastRestrictive(argTypes);
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final List<RelDataType> argTypes =
        SqlTypeUtil.deriveAndCollectTypes(
            callBinding.getValidator(),
            callBinding.getScope(),
            callBinding.operands());
    if (argTypes.size() == 0) {
      throw callBinding.newValidationError(RESOURCE.requireAtLeastOneArg());
    }
    final RelDataType componentType =
        getComponentType(
            callBinding.getTypeFactory(),
            argTypes);
    if (null == componentType) {
      if (throwOnFailure) {
        throw callBinding.newValidationError(RESOURCE.needSameTypeParameter());
      }
      return false;
    }
    return true;
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    writer.keyword(getName()); // "MULTISET" or "ARRAY"
    final SqlWriter.Frame frame = writer.startList("[", "]");
    for (SqlNode operand : call.getOperandList()) {
      writer.sep(",");
      operand.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
  }
}

// End SqlMultisetValueConstructor.java
