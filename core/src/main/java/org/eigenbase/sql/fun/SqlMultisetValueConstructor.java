/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql.fun;

import java.util.List;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.SqlValidatorException;

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
        kind,
        MaxPrec,
        false,
        SqlTypeStrategies.rtiFirstArgType,
        null,
        SqlTypeStrategies.otcVariadic);
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
            callBinding.getCall().operands);
    if (argTypes.size() == 0) {
      throw callBinding.newValidationError(
          new SqlValidatorException(
              "Require at least 1 argument", null));
    }
    final RelDataType componentType =
        getComponentType(
            callBinding.getTypeFactory(),
            argTypes);
    if (null == componentType) {
      if (throwOnFailure) {
        throw callBinding.newValidationError(
            EigenbaseResource.instance().NeedSameTypeParameter.ex());
      }
      return false;
    }
    return true;
  }

  public void unparse(
      SqlWriter writer,
      SqlNode[] operands,
      int leftPrec,
      int rightPrec) {
    writer.keyword(getName()); // "MULTISET" or "ARRAY"
    final SqlWriter.Frame frame = writer.startList("[", "]");
    for (SqlNode operand : operands) {
      writer.sep(",");
      operand.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
  }
}

// End SqlMultisetValueConstructor.java
