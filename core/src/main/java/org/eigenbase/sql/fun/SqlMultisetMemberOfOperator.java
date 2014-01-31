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

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;

/**
 * Multiset MEMBER OF. Checks to see if a element belongs to a multiset.<br>
 * Example:<br>
 * <code>'green' MEMBER OF MULTISET['red','almost green','blue']</code> returns
 * <code>false</code>.
 */
public class SqlMultisetMemberOfOperator extends SqlBinaryOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlMultisetMemberOfOperator() {
    // TODO check if precedence is correct
    super(
        "MEMBER OF",
        SqlKind.OTHER,
        30,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        null,
        null);
  }

  //~ Methods ----------------------------------------------------------------

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (!OperandTypes.MULTISET.checkSingleOperandType(
        callBinding,
        callBinding.getCall().operands[1],
        0,
        throwOnFailure)) {
      return false;
    }

    MultisetSqlType mt =
        (MultisetSqlType) callBinding.getValidator().deriveType(
            callBinding.getScope(),
            callBinding.getCall().operands[1]);

    RelDataType t0 =
        callBinding.getValidator().deriveType(
            callBinding.getScope(),
            callBinding.getCall().operands[0]);
    RelDataType t1 = mt.getComponentType();

    if (t0.getFamily() != t1.getFamily()) {
      if (throwOnFailure) {
        throw callBinding.newValidationError(
            EigenbaseResource.instance().TypeNotComparableNear.ex(
                t0.toString(),
                t1.toString()));
      }
      return false;
    }
    return true;
  }

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(2);
  }
}

// End SqlMultisetMemberOfOperator.java
