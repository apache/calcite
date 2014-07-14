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
package org.eigenbase.sql;

import java.util.List;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.type.*;

/**
 * Placeholder for an unresolved function.
 *
 * <p>Created by the parser, then it is rewritten to proper SqlFunction by
 * the validator to a function defined in an Optiq schema.</p>
 */
public class SqlUnresolvedFunction extends SqlFunction {
  /**
   * Creates a placeholder SqlUnresolvedFunction for an invocation of a function
   * with a possibly qualified name. This name must be resolved into either
   * a builtin function or a user-defined function.
   *
   * @param sqlIdentifier        possibly qualified identifier for function
   * @param returnTypeInference  strategy to use for return type inference
   * @param operandTypeInference strategy to use for parameter type inference
   * @param operandTypeChecker   strategy to use for parameter type checking
   * @param paramTypes           array of parameter types
   * @param funcType             function category
   */
  public SqlUnresolvedFunction(
      SqlIdentifier sqlIdentifier,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      List<RelDataType> paramTypes,
      SqlFunctionCategory funcType) {
    super(sqlIdentifier, returnTypeInference, operandTypeInference,
        operandTypeChecker, paramTypes, funcType);
  }

  /**
   * The operator class for this function isn't resolved to the
   * correct class. This happens in the case of user defined
   * functions. Return the return type to be 'ANY', so we don't
   * fail.
   */
  @Override
  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    return new BasicSqlType(SqlTypeName.ANY);
  }

}

// End SqlUnresolvedFunction.java
