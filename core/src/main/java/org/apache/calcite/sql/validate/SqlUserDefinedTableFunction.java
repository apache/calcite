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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.lang.reflect.Type;
import java.util.List;

/**
 * User-defined table function.
 *
 * <p>Created by the validator, after resolving a function call to a function
 * defined in a Calcite schema.
*/
public class SqlUserDefinedTableFunction extends SqlUserDefinedFunction
    implements SqlTableFunction {
  public SqlUserDefinedTableFunction(SqlIdentifier opName,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      List<RelDataType> paramTypes,
      TableFunction function) {
    super(opName, returnTypeInference, operandTypeInference, operandTypeChecker,
        paramTypes, function, SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
  }

  /**
   * Returns function that implements given operator call.
   * @return function that implements given operator call
   */
  public TableFunction getFunction() {
    return (TableFunction) super.getFunction();
  }

  @Override public SqlReturnTypeInference getRowTypeInference() {
    return this::inferRowType;
  }

  private RelDataType inferRowType(SqlOperatorBinding callBinding) {
    List<Object> arguments =
        SqlUserDefinedTableMacro.convertArguments(callBinding, function,
            getNameAsId(), false);
    return getFunction().getRowType(callBinding.getTypeFactory(), arguments);
  }

  /**
   * Returns the row type of the table yielded by this function when
   * applied to given arguments. Only literal arguments are passed,
   * non-literal are replaced with default values (null, 0, false, etc).
   *
   * @param callBinding Operand bound to arguments
   * @return element type of the table (e.g. {@code Object[].class})
   */
  public Type getElementType(SqlOperatorBinding callBinding) {
    List<Object> arguments =
        SqlUserDefinedTableMacro.convertArguments(callBinding, function,
            getNameAsId(), false);
    return getFunction().getElementType(arguments);
  }
}
