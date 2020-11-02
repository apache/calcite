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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * User-defined table macro.
 *
 * <p>Created by the validator, after resolving a function call to a function
 * defined in a Calcite schema.
*/
public class SqlUserDefinedTableMacro extends SqlFunction
    implements SqlTableFunction {
  private final TableMacro tableMacro;

  @Deprecated // to be removed before 2.0
  public SqlUserDefinedTableMacro(SqlIdentifier opName,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker, List<RelDataType> paramTypes,
      TableMacro tableMacro) {
    this(opName, SqlKind.OTHER_FUNCTION, returnTypeInference,
        operandTypeInference,
        operandTypeChecker instanceof SqlOperandMetadata
            ? (SqlOperandMetadata) operandTypeChecker : null, tableMacro);
    Util.discard(paramTypes); // no longer used
  }

  /** Creates a user-defined table macro. */
  public SqlUserDefinedTableMacro(SqlIdentifier opName, SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandMetadata operandMetadata,
      TableMacro tableMacro) {
    super(Util.last(opName.names), opName, kind,
        returnTypeInference, operandTypeInference, operandMetadata,
        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    this.tableMacro = tableMacro;
  }

  @Override public @Nullable SqlOperandMetadata getOperandTypeChecker() {
    return (@Nullable SqlOperandMetadata) super.getOperandTypeChecker();
  }

  @SuppressWarnings("deprecation")
  @Override public List<String> getParamNames() {
    return Util.transform(tableMacro.getParameters(), FunctionParameter::getName);
  }

  /** Returns the table in this UDF, or null if there is no table. */
  public TranslatableTable getTable(SqlOperatorBinding callBinding) {
    List<@Nullable Object> arguments =
        convertArguments(callBinding, tableMacro, getNameAsId(), true);
    return tableMacro.apply(arguments);
  }

  /**
   * Converts arguments from {@link org.apache.calcite.sql.SqlNode} to
   * java object format.
   *
   * @param callBinding Operator bound to arguments
   * @param function target function to get parameter types from
   * @param opName name of the operator to use in error message
   * @param failOnNonLiteral true when conversion should fail on non-literal
   * @return converted list of arguments
   */
  static List<@Nullable Object> convertArguments(SqlOperatorBinding callBinding,
      Function function, SqlIdentifier opName, boolean failOnNonLiteral) {
    RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
    List<@Nullable Object> arguments = new ArrayList<>(callBinding.getOperandCount());
    Ord.forEach(function.getParameters(), (parameter, i) -> {
      final RelDataType type = parameter.getType(typeFactory);
      final Object value;
      if (callBinding.isOperandLiteral(i, true)) {
        value = callBinding.getOperandLiteralValue(i, type);
      } else {
        if (failOnNonLiteral) {
          throw new IllegalArgumentException("All arguments of call to macro "
              + opName + " should be literal. Actual argument #"
              + parameter.getOrdinal() + " (" + parameter.getName()
              + ") is not literal");
        }
        if (type.isNullable()) {
          value = null;
        } else {
          value = 0L;
        }
      }
      arguments.add(value);
    });
    return arguments;
  }

  @Override public SqlReturnTypeInference getRowTypeInference() {
    return this::inferRowType;
  }

  private RelDataType inferRowType(SqlOperatorBinding callBinding) {
    final RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
    final TranslatableTable table = getTable(callBinding);
    return table.getRowType(typeFactory);
  }
}
