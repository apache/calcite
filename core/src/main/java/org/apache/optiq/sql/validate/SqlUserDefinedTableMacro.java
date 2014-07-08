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
package org.apache.optiq.sql.validate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.optiq.reltype.RelDataType;
import org.apache.optiq.reltype.RelDataTypeFactory;
import org.apache.optiq.reltype.RelDataTypeFactoryImpl;
import org.apache.optiq.sql.*;
import org.apache.optiq.sql.type.SqlOperandTypeChecker;
import org.apache.optiq.sql.type.SqlOperandTypeInference;
import org.apache.optiq.sql.type.SqlReturnTypeInference;
import org.apache.optiq.util.NlsString;
import org.apache.optiq.util.Pair;
import org.apache.optiq.util.Util;

import org.apache.linq4j.expressions.*;

import org.apache.optiq.Function;
import org.apache.optiq.FunctionParameter;
import org.apache.optiq.TableMacro;
import org.apache.optiq.TranslatableTable;
import org.apache.optiq.impl.enumerable.RexToLixTranslator;

/**
 * User-defined table macro.
 * <p>
 * Created by the validator, after resolving a function call to a function
 * defined in an Optiq schema.
*/
public class SqlUserDefinedTableMacro extends SqlFunction {
  private final TableMacro tableMacro;

  public SqlUserDefinedTableMacro(SqlIdentifier opName,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      TableMacro tableMacro) {
    super(Util.last(opName.names), opName, SqlKind.OTHER_FUNCTION,
        returnTypeInference, operandTypeInference, operandTypeChecker, null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.tableMacro = tableMacro;
  }

  /** Returns the table in this UDF, or null if there is no table. */
  public TranslatableTable getTable(RelDataTypeFactory typeFactory,
      List<SqlNode> operandList) {
    List<Object> arguments = convertArguments(typeFactory, operandList,
        tableMacro, getNameAsId(), true);
    return tableMacro.apply(arguments);
  }

  /**
   * Converts arguments from {@link org.apache.optiq.sql.SqlNode} to java object
   * format.
   * @param typeFactory type factory used to convert the arguments
   * @param operandList input arguments
   * @param function target function to get parameter types from
   * @param opName name of the operator to use in error message
   * @param failOnNonLiteral true when conversion should fail on non-literal
   * @return converted list of arguments
   */
  public static List<Object> convertArguments(RelDataTypeFactory typeFactory,
      List<SqlNode> operandList, Function function,
      SqlIdentifier opName,
      boolean failOnNonLiteral) {
    List<Object> arguments = new ArrayList<Object>(operandList.size());
    // Construct a list of arguments, if they are all constants.
    for (Pair<FunctionParameter, SqlNode> pair
        : Pair.zip(function.getParameters(), operandList)) {
      if (SqlUtil.isNullLiteral(pair.right, true)) {
        arguments.add(null);
      } else if (SqlUtil.isLiteral(pair.right)) {
        final Object o = ((SqlLiteral) pair.right).getValue();
        final Object o2 = coerce(o, pair.left.getType(typeFactory));
        arguments.add(o2);
      } else {
        arguments.add(null);
        if (failOnNonLiteral) {
          throw new IllegalArgumentException(
            "All arguments of call to macro " + opName + " should be "
            + "literal. Actual argument #" + pair.left.getOrdinal() + " ("
            + pair.left.getName() + ") is not literal: " + pair.right);
        }
      }
    }
    return arguments;
  }

  private static Object coerce(Object o, RelDataType type) {
    if (!(type instanceof RelDataTypeFactoryImpl.JavaType)) {
      return null;
    }
    final RelDataTypeFactoryImpl.JavaType javaType =
        (RelDataTypeFactoryImpl.JavaType) type;
    final Class clazz = javaType.getJavaClass();
    //noinspection unchecked
    if (clazz.isAssignableFrom(o.getClass())) {
      return o;
    }
    if (clazz == String.class && o instanceof NlsString) {
      return ((NlsString) o).getValue();
    }
    // We need optimization here for constant folding.
    // Not all the expressions can be interpreted (e.g. ternary), so
    // we rely on optimization capabilities to fold non-interpretable
    // expressions.
    BlockBuilder bb = new BlockBuilder();
    final Expression expr =
        RexToLixTranslator.convert(Expressions.constant(o), clazz);
    bb.add(Expressions.return_(null, expr));
    final FunctionExpression convert =
        Expressions.lambda(bb.toBlock(),
            Collections.<ParameterExpression>emptyList());
    return convert.compile().dynamicInvoke();
  }
}

// End SqlUserDefinedTableMacro.java
