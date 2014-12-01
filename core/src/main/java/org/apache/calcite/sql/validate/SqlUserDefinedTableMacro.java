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

import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * User-defined table macro.
 *
 * <p>Created by the validator, after resolving a function call to a function
 * defined in a Calcite schema.
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
   * Converts arguments from {@link org.apache.calcite.sql.SqlNode} to
   * java object format.
   *
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
          throw new IllegalArgumentException("All arguments of call to macro "
              + opName + " should be literal. Actual argument #"
              + pair.left.getOrdinal() + " (" + pair.left.getName()
              + ") is not literal: " + pair.right);
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
