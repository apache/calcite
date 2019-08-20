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

package org.apache.calcite.piglet;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import org.apache.pig.Accumulator;
import org.apache.pig.FuncSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * This class maps a Pig UDF to a corresponding SQL built-in function/operator.
 * If such mapping is not available, it creates a wrapper to allow SQL engines
 * call Pig UDFs directly.
 *
 */
class PigRelUDFConverter {
  private PigRelUDFConverter() {}

  private static final Map<String, SqlOperator> BUILTIN_FUNC = new HashMap<>();
  static {
    BUILTIN_FUNC.put("org.apache.pig.builtin.ABS", SqlStdOperatorTable.ABS);
    BUILTIN_FUNC.put("org.apache.pig.builtin.BigDecimalAbs", SqlStdOperatorTable.ABS);
    BUILTIN_FUNC.put("org.apache.pig.builtin.BigIntegerAbs", SqlStdOperatorTable.ABS);
    BUILTIN_FUNC.put("org.apache.pig.builtin.DoubleAbs", SqlStdOperatorTable.ABS);
    BUILTIN_FUNC.put("org.apache.pig.builtin.FloatAbs", SqlStdOperatorTable.ABS);
    BUILTIN_FUNC.put("org.apache.pig.builtin.IntAbs", SqlStdOperatorTable.ABS);
    BUILTIN_FUNC.put("org.apache.pig.builtin.LongAbs", SqlStdOperatorTable.ABS);
    BUILTIN_FUNC.put("org.apache.pig.builtin.CEIL", SqlStdOperatorTable.CEIL);
    BUILTIN_FUNC.put("org.apache.pig.builtin.CONCAT", SqlStdOperatorTable.CONCAT);
    BUILTIN_FUNC.put("org.apache.pig.builtin.StringConcat", SqlStdOperatorTable.CONCAT);
    BUILTIN_FUNC.put("org.apache.pig.builtin.EXP", SqlStdOperatorTable.EXP);
    BUILTIN_FUNC.put("org.apache.pig.builtin.FLOOR", SqlStdOperatorTable.FLOOR);
    BUILTIN_FUNC.put("org.apache.pig.builtin.LOG", SqlStdOperatorTable.LN);
    BUILTIN_FUNC.put("org.apache.pig.builtin.LOG10", SqlStdOperatorTable.LOG10);
    BUILTIN_FUNC.put("org.apache.pig.builtin.LOWER", SqlStdOperatorTable.LOWER);
    BUILTIN_FUNC.put("org.apache.pig.builtin.RANDOM", SqlStdOperatorTable.RAND);
    BUILTIN_FUNC.put("org.apache.pig.builtin.SQRT", SqlStdOperatorTable.SQRT);
    BUILTIN_FUNC.put("org.apache.pig.builtin.StringSize", SqlStdOperatorTable.CHAR_LENGTH);
    BUILTIN_FUNC.put("org.apache.pig.builtin.SUBSTRING", SqlStdOperatorTable.SUBSTRING);
    BUILTIN_FUNC.put("org.apache.pig.builtin.TOTUPLE", SqlStdOperatorTable.ROW);
    BUILTIN_FUNC.put("org.apache.pig.builtin.UPPER", SqlStdOperatorTable.UPPER);
  }

  private static final Map<String, SqlAggFunction> BUILTIN_AGG_FUNC = new HashMap<>();
  static {
    // AVG()
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.AVG", SqlStdOperatorTable.AVG);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.BigDecimalAvg", SqlStdOperatorTable.AVG);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.BigIntegerAvg", SqlStdOperatorTable.AVG);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.DoubleAvg", SqlStdOperatorTable.AVG);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.FloatAvg", SqlStdOperatorTable.AVG);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.IntAvg", SqlStdOperatorTable.AVG);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.LongAvg", SqlStdOperatorTable.AVG);
    // COUNT()
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.COUNT", SqlStdOperatorTable.COUNT);
    // MAX()
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.MAX", SqlStdOperatorTable.MAX);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.BigDecimalMax", SqlStdOperatorTable.MAX);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.BigIntegerMax", SqlStdOperatorTable.MAX);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.DateTimeMax", SqlStdOperatorTable.MAX);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.DoubleMax", SqlStdOperatorTable.MAX);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.FloatMax", SqlStdOperatorTable.MAX);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.IntMax", SqlStdOperatorTable.MAX);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.LongMax", SqlStdOperatorTable.MAX);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.StringMax", SqlStdOperatorTable.MAX);
    // MIN()
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.MIN", SqlStdOperatorTable.MIN);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.BigDecimalMin", SqlStdOperatorTable.MIN);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.BigIntegerMin", SqlStdOperatorTable.MIN);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.DateTimeMin", SqlStdOperatorTable.MIN);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.DoubleMin", SqlStdOperatorTable.MIN);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.FloatMin", SqlStdOperatorTable.MIN);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.IntMin", SqlStdOperatorTable.MIN);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.LongMin", SqlStdOperatorTable.MIN);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.StringMin", SqlStdOperatorTable.MIN);
    // SUM()
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.BigDecimalSum", SqlStdOperatorTable.SUM);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.BigIntegerSum", SqlStdOperatorTable.SUM);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.DoubleSum", SqlStdOperatorTable.SUM);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.FloatSum", SqlStdOperatorTable.SUM);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.IntSum", SqlStdOperatorTable.SUM);
    BUILTIN_AGG_FUNC.put("org.apache.pig.builtin.LongSum", SqlStdOperatorTable.SUM);
  }

  /**
   * Converts a Pig UDF, given its {@link FuncSpec} and a list of relational
   * operands (function arguments). To call this function, the arguments of
   * Pig functions need to be converted into the relational types before.
   *
   * @param builder The relational builder
   * @param pigFunc Pig function description
   * @param operands Relational operands for the function
   * @param returnType Function return data type
   * @return The SQL calls equivalent to the Pig function
   */
  static RexNode convertPigFunction(PigRelBuilder builder, FuncSpec pigFunc,
      ImmutableList<RexNode> operands, RelDataType returnType) throws FrontendException {
    // First, check the map for the direct mapping SQL builtin
    final SqlOperator operator = BUILTIN_FUNC.get(pigFunc.getClassName());
    if (operator != null) {
      return builder.call(operator, operands);
    }

    // If no mapping found, build the argument wrapper to convert the relation operands
    // into a Pig tuple so that the Pig function can consume it.
    try {
      // Find the implementation method for the Pig function from
      // the class defining the UDF.
      final Class clazz = Class.forName(pigFunc.getClassName());
      final Method method = findMethod(clazz);
      if (method == null) {
        throw new FrontendException("Cannot find 'exec' method for Pig UDF: "
            + pigFunc.getClassName());
      }

      // Now create the argument wrapper. Depend on the type of the UDF, the
      // relational operands are converted into a Pig Tuple or Pig DataBag
      // with the appropriate wrapper.
      final SqlUserDefinedFunction convertOp =
          Accumulator.class.isAssignableFrom(clazz)
              ? PigRelSqlUDFs.createPigBagUDF(operands)
              : PigRelSqlUDFs.createPigTupleUDF(operands);
      final RexNode rexTuple = builder.call(convertOp, operands);

      // Then convert the Pig function into a @SqlUserDefinedFunction.
      SqlUserDefinedFunction userFuncOp = PigRelSqlUDFs.createGeneralPigUDF(clazz.getSimpleName(),
          method, pigFunc, rexTuple.getType(), returnType);

      // Ready to return SqlCall after having SqlUDF and operand
      return builder.call(userFuncOp, ImmutableList.of(rexTuple));
    } catch (ClassNotFoundException e) {
      throw new FrontendException("Cannot find the implementation for Pig UDF class: "
          + pigFunc.getClassName());
    }
  }

  /**
   * Finds the implementation method for a Pig UDF.
   *
   * @param clazz Class defining the UDF
   * @return Java method implementing the Pig UDF
   */
  private static Method findMethod(Class<?> clazz) {
    // @PigUDFWrapper is a temporary solution for handling checked exceptions
    // thrown from the function. See @PigUDFWrapper for details.
    Method returnedMethod = PigUDFWrapper.getWrappedMethod(clazz.getSimpleName());
    if (returnedMethod != null) {
      return returnedMethod;
    }

    // If calcite enumerable engine can correctly generate code to handle checked
    // exceptions, we just look for the function implementation from the UDF
    // class directly
    // First find method declared directly in the class
    returnedMethod = findExecMethod(clazz.getDeclaredMethods());
    if (returnedMethod == null) {
      // Then find all methods, including inherited ones.
      return findExecMethod(clazz.getMethods());
    }
    return returnedMethod;
  }

  /**
   * Finds "exec" method from a given array of methods.
   */
  private static Method findExecMethod(Method[] methods) {
    if (methods == null) {
      return null;
    }

    Method returnedMethod = null;
    for (Method method : methods) {
      if (method.getName().equals("exec")) {
        // There may be two methods named "exec", one of them just returns a
        // Java object. We will need to look for the other one if existing.
        if (method.getReturnType() != Object.class) {
          return method;
        } else {
          returnedMethod = method;
        }
      }
    }
    return returnedMethod;
  }

  /**
   * Gets the {@link SqlAggFunction} for the corresponding Pig aggregate UDF call.
   * Returns null for invalid rex call.
   * @param call Pig aggregate UDF call
   */
  static SqlAggFunction getSqlAggFuncForPigUDF(RexCall call) {
    if (!(call.getOperator() instanceof PigUserDefinedFunction)) {
      return null;
    }

    final PigUserDefinedFunction pigUDFCall = (PigUserDefinedFunction) call.getOperator();
    if (pigUDFCall.funcSpec != null) {
      final String pigUDFClass = pigUDFCall.funcSpec.getClassName();
      final SqlAggFunction sqlAggFunction = BUILTIN_AGG_FUNC.get(pigUDFClass);
      if (sqlAggFunction == null) {
        final Class udfClass =
            ((ScalarFunctionImpl) pigUDFCall.getFunction()).method.getDeclaringClass();
        if (Accumulator.class.isAssignableFrom(udfClass)) {
          throw new UnsupportedOperationException(
              "Cannot find corresponding SqlAgg func for Pig aggegate " + pigUDFClass);
        }
      }
      return sqlAggFunction;
    }
    return null;
  }
}

// End PigRelUDFConverter.java
