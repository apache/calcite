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
import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * This class maps a Pig UDF to a corresponding SQL built-in function/operator.
 * If such mapping is not available, it creates a wrapper to allow SQL engines
 * call Pig UDFs directly.
 *
 */
class PigRelUdfConverter {

  private PigRelUdfConverter() {}

  private static final PigUdfFinder PIG_UDF_FINDER = new PigUdfFinder();

  private static final Map<String, SqlOperator> BUILTIN_FUNC =
      ImmutableMap.<String, SqlOperator>builder()
          .put("org.apache.pig.builtin.ABS", SqlStdOperatorTable.ABS)
          .put("org.apache.pig.builtin.BigDecimalAbs", SqlStdOperatorTable.ABS)
          .put("org.apache.pig.builtin.BigIntegerAbs", SqlStdOperatorTable.ABS)
          .put("org.apache.pig.builtin.DoubleAbs", SqlStdOperatorTable.ABS)
          .put("org.apache.pig.builtin.FloatAbs", SqlStdOperatorTable.ABS)
          .put("org.apache.pig.builtin.IntAbs", SqlStdOperatorTable.ABS)
          .put("org.apache.pig.builtin.LongAbs", SqlStdOperatorTable.ABS)
          .put("org.apache.pig.builtin.CEIL", SqlStdOperatorTable.CEIL)
          .put("org.apache.pig.builtin.CONCAT", SqlStdOperatorTable.CONCAT)
          .put("org.apache.pig.builtin.StringConcat", SqlStdOperatorTable.CONCAT)
          .put("org.apache.pig.builtin.EXP", SqlStdOperatorTable.EXP)
          .put("org.apache.pig.builtin.FLOOR", SqlStdOperatorTable.FLOOR)
          .put("org.apache.pig.builtin.LOG", SqlStdOperatorTable.LN)
          .put("org.apache.pig.builtin.LOG10", SqlStdOperatorTable.LOG10)
          .put("org.apache.pig.builtin.LOWER", SqlStdOperatorTable.LOWER)
          .put("org.apache.pig.builtin.RANDOM", SqlStdOperatorTable.RAND)
          .put("org.apache.pig.builtin.SQRT", SqlStdOperatorTable.SQRT)
          .put("org.apache.pig.builtin.StringSize", SqlStdOperatorTable.CHAR_LENGTH)
          .put("org.apache.pig.builtin.SUBSTRING", SqlStdOperatorTable.SUBSTRING)
          .put("org.apache.pig.builtin.TOTUPLE", SqlStdOperatorTable.ROW)
          .put("org.apache.pig.builtin.UPPER", SqlStdOperatorTable.UPPER)
          .build();

  private static final Map<String, SqlAggFunction> BUILTIN_AGG_FUNC =
      ImmutableMap.<String, SqlAggFunction>builder()
          // AVG()
          .put("org.apache.pig.builtin.AVG", SqlStdOperatorTable.AVG)
          .put("org.apache.pig.builtin.BigDecimalAvg", SqlStdOperatorTable.AVG)
          .put("org.apache.pig.builtin.BigIntegerAvg", SqlStdOperatorTable.AVG)
          .put("org.apache.pig.builtin.DoubleAvg", SqlStdOperatorTable.AVG)
          .put("org.apache.pig.builtin.FloatAvg", SqlStdOperatorTable.AVG)
          .put("org.apache.pig.builtin.IntAvg", SqlStdOperatorTable.AVG)
          .put("org.apache.pig.builtin.LongAvg", SqlStdOperatorTable.AVG)
          // COUNT()
          .put("org.apache.pig.builtin.COUNT", SqlStdOperatorTable.COUNT)
          // MAX()
          .put("org.apache.pig.builtin.MAX", SqlStdOperatorTable.MAX)
          .put("org.apache.pig.builtin.BigDecimalMax", SqlStdOperatorTable.MAX)
          .put("org.apache.pig.builtin.BigIntegerMax", SqlStdOperatorTable.MAX)
          .put("org.apache.pig.builtin.DateTimeMax", SqlStdOperatorTable.MAX)
          .put("org.apache.pig.builtin.DoubleMax", SqlStdOperatorTable.MAX)
          .put("org.apache.pig.builtin.FloatMax", SqlStdOperatorTable.MAX)
          .put("org.apache.pig.builtin.IntMax", SqlStdOperatorTable.MAX)
          .put("org.apache.pig.builtin.LongMax", SqlStdOperatorTable.MAX)
          .put("org.apache.pig.builtin.StringMax", SqlStdOperatorTable.MAX)
          // MIN()
          .put("org.apache.pig.builtin.MIN", SqlStdOperatorTable.MIN)
          .put("org.apache.pig.builtin.BigDecimalMin", SqlStdOperatorTable.MIN)
          .put("org.apache.pig.builtin.BigIntegerMin", SqlStdOperatorTable.MIN)
          .put("org.apache.pig.builtin.DateTimeMin", SqlStdOperatorTable.MIN)
          .put("org.apache.pig.builtin.DoubleMin", SqlStdOperatorTable.MIN)
          .put("org.apache.pig.builtin.FloatMin", SqlStdOperatorTable.MIN)
          .put("org.apache.pig.builtin.IntMin", SqlStdOperatorTable.MIN)
          .put("org.apache.pig.builtin.LongMin", SqlStdOperatorTable.MIN)
          .put("org.apache.pig.builtin.StringMin", SqlStdOperatorTable.MIN)
          // SUM()
          .put("org.apache.pig.builtin.BigDecimalSum", SqlStdOperatorTable.SUM)
          .put("org.apache.pig.builtin.BigIntegerSum", SqlStdOperatorTable.SUM)
          .put("org.apache.pig.builtin.DoubleSum", SqlStdOperatorTable.SUM)
          .put("org.apache.pig.builtin.FloatSum", SqlStdOperatorTable.SUM)
          .put("org.apache.pig.builtin.IntSum", SqlStdOperatorTable.SUM)
          .put("org.apache.pig.builtin.LongSum", SqlStdOperatorTable.SUM)
          .build();

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
      final Method method =
          PIG_UDF_FINDER.findPigUdfImplementationMethod(clazz);

      // Now create the argument wrapper. Depend on the type of the UDF, the
      // relational operands are converted into a Pig Tuple or Pig DataBag
      // with the appropriate wrapper.
      final SqlUserDefinedFunction convertOp =
          Accumulator.class.isAssignableFrom(clazz)
              ? PigRelSqlUdfs.createPigBagUDF(operands)
              : PigRelSqlUdfs.createPigTupleUDF(operands);
      final RexNode rexTuple = builder.call(convertOp, operands);

      // Then convert the Pig function into a @SqlUserDefinedFunction.
      SqlUserDefinedFunction userFuncOp =
          PigRelSqlUdfs.createGeneralPigUdf(clazz.getSimpleName(),
              method, pigFunc, rexTuple.getType(), returnType);

      // Ready to return SqlCall after having SqlUDF and operand
      return builder.call(userFuncOp, ImmutableList.of(rexTuple));
    } catch (ClassNotFoundException e) {
      throw new FrontendException("Cannot find the implementation for Pig UDF class: "
          + pigFunc.getClassName());
    }
  }

  /**
   * Gets the {@link SqlAggFunction} for the corresponding Pig aggregate
   * UDF call; returns null for invalid rex call.
   *
   * @param call Pig aggregate UDF call
   */
  static SqlAggFunction getSqlAggFuncForPigUdf(RexCall call) {
    if (!(call.getOperator() instanceof PigUserDefinedFunction)) {
      return null;
    }

    final PigUserDefinedFunction pigUdf = (PigUserDefinedFunction) call.getOperator();
    if (pigUdf.funcSpec != null) {
      final String pigUdfClassName = pigUdf.funcSpec.getClassName();
      final SqlAggFunction sqlAggFunction = BUILTIN_AGG_FUNC.get(pigUdfClassName);
      if (sqlAggFunction == null) {
        final Class udfClass =
            ((ScalarFunctionImpl) pigUdf.getFunction()).method.getDeclaringClass();
        if (Accumulator.class.isAssignableFrom(udfClass)) {
          throw new UnsupportedOperationException(
              "Cannot find corresponding SqlAgg func for Pig aggegate " + pigUdfClassName);
        }
      }
      return sqlAggFunction;
    }
    return null;
  }
}

// End PigRelUdfConverter.java
