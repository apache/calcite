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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import org.apache.pig.FuncSpec;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.piglet.PigTypes.TYPE_FACTORY;

/**
 * User-defined functions ({@link SqlUserDefinedFunction UDFs})
 * needed for Pig-to-{@link RelNode} translation.
 */
public class PigRelSqlUdfs {
  private PigRelSqlUdfs() {
  }

  // Defines ScalarFunc from their implementations
  private static final ScalarFunction PIG_TUPLE_FUNC =
      ScalarFunctionImpl.create(PigRelSqlUdfs.class, "buildTuple");
  private static final ScalarFunction PIG_BAG_FUNC =
      ScalarFunctionImpl.create(PigRelSqlUdfs.class, "buildBag");
  private static final ScalarFunction MULTISET_PROJECTION_FUNC =
      ScalarFunctionImpl.create(PigRelSqlUdfs.class, "projectMultiset");

  /**
   * Multiset projection projects a subset of columns from the component type
   * of a multiset type. The result is still a multiset but the component
   * type only has a subset of columns of the original component type
   *
   * <p>For example, given a multiset type
   * {@code M = [(A: int, B: double, C: varchar)]},
   * a projection
   * {@code MULTISET_PROJECTION(M, A, C)}
   * gives a new multiset
   * {@code N = [(A: int, C: varchar)]}.
   */
  static final SqlUserDefinedFunction MULTISET_PROJECTION =
      new PigUserDefinedFunction("MULTISET_PROJECTION",
          multisetProjectionInfer(), multisetProjectionCheck(), null,
          MULTISET_PROJECTION_FUNC);

  /**
   * Creates a Pig Tuple from a list of relational operands.
   *
   * @param operands Relational operands
   * @return Pig Tuple SqlUDF
   */
  static SqlUserDefinedFunction createPigTupleUDF(ImmutableList<RexNode> operands) {
    return new PigUserDefinedFunction("PIG_TUPLE",
        infer(PigRelSqlUdfs.PIG_TUPLE_FUNC),
        OperandTypes.family(getTypeFamilies(operands)),
        getRelDataTypes(operands),
        PigRelSqlUdfs.PIG_TUPLE_FUNC);
  }

  /**
   * Creates a Pig DataBag from a list of relational operands.
   *
   * @param operands Relational operands
   * @return Pig DataBag SqlUDF
   */
  static SqlUserDefinedFunction createPigBagUDF(ImmutableList<RexNode> operands) {
    return new PigUserDefinedFunction(
        "PIG_BAG",
        infer(PigRelSqlUdfs.PIG_BAG_FUNC),
        OperandTypes.family(getTypeFamilies(operands)),
        getRelDataTypes(operands),
        PigRelSqlUdfs.PIG_BAG_FUNC);
  }

  /**
   * Creates a generic SqlUDF operator from a Pig UDF.
   *
   * @param udfName Name of the UDF
   * @param method Method "exec" for implementing the UDF
   * @param funcSpec Pig Funcspec
   * @param inputType Argument type for the input
   * @param returnType Function return data type
   */
  static SqlUserDefinedFunction createGeneralPigUdf(String udfName,
      Method method, FuncSpec funcSpec, RelDataType inputType,
      RelDataType returnType) {
    return new PigUserDefinedFunction(udfName, opBinding -> returnType,
        OperandTypes.ANY, Collections.singletonList(inputType),
        ScalarFunctionImpl.createUnsafe(method), funcSpec);
  }

  /**
   * Returns a {@link SqlReturnTypeInference} for multiset projection operator.
   */
  private static SqlReturnTypeInference multisetProjectionInfer() {
    return opBinding -> {
      final MultisetSqlType source = (MultisetSqlType) opBinding.getOperandType(0);
      final List<RelDataTypeField> fields = source.getComponentType().getFieldList();
      // Project a multiset of single column
      if (opBinding.getOperandCount() == 2) {
        final int fieldNo = opBinding.getOperandLiteralValue(1, Integer.class);
        if (fields.size() == 1) {
          // Corner case: source with only single column, nothing to do.
          assert fieldNo == 0;
          return source;
        } else {
          return TYPE_FACTORY.createMultisetType(fields.get(fieldNo).getType(), -1);
        }
      }
      // Construct a multiset of records of the input argument types
      final List<String> destNames = new ArrayList<>();
      final List<RelDataType> destTypes = new ArrayList<>();
      for (int i = 1; i < opBinding.getOperandCount(); i++) {
        final int fieldNo = opBinding.getOperandLiteralValue(i, Integer.class);
        destNames.add(fields.get(fieldNo).getName());
        destTypes.add(fields.get(fieldNo).getType());
      }
      return TYPE_FACTORY.createMultisetType(
          TYPE_FACTORY.createStructType(destTypes, destNames), -1);
    };
  }

  /**
   * Returns a {@link SqlOperandTypeChecker} for multiset projection operator.
   */
  private static SqlOperandTypeChecker multisetProjectionCheck() {
    return new SqlOperandTypeChecker() {
      public boolean checkOperandTypes(
          SqlCallBinding callBinding, boolean throwOnFailure) {
        // Need at least two arguments
        if (callBinding.getOperandCount() < 2) {
          return false;
        }

        // The first argument should be a multiset
        if (!(callBinding.getOperandType(0) instanceof MultisetSqlType)) {
          return false;
        }

        // All the subsequent arguments should be appropriate integers
        final MultisetSqlType source = (MultisetSqlType) callBinding.getOperandType(0);
        final int maxFieldNo = source.getComponentType().getFieldCount() - 1;

        for (int i = 1; i < callBinding.getOperandCount(); i++) {
          if (!(callBinding.getOperandLiteralValue(i, Comparable.class)
              instanceof BigDecimal)) {
            return false;
          }
          final int fieldNo =
              callBinding.getOperandLiteralValue(i, Integer.class);
          // Field number should between 0 and maxFieldNo
          if (fieldNo < 0 || fieldNo > maxFieldNo) {
            return false;
          }
        }
        return true;
      }

      public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.from(2);
      }

      public String getAllowedSignatures(SqlOperator op, String opName) {
        return opName + "(...)";
      }

      public boolean isOptional(int i) {
        return false;
      }

      public Consistency getConsistency() {
        return Consistency.NONE;
      }
    };
  }

  /**
   * Helper method to return a list of SqlTypeFamily for a given list of relational operands
   *
   * @param operands List of relational operands
   * @return List of SqlTypeFamilies
   */
  private static List<SqlTypeFamily> getTypeFamilies(ImmutableList<RexNode> operands) {
    List<SqlTypeFamily> ret = new ArrayList<>();
    for (RexNode operand : operands) {
      SqlTypeFamily family = operand.getType().getSqlTypeName().getFamily();
      ret.add(family != null ? family : SqlTypeFamily.ANY);
    }
    return ret;
  }

  /**
   * Helper method to return a list of RelDataType for a given list of relational operands
   *
   * @param operands List of relational operands
   * @return List of RelDataTypes
   */
  private static List<RelDataType> getRelDataTypes(ImmutableList<RexNode> operands) {
    List<RelDataType> ret = new ArrayList<>();
    for (RexNode operand : operands) {
      ret.add(operand.getType());
    }
    return ret;
  }

  /**
   * Gets the SqlReturnTypeInference that can infer the return type from a
   * function.
   *
   * @param function ScalarFunction
   * @return SqlReturnTypeInference
   */
  private static SqlReturnTypeInference infer(final ScalarFunction function) {
    return opBinding -> getRelDataType(function);
  }

  /**
   * Gets the return data type for a given function.
   *
   * @param function ScalarFunction
   * @return returned data type
   */
  private static RelDataType getRelDataType(ScalarFunction function) {
    final JavaTypeFactory typeFactory = TYPE_FACTORY;
    final RelDataType type = function.getReturnType(typeFactory);
    if (type instanceof RelDataTypeFactoryImpl.JavaType
        && ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass()
        == Object.class) {
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.ANY), true);
    }
    return typeFactory.toSql(type);
  }

  /**
   * Implementation for PIG_TUPLE functions. Builds a Pig Tuple from
   * an array of objects
   *
   * @param elements Array of element objects
   * @return Pig Tuple
   */
  public static Tuple buildTuple(Object... elements) {
    return TupleFactory.getInstance().newTuple(Arrays.asList(elements));
  }


  /**
   * Implementation for PIG_BAG functions. Builds a Pig DataBag from
   * the corresponding input
   *
   * @param elements Input that contains a bag
   * @return Pig Tuple
   */
  public static Tuple buildBag(Object... elements) {
    final TupleFactory tupleFactory = TupleFactory.getInstance();
    final BagFactory bagFactory = BagFactory.getInstance();
    // Convert each row into a Tuple
    List<Tuple> tupleList = new ArrayList<>();
    if (elements != null) {
      // The first input contains a list of rows for the bag
      final List bag = (elements[0] instanceof List)
          ? (List) elements[0]
          : Collections.singletonList(elements[0]);
      for (Object row : bag) {
        tupleList.add(tupleFactory.newTuple(Arrays.asList(row)));
      }
    }

    // Then build a bag from the tuple list
    DataBag resultBag = bagFactory.newDefaultBag(tupleList);

    // The returned result is a new Tuple with the newly constructed DataBag
    // as the first item.
    List<Object> finalTuple = new ArrayList<>();
    finalTuple.add(resultBag);

    if (elements != null) {
      // Add the remaining elements from the input
      for (int i = 1; i < elements.length; i++) {
        finalTuple.add(elements[i]);
      }
    }

    return tupleFactory.newTuple(finalTuple);
  }

  /**
   * Implementation for BAG_PROJECTION functions. Builds a new multiset by
   * projecting certain columns from another multiset.
   *
   * @param objects Input argument, the first one is a multiset, the remaining
   *                are indexes of column to project.
   * @return The projected multiset
   */
  public static List projectMultiset(Object... objects) {
    // The first input is a multiset
    final List<Object[]> inputMultiset = (List) objects[0];
    final List projectedMultiset = new ArrayList<>();

    for (Object[] row : inputMultiset) {
      if (objects.length > 2) {
        // Projecting more than one column, the projected multiset should have
        // the component type of a row
        Object[] newRow = new Object[objects.length - 1];
        for (int j = 1; j < objects.length; j++) {
          newRow[j - 1] = row[(Integer) objects[j]];
        }
        projectedMultiset.add(newRow);
      } else {
        // Projecting a single column
        projectedMultiset.add(row[(Integer) objects[1]]);
      }
    }
    return projectedMultiset;
  }
}

// End PigRelSqlUdfs.java
