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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Strategies for inferring operand types.
 *
 * @see org.apache.calcite.sql.type.SqlOperandTypeInference
 * @see org.apache.calcite.sql.type.ReturnTypes
 */
public abstract class InferTypes {
  private InferTypes() {}

  /**
   * Operand type-inference strategy where an unknown operand type is derived
   * from the first operand with a known type.
   */
  public static final SqlOperandTypeInference FIRST_KNOWN =
      (callBinding, returnType, operandTypes) -> {
        final RelDataType unknownType =
            callBinding.getValidator().getUnknownType();
        RelDataType knownType = unknownType;
        for (SqlNode operand : callBinding.operands()) {
          knownType = SqlTypeUtil.deriveType(callBinding, operand);
          if (!knownType.equals(unknownType)) {
            break;
          }
        }

        // REVIEW jvs 11-Nov-2008:  We can't assert this
        // because SqlAdvisorValidator produces
        // unknown types for incomplete expressions.
        // Maybe we need to distinguish the two kinds of unknown.
        //   assert !knownType.equals(unknownType);
        Arrays.fill(operandTypes, knownType);
      };

  /**
   * Operand type-inference strategy where an unknown operand type is derived
   * from the call's return type. If the return type is a record, it must have
   * the same number of fields as the number of operands.
   */
  public static final SqlOperandTypeInference RETURN_TYPE =
      (callBinding, returnType, operandTypes) -> {
        for (int i = 0; i < operandTypes.length; ++i) {
          if (returnType.isStruct()) {
            if (returnType.getFieldCount() > i) {
              operandTypes[i] = returnType.getFieldList().get(i).getType();
            } else {
              throw callBinding.newError(RESOURCE.unequalRowSizes());
            }
          } else {
            operandTypes[i] = returnType;
          }
        }
      };

  /**
   * Operand type-inference strategy where an unknown operand type is assumed
   * to be boolean.
   */
  public static final SqlOperandTypeInference BOOLEAN =
      (callBinding, returnType, operandTypes) -> {
        RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
        for (int i = 0; i < operandTypes.length; ++i) {
          operandTypes[i] =
              typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        }
      };

  /**
   * Operand type-inference strategy where an unknown operand type is assumed
   * to be VARCHAR(1024).  This is not something which should be used in most
   * cases (especially since the precision is arbitrary), but for IS [NOT]
   * NULL, we don't really care about the type at all, so it's reasonable to
   * use something that every other type can be cast to.
   */
  public static final SqlOperandTypeInference VARCHAR_1024 =
      (callBinding, returnType, operandTypes) -> {
        RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
        for (int i = 0; i < operandTypes.length; ++i) {
          operandTypes[i] =
              typeFactory.createSqlType(SqlTypeName.VARCHAR, 1024);
        }
      };

  /**
   * Operand type-inference strategy where an unknown operand type is assumed
   * to be nullable ANY.
   */
  public static final SqlOperandTypeInference ANY_NULLABLE =
      (callBinding, returnType, operandTypes) -> {
        RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
        for (int i = 0; i < operandTypes.length; ++i) {
          operandTypes[i] =
              typeFactory.createTypeWithNullability(
                  typeFactory.createSqlType(SqlTypeName.ANY), true);
        }
      };

  /** Returns an {@link SqlOperandTypeInference} that returns a given list of
   * types. */
  public static SqlOperandTypeInference explicit(List<RelDataType> types) {
    return new ExplicitOperandTypeInference(ImmutableList.copyOf(types));
  }
}
