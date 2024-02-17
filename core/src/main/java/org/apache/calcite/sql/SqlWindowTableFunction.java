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
package org.apache.calcite.sql;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Base class for a table-valued function that computes windows. Examples
 * include {@code TUMBLE}, {@code HOP} and {@code SESSION}.
 */
public class SqlWindowTableFunction extends SqlFunction
    implements SqlTableFunction {

  /** The data source which the table function computes with. */
  protected static final String PARAM_DATA = "DATA";

  /** The time attribute column. Also known as the event time. */
  protected static final String PARAM_TIMECOL = "TIMECOL";

  /** The window duration INTERVAL. */
  protected static final String PARAM_SIZE = "SIZE";

  /** The optional align offset for each window. */
  protected static final String PARAM_OFFSET = "OFFSET";

  /** The session key(s), only used for SESSION window. */
  protected static final String PARAM_KEY = "KEY";

  /** The slide interval, only used for HOP window. */
  protected static final String PARAM_SLIDE = "SLIDE";

  /**
   * Type-inference strategy whereby the row type of a table function call is a
   * ROW, which is combined from the row type of operand #0 (which is a TABLE)
   * and two additional fields. The fields are as follows:
   *
   * <ol>
   * <li>{@code window_start}: TIMESTAMP type to indicate a window's start
   * <li>{@code window_end}: TIMESTAMP type to indicate a window's end
   * </ol>
   */
  public static final SqlReturnTypeInference ARG0_TABLE_FUNCTION_WINDOWING =
      SqlWindowTableFunction::inferRowType;

  /** Creates a window table function with a given name. */
  public SqlWindowTableFunction(String name, SqlOperandMetadata operandMetadata) {
    super(name, SqlKind.OTHER_FUNCTION, ReturnTypes.CURSOR, null,
        operandMetadata, SqlFunctionCategory.SYSTEM);
  }

  @Override public @Nullable SqlOperandMetadata getOperandTypeChecker() {
    return (@Nullable SqlOperandMetadata) super.getOperandTypeChecker();
  }

  @Override public SqlReturnTypeInference getRowTypeInference() {
    return ARG0_TABLE_FUNCTION_WINDOWING;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Overrides because the first parameter of
   * table-value function windowing is an explicit TABLE parameter,
   * which is not scalar.
   */
  @Override public boolean argumentMustBeScalar(int ordinal) {
    return ordinal != 0;
  }

  /** Helper for {@link #ARG0_TABLE_FUNCTION_WINDOWING}. */
  private static RelDataType inferRowType(SqlOperatorBinding opBinding) {
    final RelDataType inputRowType = opBinding.getOperandType(0);
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    return typeFactory.builder()
        .kind(inputRowType.getStructKind())
        .addAll(inputRowType.getFieldList())
        .add("window_start", SqlTypeName.TIMESTAMP, 3)
        .add("window_end", SqlTypeName.TIMESTAMP, 3)
        .build();
  }

  /** Partial implementation of operand type checker. */
  protected abstract static class AbstractOperandMetadata
      implements SqlOperandMetadata {
    final List<String> paramNames;
    final int mandatoryParamCount;

    AbstractOperandMetadata(List<String> paramNames,
        int mandatoryParamCount) {
      this.paramNames = ImmutableList.copyOf(paramNames);
      this.mandatoryParamCount = mandatoryParamCount;
      checkArgument(mandatoryParamCount >= 0
          && mandatoryParamCount <= paramNames.size());
    }

    @Override public SqlOperandCountRange getOperandCountRange() {
      return SqlOperandCountRanges.between(mandatoryParamCount,
          paramNames.size());
    }

    @Override public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
      return Collections.nCopies(paramNames.size(),
          typeFactory.createSqlType(SqlTypeName.ANY));
    }

    @Override public List<String> paramNames() {
      return paramNames;
    }

    @Override public boolean isOptional(int i) {
      return i > getOperandCountRange().getMin()
          && i <= getOperandCountRange().getMax();
    }

    boolean throwValidationSignatureErrorOrReturnFalse(SqlCallBinding callBinding,
        boolean throwOnFailure) {
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      } else {
        return false;
      }
    }

    /**
     * Checks whether the heading operands are in the form
     * {@code (ROW, DESCRIPTOR, DESCRIPTOR ..., other params)},
     * returning whether successful, and throwing if any columns are not found.
     *
     * @param callBinding The call binding
     * @param descriptorCount The number of descriptors following the first
     * operand (e.g. the table)
     *
     * @return true if validation passes; throws if any columns are not found
     */
    boolean checkTableAndDescriptorOperands(SqlCallBinding callBinding,
        int descriptorCount) {
      final SqlNode operand0 = callBinding.operand(0);
      final SqlValidator validator = callBinding.getValidator();
      final RelDataType type = validator.getValidatedNodeType(operand0);
      if (type.getSqlTypeName() != SqlTypeName.ROW) {
        return false;
      }
      for (int i = 1; i < descriptorCount + 1; i++) {
        final SqlNode operand = callBinding.operand(i);
        if (operand.getKind() != SqlKind.DESCRIPTOR) {
          return false;
        }
        validateColumnNames(validator, type.getFieldNames(),
            ((SqlCall) operand).getOperandList());
      }
      return true;
    }

    /**
     * Checks whether the type that the operand of time col descriptor refers to is valid.
     *
     * @param callBinding The call binding
     * @param pos The position of the descriptor at the operands of the call
     * @return true if validation passes, false otherwise
     */
    boolean checkTimeColumnDescriptorOperand(SqlCallBinding callBinding, int pos) {
      SqlValidator validator = callBinding.getValidator();
      SqlNode operand0 = callBinding.operand(0);
      RelDataType type = validator.getValidatedNodeType(operand0);
      List<SqlNode> operands = ((SqlCall) callBinding.operand(pos)).getOperandList();
      SqlIdentifier identifier = (SqlIdentifier) operands.get(0);
      String columnName = identifier.getSimple();
      SqlNameMatcher matcher = validator.getCatalogReader().nameMatcher();
      for (RelDataTypeField field : type.getFieldList()) {
        if (matcher.matches(field.getName(), columnName)) {
          return SqlTypeUtil.isTimestamp(field.getType());
        }
      }
      return false;
    }

    /**
     * Checks whether the operands starting from position {@code startPos} are
     * all of type {@code INTERVAL}, returning whether successful.
     *
     * @param callBinding The call binding
     * @param startPos    The start position to validate (starting index is 0)
     *
     * @return true if validation passes
     */
    boolean checkIntervalOperands(SqlCallBinding callBinding, int startPos) {
      final SqlValidator validator = callBinding.getValidator();
      for (int i = startPos; i < callBinding.getOperandCount(); i++) {
        final RelDataType type = validator.getValidatedNodeType(callBinding.operand(i));
        if (!SqlTypeUtil.isInterval(type)) {
          return false;
        }
      }
      return true;
    }

    void validateColumnNames(SqlValidator validator,
        List<String> fieldNames, List<SqlNode> columnNames) {
      final SqlNameMatcher matcher = validator.getCatalogReader().nameMatcher();
      Ord.forEach(SqlIdentifier.simpleNames(columnNames), (name, i) -> {
        if (matcher.indexOf(fieldNames, name) < 0) {
          final SqlIdentifier columnName = (SqlIdentifier) columnNames.get(i);
          throw SqlUtil.newContextException(columnName.getParserPosition(),
              RESOURCE.unknownIdentifier(name));
        }
      });
    }
  }
}
