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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Base class for table-valued function windowing operator (TUMBLE, HOP and SESSION).
 */
public class SqlWindowTableFunction extends SqlFunction {
  public SqlWindowTableFunction(String name) {
    super(name,
        SqlKind.OTHER_FUNCTION,
        ARG0_TABLE_FUNCTION_WINDOWING,
        null,
        null,
        SqlFunctionCategory.SYSTEM);
  }

  protected boolean throwValidationSignatureErrorOrReturnFalse(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (throwOnFailure) {
      throw callBinding.newValidationSignatureError();
    } else {
      return false;
    }
  }

  protected void validateColumnNames(SqlValidator validator,
      List<String> fieldNames, List<SqlNode> unvalidatedColumnNames) {
    for (SqlNode descOperand: unvalidatedColumnNames) {
      final String colName = ((SqlIdentifier) descOperand).getSimple();
      boolean matches = false;
      for (String field : fieldNames) {
        if (validator.getCatalogReader().nameMatcher().matches(field, colName)) {
          matches = true;
          break;
        }
      }
      if (!matches) {
        throw SqlUtil.newContextException(descOperand.getParserPosition(),
            RESOURCE.unknownIdentifier(colName));
      }
    }
  }

  /**
   * The first parameter of table-value function windowing is a TABLE parameter,
   * which is not scalar. So need to override SqlOperator.argumentMustBeScalar.
   */
  @Override public boolean argumentMustBeScalar(int ordinal) {
    return ordinal != 0;
  }

  /**
   * Type-inference strategy whereby the result type of a table function call is a ROW,
   * which is combined from the operand #0(TABLE parameter)'s schema and two
   * additional fields:
   *
   * <ol>
   *  <li>window_start. TIMESTAMP type to indicate a window's start.</li>
   *  <li>window_end. TIMESTAMP type to indicate a window's end.</li>
   * </ol>
   */
  public static final SqlReturnTypeInference ARG0_TABLE_FUNCTION_WINDOWING =
      opBinding -> {
        RelDataType inputRowType = opBinding.getOperandType(0);
        List<RelDataTypeField> newFields = new ArrayList<>(inputRowType.getFieldList());
        RelDataType timestampType = opBinding.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);

        RelDataTypeField windowStartField =
            new RelDataTypeFieldImpl("window_start", newFields.size(), timestampType);
        newFields.add(windowStartField);
        RelDataTypeField windowEndField =
            new RelDataTypeFieldImpl("window_end", newFields.size(), timestampType);
        newFields.add(windowEndField);

        return new RelRecordType(inputRowType.getStructKind(), newFields);
      };
}
