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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Util;

import java.util.AbstractList;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parameter type-checking strategy for a set operator (UNION, INTERSECT,
 * EXCEPT).
 *
 * <p>Both arguments must be records with the same number of fields, and the
 * fields must be union-compatible.
 */
public class SetopOperandTypeChecker implements SqlOperandTypeChecker {
  //~ Methods ----------------------------------------------------------------

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    assert callBinding.getOperandCount() == 2
        : "setops are binary (for now)";
    final RelDataType[] argTypes =
        new RelDataType[callBinding.getOperandCount()];
    int colCount = -1;
    final SqlValidator validator = callBinding.getValidator();
    for (int i = 0; i < argTypes.length; i++) {
      final RelDataType argType =
          argTypes[i] = callBinding.getOperandType(i);
      Util.permAssert(
          argType.isStruct(),
          "setop arg must be a struct");

      // Each operand must have the same number of columns.
      final List<RelDataTypeField> fields = argType.getFieldList();
      if (i == 0) {
        colCount = fields.size();
        continue;
      }

      if (fields.size() != colCount) {
        if (throwOnFailure) {
          SqlNode node = callBinding.getCall().operand(i);
          if (node instanceof SqlSelect) {
            node = ((SqlSelect) node).getSelectList();
          }
          throw validator.newValidationError(node,
              RESOURCE.columnCountMismatchInSetop(
                  callBinding.getOperator().getName()));
        } else {
          return false;
        }
      }
    }

    // The columns must be pairwise union compatible. For each column
    // ordinal, form a 'slice' containing the types of the ordinal'th
    // column j.
    for (int i = 0; i < colCount; i++) {
      final int i2 = i;
      final RelDataType type =
          callBinding.getTypeFactory().leastRestrictive(
              new AbstractList<RelDataType>() {
                public RelDataType get(int index) {
                  return argTypes[index].getFieldList().get(i2)
                      .getType();
                }

                public int size() {
                  return argTypes.length;
                }
              });
      if (type == null) {
        if (throwOnFailure) {
          SqlNode field =
              SqlUtil.getSelectListItem(
                  callBinding.getCall().operand(0),
                  i);
          throw validator.newValidationError(field,
              RESOURCE.columnTypeMismatchInSetop(i + 1, // 1-based
                  callBinding.getOperator().getName()));
        } else {
          return false;
        }
      }
    }

    return true;
  }

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(2);
  }

  public String getAllowedSignatures(SqlOperator op, String opName) {
    return "{0} " + opName + " {1}"; // todo: Wael, please review.
  }
}

// End SetopOperandTypeChecker.java
