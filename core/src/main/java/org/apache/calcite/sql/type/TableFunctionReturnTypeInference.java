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

import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperatorBinding;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * TableFunctionReturnTypeInference implements rules for deriving table function
 * output row types by expanding references to cursor parameters.
 */
public class TableFunctionReturnTypeInference
    extends ExplicitReturnTypeInference {
  //~ Instance fields --------------------------------------------------------

  private final List<String> paramNames;

  private Set<RelColumnMapping> columnMappings; // not re-entrant!

  private final boolean isPassthrough;

  //~ Constructors -----------------------------------------------------------

  public TableFunctionReturnTypeInference(
      RelProtoDataType unexpandedOutputType,
      List<String> paramNames,
      boolean isPassthrough) {
    super(unexpandedOutputType);
    this.paramNames = paramNames;
    this.isPassthrough = isPassthrough;
  }

  //~ Methods ----------------------------------------------------------------

  public Set<RelColumnMapping> getColumnMappings() {
    return columnMappings;
  }

  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    columnMappings = new HashSet<>();
    RelDataType unexpandedOutputType =
        protoType.apply(opBinding.getTypeFactory());
    List<RelDataType> expandedOutputTypes = new ArrayList<>();
    List<String> expandedFieldNames = new ArrayList<>();
    for (RelDataTypeField field : unexpandedOutputType.getFieldList()) {
      RelDataType fieldType = field.getType();
      String fieldName = field.getName();
      if (fieldType.getSqlTypeName() != SqlTypeName.CURSOR) {
        expandedOutputTypes.add(fieldType);
        expandedFieldNames.add(fieldName);
        continue;
      }

      // Look up position of cursor parameter with same name as output
      // field, also counting how many cursors appear before it
      // (need this for correspondence with RelNode child position).
      int paramOrdinal = -1;
      int iCursor = 0;
      for (int i = 0; i < paramNames.size(); ++i) {
        if (paramNames.get(i).equals(fieldName)) {
          paramOrdinal = i;
          break;
        }
        RelDataType cursorType = opBinding.getCursorOperand(i);
        if (cursorType != null) {
          ++iCursor;
        }
      }
      assert paramOrdinal != -1;

      // Translate to actual argument type.
      boolean isRowOp = false;
      List<String> columnNames = new ArrayList<>();
      RelDataType cursorType = opBinding.getCursorOperand(paramOrdinal);
      if (cursorType == null) {
        isRowOp = true;
        String parentCursorName =
            opBinding.getColumnListParamInfo(
                paramOrdinal,
                fieldName,
                columnNames);
        assert parentCursorName != null;
        paramOrdinal = -1;
        iCursor = 0;
        for (int i = 0; i < paramNames.size(); ++i) {
          if (paramNames.get(i).equals(parentCursorName)) {
            paramOrdinal = i;
            break;
          }
          cursorType = opBinding.getCursorOperand(i);
          if (cursorType != null) {
            ++iCursor;
          }
        }
        cursorType = opBinding.getCursorOperand(paramOrdinal);
        assert cursorType != null;
      }

      // And expand. Function output is always nullable... except system
      // fields.
      int iInputColumn;
      if (isRowOp) {
        for (String columnName : columnNames) {
          iInputColumn = -1;
          RelDataTypeField cursorField = null;
          for (RelDataTypeField cField : cursorType.getFieldList()) {
            ++iInputColumn;
            if (cField.getName().equals(columnName)) {
              cursorField = cField;
              break;
            }
          }
          addOutputColumn(
              expandedFieldNames,
              expandedOutputTypes,
              iInputColumn,
              iCursor,
              opBinding,
              cursorField);
        }
      } else {
        iInputColumn = -1;
        for (RelDataTypeField cursorField : cursorType.getFieldList()) {
          ++iInputColumn;
          addOutputColumn(
              expandedFieldNames,
              expandedOutputTypes,
              iInputColumn,
              iCursor,
              opBinding,
              cursorField);
        }
      }
    }
    return opBinding.getTypeFactory().createStructType(
        expandedOutputTypes,
        expandedFieldNames);
  }

  private void addOutputColumn(
      List<String> expandedFieldNames,
      List<RelDataType> expandedOutputTypes,
      int iInputColumn,
      int iCursor,
      SqlOperatorBinding opBinding,
      RelDataTypeField cursorField) {
    columnMappings.add(
        new RelColumnMapping(
            expandedFieldNames.size(),
            iCursor,
            iInputColumn,
            !isPassthrough));

    // As a special case, system fields are implicitly NOT NULL.
    // A badly behaved UDX can still provide NULL values, so the
    // system must ensure that each generated system field has a
    // reasonable value.
    boolean nullable = true;
    if (opBinding instanceof SqlCallBinding) {
      SqlCallBinding sqlCallBinding = (SqlCallBinding) opBinding;
      if (sqlCallBinding.getValidator().isSystemField(
          cursorField)) {
        nullable = false;
      }
    }
    RelDataType nullableType =
        opBinding.getTypeFactory().createTypeWithNullability(
            cursorField.getType(),
            nullable);

    // Make sure there are no duplicates in the output column names
    for (String fieldName : expandedFieldNames) {
      if (fieldName.equals(cursorField.getName())) {
        throw opBinding.newError(
            RESOURCE.duplicateColumnName(cursorField.getName()));
      }
    }
    expandedOutputTypes.add(nullableType);
    expandedFieldNames.add(cursorField.getName());
  }
}

// End TableFunctionReturnTypeInference.java
