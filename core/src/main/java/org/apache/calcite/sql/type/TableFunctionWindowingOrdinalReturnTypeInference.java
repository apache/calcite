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
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.SqlOperatorBinding;

import java.util.ArrayList;
import java.util.List;

/**
 * Type-inference strategy whereby the result type of a table function call is a ROW,
 * which is combined from the TABLE parameter's schema (the parameter is specified by ordinal)
 * and two additional fields:
 *  1. wstart. TIMESTAMP type to indicate a window's start.
 *  2. wend. TIMESTAMP type to indicate a window's end.
 */
public class TableFunctionWindowingOrdinalReturnTypeInference implements SqlReturnTypeInference {
  //~ Instance fields --------------------------------------------------------

  private final int ordinal;

  //~ Constructors -----------------------------------------------------------

  public TableFunctionWindowingOrdinalReturnTypeInference(int ordinal) {
    this.ordinal = ordinal;
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    RelDataType inputRowType = opBinding.getOperandType(ordinal);
    List<RelDataTypeField> newFields = new ArrayList<>(inputRowType.getFieldList());
    RelDataType timestampType = opBinding.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);

    RelDataTypeField windowStartField =
        new RelDataTypeFieldImpl("wstart", newFields.size(), timestampType);
    newFields.add(windowStartField);
    RelDataTypeField windowEndField =
        new RelDataTypeFieldImpl("wend", newFields.size(), timestampType);
    newFields.add(windowEndField);

    return new RelRecordType(inputRowType.getStructKind(), newFields);
  }
}

// End TableFunctionWindowingOrdinalReturnTypeInference.java
