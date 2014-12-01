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
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.type.OperandTypes;

/**
 * The <code>UNNEST</code> operator.
 */
public class SqlUnnestOperator extends SqlFunctionalOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlUnnestOperator() {
    super(
        "UNNEST",
        SqlKind.UNNEST,
        200,
        true,
        null,
        null,
        OperandTypes.SCALAR_OR_RECORD_COLLECTION);
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    RelDataType type = opBinding.getOperandType(0);
    if (type.isStruct()) {
      type = type.getFieldList().get(0).getType();
    }
    assert type instanceof ArraySqlType || type instanceof MultisetSqlType;
    return type.getComponentType();
  }

  public boolean argumentMustBeScalar(int ordinal) {
    return false;
  }
}

// End SqlUnnestOperator.java
