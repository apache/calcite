/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.optiq.test;

import org.apache.optiq.reltype.*;
import org.apache.optiq.sql.*;
import org.apache.optiq.sql.type.*;
import org.apache.optiq.sql.util.*;

import com.google.common.collect.ImmutableList;

/**
 * Mock operator table for testing purposes. Contains the standard SQL operator
 * table, plus a list of operators.
 */
public class MockSqlOperatorTable extends ChainedSqlOperatorTable {
  //~ Instance fields --------------------------------------------------------

  private final ListSqlOperatorTable listOpTab;

  //~ Constructors -----------------------------------------------------------

  public MockSqlOperatorTable(SqlOperatorTable parentTable) {
    super(ImmutableList.of(parentTable, new ListSqlOperatorTable()));
    listOpTab = (ListSqlOperatorTable) tableList.get(1);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Adds an operator to this table.
   */
  public void addOperator(SqlOperator op) {
    listOpTab.add(op);
  }

  public static void addRamp(MockSqlOperatorTable opTab) {
    // Don't use anonymous inner classes. They can't be instantiated
    // using reflection when we are deserializing from JSON.
    opTab.addOperator(new RampFunction());
    opTab.addOperator(new DedupFunction());
  }

  public static class RampFunction extends SqlFunction {
    public RampFunction() {
      super(
          "RAMP",
          SqlKind.OTHER_FUNCTION,
          null,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory =
          opBinding.getTypeFactory();
      return typeFactory.builder()
          .add("I", SqlTypeName.INTEGER)
          .build();
    }
  }

  public static class DedupFunction extends SqlFunction {
    public DedupFunction() {
      super(
          "DEDUP",
          SqlKind.OTHER_FUNCTION,
          null,
          null,
          OperandTypes.VARIADIC,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory =
          opBinding.getTypeFactory();
      return typeFactory.builder()
          .add("NAME", SqlTypeName.VARCHAR, 1024)
          .build();
    }
  }
}

// End MockSqlOperatorTable.java
