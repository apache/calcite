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
package org.apache.calcite.test;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.util.Optionality;

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
    opTab.addOperator(new MyFunction());
    opTab.addOperator(new MyAvgAggFunction());
  }

  /** "RAMP" user-defined function. */
  public static class RampFunction extends SqlFunction {
    public RampFunction() {
      super("RAMP",
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

  /** "DEDUP" user-defined function. */
  public static class DedupFunction extends SqlFunction {
    public DedupFunction() {
      super("DEDUP",
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

  /** "MYFUN" user-defined scalar function. */
  public static class MyFunction extends SqlFunction {
    public MyFunction() {
      super("MYFUN",
          new SqlIdentifier("MYFUN", SqlParserPos.ZERO),
          SqlKind.OTHER_FUNCTION,
          null,
          null,
          OperandTypes.NUMERIC,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory =
          opBinding.getTypeFactory();
      return typeFactory.createSqlType(SqlTypeName.BIGINT);
    }
  }

  /** "MYAGG" user-defined aggregate function. This agg function accept two numeric arguments
   * in order to reproduce the throws of CALCITE-2744. */
  public static class MyAvgAggFunction extends SqlAggFunction {
    public MyAvgAggFunction() {
      super("MYAGG", null, SqlKind.AVG, ReturnTypes.AVG_AGG_FUNCTION,
          null, OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
          SqlFunctionCategory.NUMERIC, false, false, Optionality.FORBIDDEN);
    }

    @Override public boolean isDeterministic() {
      return false;
    }
  }
}

// End MockSqlOperatorTable.java
