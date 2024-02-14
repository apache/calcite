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
package org.apache.calcite.test.catalog;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Tags a few columns in the tables as must-filter.
 * Used for testing must-filter validation.
 * See {@code org.apache.calcite.test.SqlValidatorTest#testMustFilterColumns()}
 */
public class MustFilterMockCatalogReader extends MockCatalogReader {

  MustFilterMockCatalogReader(RelDataTypeFactory typeFactory,
      boolean caseSensitive) {
    super(typeFactory, caseSensitive);
  }

  public static SqlValidatorCatalogReader create(RelDataTypeFactory typeFactory,
      boolean caseSensitive) {
    return new MustFilterMockCatalogReader(typeFactory, caseSensitive).init();
  }

  @Override public MockCatalogReader init() {
    MockSchema salesSchema = new MockSchema("SALES");
    registerSchema(salesSchema);
    Map<String, String> empMustFilterFields =
        new HashMap<String, String>() {{
          put("EMPNO", "10");
          put("JOB", "JOB_1");
        }};
    // Register "EMP" table.
    MustFilterMockTable empTable =
        MustFilterMockTable.create(this, salesSchema, "EMP",
            false, 14, null, NullInitializerExpressionFactory.INSTANCE,
            false, empMustFilterFields);

    final RelDataType integerType =
        typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType timestampType =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    final RelDataType varcharType =
        typeFactory.createSqlType(SqlTypeName.VARCHAR);
    final RelDataType booleanType =
        typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    empTable.addColumn("EMPNO", integerType, true);
    empTable.addColumn("ENAME", varcharType);
    empTable.addColumn("JOB", varcharType);
    empTable.addColumn("MGR", integerType);
    empTable.addColumn("HIREDATE", timestampType);
    empTable.addColumn("SAL", integerType);
    empTable.addColumn("COMM", integerType);
    empTable.addColumn("DEPTNO", integerType);
    empTable.addColumn("SLACKER", booleanType);
    registerTable(empTable);

    // Register "DEPT" table.
    Map<String, String> deptMustFilterFields =
        new HashMap<String, String>() {{
          put("NAME", "ACCOUNTING_DEPT");
        }};
    MustFilterMockTable deptTable =
        MustFilterMockTable.create(this, salesSchema, "DEPT",
            false, 14, null, NullInitializerExpressionFactory.INSTANCE,
            false, deptMustFilterFields);
    deptTable.addColumn("DEPTNO", integerType, true);
    deptTable.addColumn("NAME", varcharType);
    registerTable(deptTable);
    return this;
  }
}
