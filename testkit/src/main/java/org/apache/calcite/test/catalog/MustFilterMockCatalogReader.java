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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Mock catalog reader that tags a few columns in the tables as must-filter.
 *
 * <p>Used for testing must-filter validation.
 * See {@code org.apache.calcite.test.SqlValidatorTest#testMustFilterColumns()}.
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

    // Register "EMP" table. Must-filter fields are "EMPNO", "JOB".
    // Bypass field of column (1): ENAME.
    MustFilterMockTable empTable =
        MustFilterMockTable.create(this, salesSchema, "EMP",
            false, 14, null, NullInitializerExpressionFactory.INSTANCE,
            false, ImmutableMap.of("EMPNO", "10", "JOB", "JOB_1"),
            ImmutableList.of(1));

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

    // Register "DEPT" table. "NAME" is a must-filter field.
    // Bypass field of column (0): DEPTNO.
    MustFilterMockTable deptTable =
        MustFilterMockTable.create(this, salesSchema, "DEPT",
            false, 14, null, NullInitializerExpressionFactory.INSTANCE,
            false, ImmutableMap.of("NAME", "ACCOUNTING_DEPT"),
            ImmutableList.of(0));
    deptTable.addColumn("DEPTNO", integerType, true);
    deptTable.addColumn("NAME", varcharType);
    registerTable(deptTable);
    return this;
  }
}
