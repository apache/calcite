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
package org.eigenbase.sql.test;

import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.advise.SqlAdvisor;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;
import org.eigenbase.sql.validate.*;
import org.eigenbase.test.MockCatalogReader;
import org.eigenbase.test.MockSqlOperatorTable;

import com.google.common.collect.ImmutableMap;

/**
* Default implementation of {@link SqlTestFactory}.
 *
 * <p>Suitable for most tests. If you want different behavior, you can extend;
 * if you want a factory with different properties (e.g. SQL conformance level
 * or identifier quoting), wrap in a
 * {@link DelegatingSqlTestFactory} and
 * override {@link #get}.</p>
*/
public class DefaultSqlTestFactory implements SqlTestFactory {
  public static final ImmutableMap<String, Object> DEFAULT_OPTIONS =
      ImmutableMap.of(
          "quoting", (Object) SqlParser.Quoting.DOUBLE_QUOTE,
          "conformance", SqlConformance.Default);

  public static final DefaultSqlTestFactory INSTANCE =
      new DefaultSqlTestFactory();

  private DefaultSqlTestFactory() {
  }

  public SqlOperatorTable createOperatorTable() {
    MockSqlOperatorTable opTab =
        new MockSqlOperatorTable(SqlStdOperatorTable.instance());
    MockSqlOperatorTable.addRamp(opTab);
    return opTab;
  }

  public SqlParser createParser(SqlTestFactory factory, String sql) {
    SqlParser.Quoting quoting = (SqlParser.Quoting) factory.get("quoting");
    return new SqlParser(sql, quoting);
  }

  public SqlValidator getValidator(SqlTestFactory factory) {
    final SqlOperatorTable operatorTable = factory.createOperatorTable();
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl();
    return SqlValidatorUtil.newValidator(operatorTable,
        new MockCatalogReader(typeFactory),
        typeFactory);
  }

  public SqlAdvisor createAdvisor(SqlValidatorWithHints validator) {
    throw new UnsupportedOperationException();
  }

  public Object get(String name) {
    return DEFAULT_OPTIONS.get(name);
  }
}

// End DefaultSqlTestFactory.java
