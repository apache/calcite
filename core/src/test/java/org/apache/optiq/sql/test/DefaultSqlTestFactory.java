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
package org.apache.optiq.sql.test;

import org.apache.optiq.reltype.RelDataTypeFactory;
import org.apache.optiq.sql.SqlOperatorTable;
import org.apache.optiq.sql.advise.SqlAdvisor;
import org.apache.optiq.sql.fun.SqlStdOperatorTable;
import org.apache.optiq.sql.parser.SqlParser;
import org.apache.optiq.sql.parser.impl.SqlParserImpl;
import org.apache.optiq.sql.type.SqlTypeFactoryImpl;
import org.apache.optiq.sql.validate.*;
import org.apache.optiq.test.MockCatalogReader;
import org.apache.optiq.test.MockSqlOperatorTable;

import org.apache.avatica.Casing;
import org.apache.avatica.Quoting;

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
          "quoting", (Object) Quoting.DOUBLE_QUOTE,
          "quotedCasing", Casing.UNCHANGED,
          "unquotedCasing", Casing.TO_UPPER,
          "caseSensitive", true,
          "conformance", SqlConformance.DEFAULT);

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
    Quoting quoting = (Quoting) factory.get("quoting");
    Casing quotedCasing = (Casing) factory.get("quotedCasing");
    Casing unquotedCasing = (Casing) factory.get("unquotedCasing");
    return SqlParser.create(SqlParserImpl.FACTORY, sql, quoting,
        unquotedCasing, quotedCasing);
  }

  public SqlValidator getValidator(SqlTestFactory factory) {
    final SqlOperatorTable operatorTable = factory.createOperatorTable();
    final boolean caseSensitive = (Boolean) factory.get("caseSensitive");
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl();
    return SqlValidatorUtil.newValidator(operatorTable,
        new MockCatalogReader(typeFactory, caseSensitive).init(),
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
