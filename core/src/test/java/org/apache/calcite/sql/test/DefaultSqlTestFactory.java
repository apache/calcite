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
package org.apache.calcite.sql.test;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.MockCatalogReader;
import org.apache.calcite.test.MockSqlOperatorTable;

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
      ImmutableMap.<String, Object>builder()
          .put("quoting", Quoting.DOUBLE_QUOTE)
          .put("quotedCasing", Casing.UNCHANGED)
          .put("unquotedCasing", Casing.TO_UPPER)
          .put("caseSensitive", true)
          .put("conformance", SqlConformanceEnum.DEFAULT)
          .put("operatorTable", SqlStdOperatorTable.instance())
          .put("connectionFactory",
              CalciteAssert.EMPTY_CONNECTION_FACTORY
                  .with(
                      new CalciteAssert.AddSchemaSpecPostProcessor(
                          CalciteAssert.SchemaSpec.HR)))
          .build();

  public static final DefaultSqlTestFactory INSTANCE =
      new DefaultSqlTestFactory();

  private DefaultSqlTestFactory() {
  }

  public SqlOperatorTable createOperatorTable(SqlTestFactory factory) {
    final SqlOperatorTable opTab0 =
        (SqlOperatorTable) factory.get("operatorTable");
    MockSqlOperatorTable opTab = new MockSqlOperatorTable(opTab0);
    MockSqlOperatorTable.addRamp(opTab);
    return opTab;
  }

  public SqlParser createParser(SqlTestFactory factory, String sql) {
    return SqlParser.create(sql,
        SqlParser.configBuilder()
            .setQuoting((Quoting) factory.get("quoting"))
            .setUnquotedCasing((Casing) factory.get("unquotedCasing"))
            .setQuotedCasing((Casing) factory.get("quotedCasing"))
            .setConformance((SqlConformance) factory.get("conformance"))
            .build());
  }

  public SqlValidator getValidator(SqlTestFactory factory) {
    final SqlOperatorTable operatorTable = factory.createOperatorTable(factory);
    final boolean caseSensitive = (Boolean) factory.get("caseSensitive");
    final SqlConformance conformance =
        (SqlConformance) factory.get("conformance");
    final JavaTypeFactory typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    return SqlValidatorUtil.newValidator(operatorTable,
        new MockCatalogReader(typeFactory, caseSensitive).init(),
        typeFactory, conformance);
  }

  public SqlAdvisor createAdvisor(SqlValidatorWithHints validator) {
    throw new UnsupportedOperationException();
  }

  public Object get(String name) {
    return DEFAULT_OPTIONS.get(name);
  }
}

// End DefaultSqlTestFactory.java
