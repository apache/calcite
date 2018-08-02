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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.DelegatingTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.MockSqlOperatorTable;
import org.apache.calcite.test.catalog.MockCatalogReader;
import org.apache.calcite.test.catalog.MockCatalogReaderSimple;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

import java.util.Map;
import java.util.Objects;

/**
 * Default implementation of {@link SqlTestFactory}.
 *
 * <p>Suitable for most tests. If you want different behavior, you can extend;
 * if you want a factory with different properties (e.g. SQL conformance level
 * or identifier quoting), use {@link #with(String, Object)} to create a new factory.</p>
*/
public class SqlTestFactory {
  public static final ImmutableMap<String, Object> DEFAULT_OPTIONS =
      ImmutableSortedMap.<String, Object>naturalOrder()
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

  public static final SqlTestFactory INSTANCE =
      new SqlTestFactory();

  private final ImmutableMap<String, Object> options;
  private final MockCatalogReaderFactory catalogReaderFactory;
  private final ValidatorFactory validatorFactory;

  private final RelDataTypeFactory typeFactory;
  private final SqlOperatorTable operatorTable;
  private final SqlValidatorCatalogReader catalogReader;
  private final SqlParser.Config parserConfig;

  protected SqlTestFactory() {
    this(DEFAULT_OPTIONS, MockCatalogReaderSimple::new, SqlValidatorUtil::newValidator);
  }

  protected SqlTestFactory(ImmutableMap<String, Object> options,
      MockCatalogReaderFactory catalogReaderFactory,
      ValidatorFactory validatorFactory) {
    this.options = options;
    this.catalogReaderFactory = catalogReaderFactory;
    this.validatorFactory = validatorFactory;
    this.operatorTable = createOperatorTable((SqlOperatorTable) options.get("operatorTable"));
    this.typeFactory = createTypeFactory((SqlConformance) options.get("conformance"));
    Boolean caseSensitive = (Boolean) options.get("caseSensitive");
    this.catalogReader = catalogReaderFactory.create(typeFactory, caseSensitive).init();
    this.parserConfig = createParserConfig(options);
  }

  public static MockCatalogReader createCatalogReader(
      RelDataTypeFactory typeFactory,
      boolean caseSensitive) {
    return new MockCatalogReaderSimple(typeFactory, caseSensitive).init();
  }

  private static SqlOperatorTable createOperatorTable(SqlOperatorTable opTab0) {
    MockSqlOperatorTable opTab = new MockSqlOperatorTable(opTab0);
    MockSqlOperatorTable.addRamp(opTab);
    return opTab;
  }

  public SqlParser createParser(String sql) {
    return SqlParser.create(sql, parserConfig);
  }

  public static SqlParser.Config createParserConfig(ImmutableMap<String, Object> options) {
    return SqlParser.configBuilder()
        .setQuoting((Quoting) options.get("quoting"))
        .setUnquotedCasing((Casing) options.get("unquotedCasing"))
        .setQuotedCasing((Casing) options.get("quotedCasing"))
        .setConformance((SqlConformance) options.get("conformance"))
        .build();
  }

  public SqlValidator getValidator() {
    final SqlConformance conformance =
        (SqlConformance) options.get("conformance");
    return validatorFactory.create(operatorTable, catalogReader, typeFactory, conformance);
  }

  public SqlAdvisor createAdvisor() {
    SqlValidator validator = getValidator();
    if (validator instanceof SqlValidatorWithHints) {
      return new SqlAdvisor((SqlValidatorWithHints) validator);
    }
    throw new UnsupportedOperationException(
        "Validator should implement SqlValidatorWithHints, actual validator is " + validator);
  }

  public SqlTestFactory with(String name, Object value) {
    if (Objects.equals(value, options.get(name))) {
      return this;
    }
    ImmutableMap.Builder<String, Object> builder = ImmutableSortedMap.naturalOrder();
    // Protect from IllegalArgumentException: Multiple entries with same key
    for (Map.Entry<String, Object> entry : options.entrySet()) {
      if (name.equals(entry.getKey())) {
        continue;
      }
      builder.put(entry);
    }
    builder.put(name, value);
    return new SqlTestFactory(builder.build(), catalogReaderFactory, validatorFactory);
  }

  public SqlTestFactory withCatalogReader(MockCatalogReaderFactory newCatalogReaderFactory) {
    return new SqlTestFactory(options, newCatalogReaderFactory, validatorFactory);
  }

  public SqlTestFactory withValidator(ValidatorFactory newValidatorFactory) {
    return new SqlTestFactory(options, catalogReaderFactory, newValidatorFactory);
  }

  public final Object get(String name) {
    return options.get(name);
  }

  private static RelDataTypeFactory createTypeFactory(SqlConformance conformance) {
    RelDataTypeSystem typeSystem = RelDataTypeSystem.DEFAULT;
    if (conformance.shouldConvertRaggedUnionTypesToVarying()) {
      typeSystem = new DelegatingTypeSystem(typeSystem) {
        public boolean shouldConvertRaggedUnionTypesToVarying() {
          return true;
        }
      };
    }
    return new JavaTypeFactoryImpl(typeSystem);
  }

  /**
   * Creates {@link SqlValidator} for tests.
   */
  public interface ValidatorFactory {
    SqlValidator create(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        SqlConformance conformance);
  }

  /**
   * Creates {@link MockCatalogReader} for tests.
   * Note: {@link MockCatalogReader#init()} is to be invoked later, so a typical implementation
   * should be via constructor reference like {@code MockCatalogReaderSimple::new}.
   */
  public interface MockCatalogReaderFactory {
    MockCatalogReader create(RelDataTypeFactory typeFactory, boolean caseSensitive);
  }
}

// End SqlTestFactory.java
