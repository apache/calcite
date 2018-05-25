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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.MockCatalogReader;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;

/**
 * Customed implementation of {@link SqlTestFactory}.
 */
public class CustomedSqlTestFactory extends DefaultSqlTestFactory {
  public static final ImmutableMap<String, Object> DEFAULT_OPTIONS =
      ImmutableMap.<String, Object>builder()
          .put("quoting", Quoting.DOUBLE_QUOTE)
          .put("quotedCasing", Casing.UNCHANGED)
          .put("unquotedCasing", Casing.TO_UPPER)
          .put("caseSensitive", true)
          .put("conformance", SqlConformanceEnum.PRAGMATIC_2003)
          .put("operatorTable", SqlStdOperatorTable.instance())
          .put("connectionFactory",
              CalciteAssert.EMPTY_CONNECTION_FACTORY
                  .with(
                      new CalciteAssert.AddSchemaSpecPostProcessor(
                          CalciteAssert.SchemaSpec.HR)))
          .build();

  /** Caches the mock catalog.
   * Due to view parsing, initializing a mock catalog is quite expensive.
   * Validator is not re-entrant, so we create a new one for each test.
   * Caching improves SqlValidatorTest from 23s to 8s,
   * and CalciteSqlOperatorTest from 65s to 43s. */
  private final LoadingCache<SqlTestFactory, Xyz> cache =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<SqlTestFactory, Xyz>() {
                public Xyz load(@Nonnull SqlTestFactory factory)
                    throws Exception {
                  final SqlOperatorTable operatorTable =
                      factory.createOperatorTable(factory);
                  final JavaTypeFactory typeFactory =
                      new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT,
                          (SqlConformance) DEFAULT_OPTIONS.get("conformance"));
                  final MockCatalogReader catalogReader =
                      factory.createCatalogReader(factory, typeFactory);
                  return new Xyz(operatorTable, typeFactory, catalogReader);
                }
              });

  public static final CustomedSqlTestFactory INSTANCE =
      new CustomedSqlTestFactory();

  private CustomedSqlTestFactory() {
    super();
  }

  public SqlValidator getValidator(SqlTestFactory factory) {
    final Xyz xyz = cache.getUnchecked(factory);
    final SqlConformance conformance =
        (SqlConformance) factory.get("conformance");
    return SqlValidatorUtil.newValidator(xyz.getOperatorTable(),
        xyz.getCatalogReader(), xyz.getTypeFactory(), conformance);
  }

  public Object get(String name) {
    return DEFAULT_OPTIONS.get(name);
  }
}

// End CustomedSqlTestFactory.java
