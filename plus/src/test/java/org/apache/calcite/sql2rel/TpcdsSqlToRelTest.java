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
package org.apache.calcite.sql2rel;

import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Unit tests for SQL-to-Rel conversion of TPC-DS queries.
 */
class TpcdsSqlToRelTest {

  private static final RelDataTypeSystem TYPE_SYSTEM = new RelDataTypeSystemImpl() {
  };

  private static final CalciteCatalogReader TPCDS_CATALOG =
      schemaToCatalog("tpcds", new TpcdsSchema(1.0), TYPE_SYSTEM);

  static IntStream testCases() {
    return IntStream.rangeClosed(1, 99);
  }

  @ParameterizedTest
  @MethodSource("testCases")
  void testQuery(int query) throws IOException, SqlParseException {
    String sql = getResourceAsString(inputSqlFile(query));
    List<SqlNode> sqlNodes = parseSqlStatements(sql);
    convertSqlToRel(sqlNodes);
  }

  private static CalciteCatalogReader schemaToCatalog(String schemaName, Schema schema,
      RelDataTypeSystem typeSystem) {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    rootSchema.add(schemaName, schema);
    List<String> defaultSchema = Collections.singletonList(schemaName);
    return new CalciteCatalogReader(
        rootSchema,
        defaultSchema,
        new JavaTypeFactoryImpl(typeSystem),
        CalciteConnectionConfig.DEFAULT.set(
            CalciteConnectionProperty.CASE_SENSITIVE, Boolean.FALSE.toString()));
  }

  private String inputSqlFile(int query) {
    return String.format(Locale.getDefault(), "sql/tpcds/%02d.sql", query);
  }

  private String getResourceAsString(String name) throws IOException {
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(name);
         Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
         BufferedReader bufferedReader = new BufferedReader(reader)) {
      return bufferedReader.lines().collect(Collectors.joining("\n"));
    }
  }

  private List<SqlNode> parseSqlStatements(String sql) throws SqlParseException {
    SqlParser.Config config = SqlParser.config()
        .withUnquotedCasing(Casing.TO_UPPER)
        .withConformance(SqlConformanceEnum.LENIENT)
        .withParserFactory(SqlParserImpl.FACTORY);
    SqlParser parser = SqlParser.create(sql, config);
    return parser.parseStmtList();
  }

  private List<RelRoot> convertSqlToRel(List<SqlNode> sqlNodes) {
    SqlToRelConverter converter = createSqlToRelConverter();
    return sqlNodes.stream()
        .map(sqlNode -> converter.convertQuery(sqlNode, true, true))
        .collect(Collectors.toList());
  }

  private SqlToRelConverter createSqlToRelConverter() {
    RelOptTable.ViewExpander viewExpander = null;
    SqlValidator validator = new TpcdsSqlValidator();
    RelOptCluster cluster = newDefaultRelOptCluster();

    return new SqlToRelConverter(viewExpander, validator, TPCDS_CATALOG, cluster,
        StandardConvertletTable.INSTANCE, SqlToRelConverter.CONFIG);
  }

  RelOptCluster newDefaultRelOptCluster() {
    RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl(TYPE_SYSTEM));
    HepProgram program = HepProgram.builder().build();
    RelOptPlanner emptyPlanner = new HepPlanner(program);
    return RelOptCluster.create(emptyPlanner, rexBuilder);
  }

  /**
   * Minimal SqlValidator implementation.
   */
  private static final class TpcdsSqlValidator extends SqlValidatorImpl {

    TpcdsSqlValidator() {
      super(SqlStdOperatorTable.instance(), TPCDS_CATALOG,
          TPCDS_CATALOG.getTypeFactory(), Config.DEFAULT);
    }
  }
}
