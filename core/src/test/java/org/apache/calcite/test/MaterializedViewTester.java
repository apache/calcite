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

import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Abstract base class for testing materialized views.
 *
 * @see MaterializedViewFixture
 */
public abstract class MaterializedViewTester {
  /** Customizes materialization matching approach. */
  protected abstract List<RelNode> optimize(RelNode queryRel,
      List<RelOptMaterialization> materializationList);

  /** Checks that a given query can use a materialized view with a given
   * definition. */
  void checkMaterialize(MaterializedViewFixture f) {
    final TestConfig testConfig = build(f);
    final Predicate<String> checker =
        Util.first(f.checker,
            s -> MaterializedViewFixture.resultContains(s,
                "EnumerableTableScan(table=[["
                    + testConfig.defaultSchema + ", MV0]]"));
    final List<RelNode> substitutes =
        optimize(testConfig.queryRel, testConfig.materializationList);
    if (substitutes.stream()
        .noneMatch(sub -> checker.test(RelOptUtil.toString(sub)))) {
      StringBuilder substituteMessages = new StringBuilder();
      for (RelNode sub : substitutes) {
        substituteMessages.append(RelOptUtil.toString(sub)).append("\n");
      }
      throw new AssertionError("Materialized view failed to be matched by optimized results:\n"
          + substituteMessages);
    }
  }

  /** Checks that a given query cannot use a materialized view with a given
   * definition. */
  void checkNoMaterialize(MaterializedViewFixture f) {
    final TestConfig testConfig = build(f);
    final List<RelNode> results =
        optimize(testConfig.queryRel, testConfig.materializationList);
    if (results.isEmpty()
        || (results.size() == 1
        && !RelOptUtil.toString(results.get(0)).contains("MV0"))) {
      return;
    }
    final StringBuilder errMsgBuilder = new StringBuilder();
    errMsgBuilder.append("Optimization succeeds out of expectation: ");
    for (RelNode res : results) {
      errMsgBuilder.append(RelOptUtil.toString(res)).append("\n");
    }
    throw new AssertionError(errMsgBuilder.toString());
  }

  private TestConfig build(MaterializedViewFixture f) {
    return Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
      cluster.getPlanner().setExecutor(new RexExecutorImpl(DataContexts.EMPTY));
      try {
        final SchemaPlus defaultSchema;
        if (f.schemaSpec == null) {
          defaultSchema =
              rootSchema.add("hr",
                  new ReflectiveSchemaWithoutRowCount(new MaterializationTest.HrFKUKSchema()));
        } else {
          defaultSchema = CalciteAssert.addSchema(rootSchema, f.schemaSpec);
        }
        final RelNode queryRel = toRel(cluster, rootSchema, defaultSchema, f.query);
        final List<RelOptMaterialization> mvs = new ArrayList<>();
        final RelBuilder relBuilder =
            RelFactories.LOGICAL_BUILDER.create(cluster, relOptSchema);
        final MaterializationService.DefaultTableFactory tableFactory =
            new MaterializationService.DefaultTableFactory();
        for (Pair<String, String> pair : f.materializationList) {
          String sql = pair.left;
          final RelNode mvRel = toRel(cluster, rootSchema, defaultSchema, sql);
          final Table table =
              tableFactory.createTable(CalciteSchema.from(rootSchema),
                  sql, ImmutableList.of(defaultSchema.getName()));
          String name = pair.right;
          defaultSchema.add(name, table);
          relBuilder.scan(defaultSchema.getName(), name);
          final LogicalTableScan logicalScan = (LogicalTableScan) relBuilder.build();
          final EnumerableTableScan replacement =
              EnumerableTableScan.create(cluster, logicalScan.getTable());
          mvs.add(
              new RelOptMaterialization(replacement, mvRel, null,
                  ImmutableList.of(defaultSchema.getName(), name)));
        }
        return new TestConfig(defaultSchema.getName(), queryRel, mvs);
      } catch (Exception e) {
        throw TestUtil.rethrow(e);
      }
    });
  }

  private RelNode toRel(RelOptCluster cluster, SchemaPlus rootSchema,
      SchemaPlus defaultSchema, String sql) throws SqlParseException {
    final SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
    final SqlNode parsed = parser.parseStmt();

    final CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(CalciteSchema.from(rootSchema),
            CalciteSchema.from(defaultSchema).path(null),
            new JavaTypeFactoryImpl(),
            CalciteConnectionConfig.DEFAULT);

    final SqlValidator validator =
        SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
            catalogReader, new JavaTypeFactoryImpl(),
            SqlValidator.Config.DEFAULT);
    final SqlNode validated = validator.validate(parsed);
    final SqlToRelConverter.Config config = SqlToRelConverter.config()
        .withTrimUnusedFields(true)
        .withExpand(true)
        .withDecorrelationEnabled(true);
    final SqlToRelConverter converter =
        new SqlToRelConverter(
            (rowType, queryString, schemaPath, viewPath) -> {
              throw new UnsupportedOperationException("cannot expand view");
            },
            validator, catalogReader, cluster, StandardConvertletTable.INSTANCE,
            config);
    return converter.convertQuery(validated, false, true).rel;
  }

  /**
   * Processed testing definition.
   */
  private static class TestConfig {
    final String defaultSchema;
    final RelNode queryRel;
    final List<RelOptMaterialization> materializationList;

    TestConfig(String defaultSchema, RelNode queryRel,
        List<RelOptMaterialization> materializationList) {
      this.defaultSchema = defaultSchema;
      this.queryRel = queryRel;
      this.materializationList = materializationList;
    }
  }
}
