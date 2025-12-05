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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;
import java.util.function.Function;

/**
 * The source of a {@link RelNode} for running a test.
 */
interface RelSupplier {
  RelNode apply(RelOptFixture fixture);
  RelNode apply2(RelMetadataFixture metadataFixture);


  RelSupplier NONE = new RelSupplier() {
    @Override public RelNode apply(RelOptFixture fixture) {
      throw new UnsupportedOperationException();
    }

    @Override public RelNode apply2(RelMetadataFixture metadataFixture) {
      throw new UnsupportedOperationException();
    }
  };

  static RelSupplier of(String sql) {
    if (sql.contains(" \n")) {
      throw new AssertionError("trailing whitespace");
    }
    return new SqlRelSupplier(sql);
  }

  /**
   * RelBuilder config based on the "scott" schema.
   */
  FrameworkConfig FRAMEWORK_CONFIG =
      Frameworks.newConfigBuilder()
          .parserConfig(SqlParser.Config.DEFAULT)
          .defaultSchema(
              CalciteAssert.addSchema(
                  Frameworks.createRootSchema(true),
                  CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL))
          .traitDefs((List<RelTraitDef>) null)
          .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
          .build();

  static RelSupplier of(Function<RelBuilder, RelNode> relFn) {
    return new FnRelSupplier(relFn);
  }

  /** Creates a RelNode by parsing SQL. */
  class SqlRelSupplier implements RelSupplier {
    private final String sql;

    private SqlRelSupplier(String sql) {
      this.sql = sql;
    }

    @Override public String toString() {
      return sql;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof SqlRelSupplier
          && ((SqlRelSupplier) o).sql.equals(this.sql);
    }

    @Override public int hashCode() {
      return 3709 + sql.hashCode();
    }

    @Override public RelNode apply(RelOptFixture fixture) {
      String sql2 = fixture.diffRepos().expand("sql", sql);
      return fixture.tester
          .convertSqlToRel(fixture.factory, sql2, fixture.decorrelate,
              fixture.factory.sqlToRelConfig.isTrimUnusedFields())
          .project();
    }

    @Override public RelNode apply2(RelMetadataFixture metadataFixture) {
      return metadataFixture.sqlToRel(sql);
    }
  }

  /** Creates a RelNode by passing a lambda to a {@link RelBuilder}. */
  class FnRelSupplier implements RelSupplier {
    private final Function<RelBuilder, RelNode> relFn;

    private FnRelSupplier(Function<RelBuilder, RelNode> relFn) {
      this.relFn = relFn;
    }

    @Override public String toString() {
      return "<relFn>";
    }

    @Override public int hashCode() {
      return relFn.hashCode();
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof FnRelSupplier
          && ((FnRelSupplier) o).relFn == relFn;
    }

    @Override public RelNode apply(RelOptFixture fixture) {
      if (fixture.planner instanceof VolcanoPlanner) {
        RelOptCluster existingCluster = fixture.factory.createSqlToRelConverter().getCluster();
        return relFn.apply(RelBuilder.create(FRAMEWORK_CONFIG, existingCluster));
      }
      return relFn.apply(RelBuilder.create(FRAMEWORK_CONFIG));
    }

    @Override public RelNode apply2(RelMetadataFixture metadataFixture) {
      return relFn.apply(RelBuilder.create(FRAMEWORK_CONFIG));
    }
  }
}
