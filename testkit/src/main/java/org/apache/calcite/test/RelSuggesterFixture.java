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

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCommonExpressionSuggester;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * A fixture for testing implementations of the {@link RelCommonExpressionSuggester} API.
 */
public class RelSuggesterFixture {

  /** Creates the default fixture for a given test class and suggester implementation. */
  public static RelSuggesterFixture of(Class<?> clazz, RelCommonExpressionSuggester suggester) {
    return new RelSuggesterFixture("?", suggester, DiffRepository.lookup(clazz), defaultSchema());
  }

  private final String sql;
  private final RelCommonExpressionSuggester suggester;
  private final DiffRepository diffRepo;
  private final SchemaPlus schema;

  private RelSuggesterFixture(String sql, RelCommonExpressionSuggester suggester,
      DiffRepository diffRepo, SchemaPlus schema) {
    this.sql = requireNonNull(sql, "sql");
    this.suggester = requireNonNull(suggester, "suggester");
    this.diffRepo = requireNonNull(diffRepo, "diffRepo");
    this.schema = requireNonNull(schema, "schema");
  }

  /** Creates a copy of this fixture that uses a given SQL query. */
  public RelSuggesterFixture withSql(String sql) {
    return new RelSuggesterFixture(sql, suggester, diffRepo, schema);
  }

  /**
   * Checks that the suggester returns the expected plans for the specified SQL statement.
   *
   * <p>The expected suggestions are defined in a reference file handled by the provided
   * DiffRepository.
   */
  public void checkSuggestions() {
    RelNode rel = toRel(sql);
    AtomicInteger i = new AtomicInteger();
    suggester.suggest(rel, null).stream().map(RelOptUtil::toString).sorted().forEach(plan -> {
      String tag = "suggestion_" + i.getAndIncrement();
      diffRepo.assertEquals(tag, "${" + tag + "}", plan);
    });
  }

  /** Checks that the actual and reference file are consistent. */
  public void checkActualAndReferenceFiles() {
    diffRepo.checkActualAndReferenceFiles();
  }

  private RelNode toRel(String sql) {
    Planner planner =
        Frameworks.getPlanner(Frameworks.newConfigBuilder().defaultSchema(schema).build());
    try {
      return planner.rel(planner.validate(planner.parse(sql))).rel;
    } catch (SqlParseException | RelConversionException | ValidationException e) {
      throw new RuntimeException(e);
    }
  }

  private static SchemaPlus defaultSchema() {
    SchemaPlus schema = CalciteSchema.createRootSchema(false, false).plus();
    schema.add("EMP", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("EMPNO", SqlTypeName.INTEGER)
            .add("ENAME", SqlTypeName.VARCHAR)
            .add("DEPTNO", SqlTypeName.INTEGER)
            .build();
      }
    });
    schema.add("DEPT", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("DEPTNO", SqlTypeName.INTEGER)
            .add("DNAME", SqlTypeName.VARCHAR)
            .add("EMPNO", SqlTypeName.INTEGER)
            .build();
      }
    });
    return schema;
  }
}
