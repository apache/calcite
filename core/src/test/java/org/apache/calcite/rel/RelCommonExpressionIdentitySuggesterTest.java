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
package org.apache.calcite.rel;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.DiffRepository;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import org.junit.jupiter.api.Test;

import java.util.Collection;

import static java.util.stream.Collectors.joining;

/**
 * Tests for {@link RelCommonExpressionIdentitySuggester}.
 * The tests ensure that the suggester can correctly identify common expressions in the query plan
 * that have a certain structure.
 */
public class RelCommonExpressionIdentitySuggesterTest {

  @Test void testSingleFilterScanSuggestion() {
    checkSuggestions();
  }

  @Test void testTwoFilterScanSuggestions() {
    checkSuggestions();
  }

  @Test void testSingleProjectFilterScanSuggestion() {
    checkSuggestions();
  }

  @Test void testSingleAggregateProjectScanSuggestion() {
    checkSuggestions();
  }

  @Test void testSingleAggregateProjectFilterScanSuggestion() {
    checkSuggestions();
  }

  @Test void testSingleProjectJoinScanSuggestion() {
    checkSuggestions();
  }

  @Test void testSingleProjectFilterJoinScanSuggestion() {
    checkSuggestions();
  }

  @Test void testSingleAggregateProjectFilterJoinScanSuggestion() {
    checkSuggestions();
  }

  private void checkSuggestions() {
    DiffRepository diffRepo = DiffRepository.lookup(RelCommonExpressionIdentitySuggesterTest.class);
    RelNode rel = toRel(diffRepo.expand("sql", "${sql}"));
    diffRepo.assertEquals("plan", "${plan}", RelOptUtil.toString(rel));
    RelCommonExpressionSuggester suggester = new RelCommonExpressionIdentitySuggester();
    Collection<RelNode> output = suggester.suggest(rel, null);
    String result = output.stream().map(RelOptUtil::toString).sorted().collect(joining("\n"));
    diffRepo.assertEquals("suggestions", "${suggestions}", result);
  }

  private static RelNode toRel(String sql) {
    Planner planner =
        Frameworks.getPlanner(Frameworks.newConfigBuilder().defaultSchema(schema()).build());
    try {
      return planner.rel(planner.validate(planner.parse(sql))).rel;
    } catch (SqlParseException | RelConversionException | ValidationException e) {
      throw new RuntimeException(e);
    }
  }

  private static SchemaPlus schema() {
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
