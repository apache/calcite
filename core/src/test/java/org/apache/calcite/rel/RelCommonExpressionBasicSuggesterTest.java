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
 * Tests for {@link RelCommonExpressionBasicSuggester}.
 * The tests ensure that the suggester can correctly identify common expressions in the query plan
 * that have a certain structure.
 */
public class RelCommonExpressionBasicSuggesterTest {

  @Test void testSuggestReturnsCommonFilterTableScan() {
    String sql = "SELECT '2025' FROM emp WHERE empno = 1\n"
        + "UNION\n"
        + "SELECT '2024' FROM emp WHERE empno = 1";
    checkSuggestions(sql);
  }

  @Test void testSuggestReturnsTwoCommonFilterTableScans() {
    String sql = "SELECT '2025' FROM emp WHERE empno = 1\n"
        + "UNION\n"
        + "SELECT '2024' FROM emp WHERE empno = 1\n"
        + "UNION\n"
        + "SELECT '2023' FROM emp WHERE empno = 2\n"
        + "UNION\n"
        + "SELECT '2022' FROM emp WHERE empno = 2";
    checkSuggestions(sql);
  }

  @Test void testSuggestReturnsCommonProjectFilterTableScan() {
    String sql = "SELECT ename FROM emp WHERE empno = 1\n"
        + "UNION\n"
        + "SELECT ename FROM emp WHERE empno = 1";
    checkSuggestions(sql);
  }

  @Test void testSuggestReturnsCommonAggregateProjectTableScan() {
    String sql = "SELECT COUNT(*), 'A' FROM emp GROUP BY ename\n"
        + "UNION\n"
        + "SELECT COUNT(*), 'B' FROM emp GROUP BY ename";
    checkSuggestions(sql);
  }

  @Test void testSuggestReturnsCommonAggregateProjectFilterTableScan() {
    String sql = "SELECT COUNT(*), 'A' FROM emp WHERE empno > 50 GROUP BY ename\n"
        + "UNION\n"
        + "SELECT COUNT(*), 'B' FROM emp WHERE empno > 50 GROUP BY ename";
    checkSuggestions(sql);
  }

  @Test void testSuggestReturnsCommonProjectJoinTableScan() {
    String sql = "WITH cx AS (\n"
        + "SELECT e.empno, d.dname FROM emp e\n"
        + "INNER JOIN dept d ON e.deptno = d.deptno)\n"
        + "SELECT * FROM cx WHERE cx.empno > 50\n"
        + "INTERSECT\n"
        + "SELECT * FROM cx WHERE cx.empno < 100";
    checkSuggestions(sql);
  }

  @Test void testSuggestReturnsCommonProjectFilterJoinTableScan() {
    String sql = "WITH cx AS (\n"
        + "SELECT e.empno, d.dname FROM emp e\n"
        + "INNER JOIN dept d ON e.deptno = d.deptno\n"
        + "WHERE d.dname = 'Sales')\n"
        + "SELECT * FROM cx WHERE cx.empno > 50\n"
        + "INTERSECT\n"
        + "SELECT * FROM cx WHERE cx.empno < 100";
    checkSuggestions(sql);
  }

  @Test void testSuggestReturnsCommonAggregateProjectFilterJoinTableScan() {
    String sql = "WITH cx AS (\n"
        + "SELECT ename, COUNT(*) as cnt FROM emp e\n"
        + "INNER JOIN dept d ON e.deptno = d.deptno\n"
        + "WHERE d.dname = 'Sales' GROUP BY ename)\n"
        + "SELECT * FROM cx WHERE cnt > 50\n"
        + "INTERSECT\n"
        + "SELECT * FROM cx WHERE cnt < 100";
    checkSuggestions(sql);
  }

  private void checkSuggestions(String sql) {
    DiffRepository diffRepo = DiffRepository.lookup(RelCommonExpressionBasicSuggesterTest.class);
    RelNode rel = toRel(sql);
    RelCommonExpressionSuggester suggester = new RelCommonExpressionBasicSuggester();
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
