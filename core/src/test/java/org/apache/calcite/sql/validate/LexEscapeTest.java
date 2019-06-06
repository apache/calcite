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
package org.apache.calcite.sql.validate;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Testing {@link SqlValidator} and {@link Lex} quoting.
 */
public class LexEscapeTest {

  private static Planner getPlanner(List<RelTraitDef> traitDefs,
      Config parserConfig, Program... programs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("TMP", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(
            ImmutableList.of(typeFactory.createSqlType(SqlTypeName.VARCHAR),
                typeFactory.createSqlType(SqlTypeName.INTEGER)),
            ImmutableList.of("localtime", "current_timestamp"));
      }
    });
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(rootSchema)
        .traitDefs(traitDefs)
        .programs(programs)
        .operatorTable(SqlStdOperatorTable.instance())
        .build();
    return Frameworks.getPlanner(config);
  }

  private static void runProjectQueryWithLex(Lex lex, String sql)
      throws SqlParseException, ValidationException, RelConversionException {
    Config javaLex = SqlParser.configBuilder().setLex(lex).build();
    Planner planner = getPlanner(null, javaLex, Programs.ofRules(Programs.RULE_SET));
    SqlNode parse = planner.parse(sql);
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).rel;
    RelTraitSet traitSet =
        planner.getEmptyTraitSet().replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(transform, instanceOf(EnumerableProject.class));
    List<RelDataTypeField> fields = transform.getRowType().getFieldList();
    // Get field type from sql text and validate we parsed it after validation.
    assertThat(fields.size(), is(4));
    assertThat(fields.get(0).getType().getSqlTypeName(), is(SqlTypeName.VARCHAR));
    assertThat(fields.get(1).getType().getSqlTypeName(), is(SqlTypeName.TIME));
    assertThat(fields.get(2).getType().getSqlTypeName(), is(SqlTypeName.INTEGER));
    assertThat(fields.get(3).getType().getSqlTypeName(), is(SqlTypeName.TIMESTAMP));
  }

  @Test public void testCalciteEscapeOracle()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select \"localtime\", localtime, "
        + "\"current_timestamp\", current_timestamp from TMP";
    runProjectQueryWithLex(Lex.ORACLE, sql);
  }

  @Test public void testCalciteEscapeMySql()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select `localtime`, localtime, `current_timestamp`, current_timestamp from TMP";
    runProjectQueryWithLex(Lex.MYSQL, sql);
  }

  @Test public void testCalciteEscapeMySqlAnsi()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select \"localtime\", localtime, "
        + "\"current_timestamp\", current_timestamp from TMP";
    runProjectQueryWithLex(Lex.MYSQL_ANSI, sql);
  }

  @Test public void testCalciteEscapeSqlServer()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select [localtime], localtime, [current_timestamp], current_timestamp from TMP";
    runProjectQueryWithLex(Lex.SQL_SERVER, sql);
  }

  @Test public void testCalciteEscapeJava()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select `localtime`, localtime, `current_timestamp`, current_timestamp from TMP";
    runProjectQueryWithLex(Lex.JAVA, sql);
  }
}

// End LexEscapeTest.java
