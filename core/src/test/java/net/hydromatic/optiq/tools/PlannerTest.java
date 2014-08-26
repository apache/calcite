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
package net.hydromatic.optiq.tools;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.config.Lex;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.impl.jdbc.*;
import net.hydromatic.optiq.impl.jdbc.JdbcRules.JdbcProjectRel;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.JavaRules;
import net.hydromatic.optiq.rules.java.JavaRules.EnumerableProjectRel;
import net.hydromatic.optiq.test.OptiqAssert;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.ChainedSqlOperatorTable;
import org.eigenbase.sql.util.ListSqlOperatorTable;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.util.Util;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link Planner}.
 */
public class PlannerTest {
  private void checkParseAndConvert(String query,
    String queryFromParseTree, String expectedRelExpr) throws Exception {
    Planner planner = getPlanner(null);
    SqlNode parse = planner.parse(query);
    assertThat(Util.toLinux(parse.toString()), equalTo(queryFromParseTree));

    SqlNode validate = planner.validate(parse);
    RelNode rel = planner.convert(validate);
    assertThat(toString(rel), equalTo(expectedRelExpr));
  }

  @Test public void testParseAndConvert() throws Exception {
    checkParseAndConvert(
        "select * from \"emps\" where \"name\" like '%e%'",

        "SELECT *\n"
        + "FROM `emps`\n"
        + "WHERE `name` LIKE '%e%'",

        "ProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "  FilterRel(condition=[LIKE($2, '%e%')])\n"
        + "    EnumerableTableAccessRel(table=[[hr, emps]])\n");
  }

  /** Unit test that parses, validates and converts the query using
   * order by and offset. */
  @Test public void testParseAndConvertWithOrderByAndOffset() throws Exception {
    checkParseAndConvert(
        "select * from \"emps\" "
        + "order by \"emps\".\"deptno\" offset 10",

        "SELECT *\n"
        + "FROM `emps`\n"
        + "ORDER BY `emps`.`deptno`\n"
        + "OFFSET 10 ROWS",

        "SortRel(sort0=[$1], dir0=[ASC], offset=[10])\n"
        + "  ProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "    EnumerableTableAccessRel(table=[[hr, emps]])\n");
  }

  private String toString(RelNode rel) {
    return Util.toLinux(
        RelOptUtil.dumpPlan("", rel, false,
            SqlExplainLevel.DIGEST_ATTRIBUTES));
  }

  @Test public void testParseFails() throws SqlParseException {
    Planner planner = getPlanner(null);
    try {
      SqlNode parse =
          planner.parse("select * * from \"emps\"");
      fail("expected error, got " + parse);
    } catch (SqlParseException e) {
      assertThat(e.getMessage(),
          containsString("Encountered \"*\" at line 1, column 10."));
    }
  }

  @Test public void testValidateFails() throws SqlParseException {
    Planner planner = getPlanner(null);
    SqlNode parse =
        planner.parse("select * from \"emps\" where \"Xname\" like '%e%'");
    assertThat(Util.toLinux(parse.toString()), equalTo(
        "SELECT *\n"
        + "FROM `emps`\n"
        + "WHERE `Xname` LIKE '%e%'"));

    try {
      SqlNode validate = planner.validate(parse);
      fail("expected error, got " + validate);
    } catch (ValidationException e) {
      assertThat(Util.getStackTrace(e),
          containsString("Column 'Xname' not found in any table"));
      // ok
    }
  }

  @Test public void testValidateUserDefinedAggregate() throws Exception {
    final SqlStdOperatorTable stdOpTab = SqlStdOperatorTable.instance();
    SqlOperatorTable opTab = new ChainedSqlOperatorTable(
        ImmutableList.of(stdOpTab,
            new ListSqlOperatorTable(
                ImmutableList.<SqlOperator>of(new MyCountAggFunction()))));
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(
            OptiqAssert.addSchema(rootSchema, OptiqAssert.SchemaSpec.HR))
        .operatorTable(opTab)
        .build();
    final Planner planner = Frameworks.getPlanner(config);
    SqlNode parse =
        planner.parse("select \"deptno\", my_count(\"empid\") from \"emps\"\n"
            + "group by \"deptno\"");
    assertThat(Util.toLinux(parse.toString()),
        equalTo(
            "SELECT `deptno`, `MY_COUNT`(`empid`)\n"
            + "FROM `emps`\n"
            + "GROUP BY `deptno`"));

    // MY_COUNT is recognized as an aggregate function, and therefore it is OK
    // that its argument empid is not in the GROUP BY clause.
    SqlNode validate = planner.validate(parse);
    assertThat(validate, notNullValue());

    // The presence of an aggregate function in the SELECT clause causes it
    // to become an aggregate query. Non-aggregate expressions become illegal.
    planner.close();
    planner.reset();
    parse = planner.parse("select \"deptno\", count(1) from \"emps\"");
    try {
      validate = planner.validate(parse);
      fail("expected exception, got " + validate);
    } catch (ValidationException e) {
      assertThat(e.getCause().getCause().getMessage(),
          containsString("Expression 'deptno' is not being grouped"));
    }
  }

  private Planner getPlanner(List<RelTraitDef> traitDefs, Program... programs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .lex(Lex.ORACLE)
        .defaultSchema(
            OptiqAssert.addSchema(rootSchema, OptiqAssert.SchemaSpec.HR))
        .traitDefs(traitDefs)
        .programs(programs)
        .build();
    return Frameworks.getPlanner(config);
  }

  /** Tests that planner throws an error if you pass to
   * {@link Planner#convert(org.eigenbase.sql.SqlNode)}
   * a {@link org.eigenbase.sql.SqlNode} that has been parsed but not
   * validated. */
  @Test public void testConvertWithoutValidateFails() throws Exception {
    Planner planner = getPlanner(null);
    SqlNode parse = planner.parse("select * from \"emps\"");
    try {
      RelNode rel = planner.convert(parse);
      fail("expected error, got " + rel);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString(
          "cannot move from STATE_3_PARSED to STATE_4_VALIDATED"));
    }
  }

  /** Unit test that parses, validates, converts and plans. */
  @Test public void testPlan() throws Exception {
    Program program =
        Programs.ofRules(
            MergeFilterRule.INSTANCE,
            JavaRules.ENUMERABLE_FILTER_RULE,
            JavaRules.ENUMERABLE_PROJECT_RULE);
    Planner planner = getPlanner(null, program);
    SqlNode parse = planner.parse("select * from \"emps\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), equalTo(
        "EnumerableProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "  EnumerableTableAccessRel(table=[[hr, emps]])\n"));
  }

  /** Unit test that parses, validates, converts and
   * plans for query using order by */
  @Test public void testSortPlan() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            RemoveSortRule.INSTANCE,
            JavaRules.ENUMERABLE_PROJECT_RULE,
            JavaRules.ENUMERABLE_SORT_RULE);
    Planner planner = getPlanner(null, Programs.of(ruleSet));
    SqlNode parse = planner.parse(
        "select * from \"emps\" "
            + "order by \"emps\".\"deptno\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = convert.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), equalTo(
        "EnumerableSortRel(sort0=[$1], dir0=[ASC])\n"
        + "  EnumerableProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "    EnumerableTableAccessRel(table=[[hr, emps]])\n"));
  }

  /** Unit test that parses, validates, converts and
   * plans for query using two duplicate order by.
   * The duplicate order by should be removed by RemoveSortRule*/
  @Test public void testDuplicateSortPlan() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            RemoveSortRule.INSTANCE,
            JavaRules.ENUMERABLE_PROJECT_RULE,
            JavaRules.ENUMERABLE_SORT_RULE);
    Planner planner = getPlanner(null, Programs.of(ruleSet));
    SqlNode parse = planner.parse(
        "select \"empid\" from ( "
         + "select * "
         + "from \"emps\" "
         + "order by \"emps\".\"deptno\") "
         + "order by \"deptno\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), equalTo(
        "EnumerableProjectRel(empid=[$0])\n"
        + "  EnumerableProjectRel(empid=[$0], deptno=[$1])\n"
        + "    EnumerableSortRel(sort0=[$1], dir0=[ASC])\n"
        + "      EnumerableProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "        EnumerableTableAccessRel(table=[[hr, emps]])\n"));
  }

  /** Unit test that parses, validates, converts and
   * plans for query using two duplicate order by.*/
  @Test public void testDuplicateSortPlanWORemoveSortRule() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            JavaRules.ENUMERABLE_PROJECT_RULE,
            JavaRules.ENUMERABLE_SORT_RULE);
    Planner planner = getPlanner(null, Programs.of(ruleSet));
    SqlNode parse = planner.parse(
        "select \"empid\" from ( "
            + "select * "
            + "from \"emps\" "
            + "order by \"emps\".\"deptno\") "
            + "order by \"deptno\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), equalTo(
        "EnumerableProjectRel(empid=[$0])\n"
        + "  EnumerableSortRel(sort0=[$1], dir0=[ASC])\n"
        + "    EnumerableProjectRel(empid=[$0], deptno=[$1])\n"
        + "      EnumerableSortRel(sort0=[$1], dir0=[ASC])\n"
        + "        EnumerableProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "          EnumerableTableAccessRel(table=[[hr, emps]])\n"));
  }

  /** Unit test that parses, validates, converts and plans. Planner is
   * provided with a list of RelTraitDefs to register. */
  @Test public void testPlanWithExplicitTraitDefs() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            MergeFilterRule.INSTANCE,
            JavaRules.ENUMERABLE_FILTER_RULE,
            JavaRules.ENUMERABLE_PROJECT_RULE);
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);

    Planner planner = getPlanner(traitDefs, Programs.of(ruleSet));

    SqlNode parse = planner.parse("select * from \"emps\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), equalTo(
        "EnumerableProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "  EnumerableTableAccessRel(table=[[hr, emps]])\n"));
  }

  /** Unit test that calls {@link Planner#transform} twice. */
  @Test public void testPlanTransformTwice() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            MergeFilterRule.INSTANCE,
            JavaRules.ENUMERABLE_FILTER_RULE,
            JavaRules.ENUMERABLE_PROJECT_RULE);
    Planner planner = getPlanner(null, Programs.of(ruleSet));
    SqlNode parse = planner.parse("select * from \"emps\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    RelNode transform2 = planner.transform(0, traitSet, transform);
    assertThat(toString(transform2), equalTo(
        "EnumerableProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "  EnumerableTableAccessRel(table=[[hr, emps]])\n"));
  }

  /** Tests that Hive dialect does not generate "AS". */
  @Test public void testHiveDialect() throws SqlParseException {
    Planner planner = getPlanner(null);
    SqlNode parse = planner.parse(
        "select * from (select * from \"emps\") as t\n"
        + "where \"name\" like '%e%'");
    final SqlDialect hiveDialect =
        new SqlDialect(SqlDialect.DatabaseProduct.HIVE, "Hive", null);
    assertThat(Util.toLinux(parse.toSqlString(hiveDialect).getSql()),
        equalTo("SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM emps) T\n"
            + "WHERE name LIKE '%e%'"));
  }

  /** Unit test that calls {@link Planner#transform} twice,
   * with different rule sets, with different conventions.
   *
   * <p>{@link net.hydromatic.optiq.impl.jdbc.JdbcConvention} is different
   * from the typical convention in that it is not a singleton. Switching to
   * a different instance causes problems unless planner state is wiped clean
   * between calls to {@link Planner#transform}. */
  @Test public void testPlanTransformWithDiffRuleSetAndConvention()
      throws Exception {
    Program program0 =
        Programs.ofRules(
            MergeFilterRule.INSTANCE,
            JavaRules.ENUMERABLE_FILTER_RULE,
            JavaRules.ENUMERABLE_PROJECT_RULE);

    JdbcConvention out = new JdbcConvention(null, null, "myjdbc");
    Program program1 = Programs.ofRules(
        new MockJdbcProjectRule(out), new MockJdbcTableRule(out));

    Planner planner = getPlanner(null, program0, program1);
    SqlNode parse = planner.parse("select T1.\"name\" from \"emps\" as T1 ");

    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);

    RelTraitSet traitSet0 = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);

    RelTraitSet traitSet1 = planner.getEmptyTraitSet()
        .replace(out);

    RelNode transform = planner.transform(0, traitSet0, convert);
    RelNode transform2 = planner.transform(1, traitSet1, transform);
    assertThat(toString(transform2), equalTo(
        "JdbcProjectRel(name=[$2])\n"
        + "  MockJdbcTableScan(table=[[hr, emps]])\n"));
  }

  /** Unit test that plans a query with a large number of joins. */
  @Test public void testPlanNWayJoin()
      throws Exception {
    // Here the times before and after enabling LoptOptimizeJoinRule.
    //
    // Note the jump between N=6 and N=7; LoptOptimizeJoinRule is disabled if
    // there are fewer than 6 joins (7 relations).
    //
    //       N    Before     After
    //         time (ms) time (ms)
    // ======= ========= =========
    //       5                 382
    //       6                 790
    //       7                  26
    //       9    6,000         39
    //      10    9,000         47
    //      11   19,000         67
    //      12   40,000         63
    //      13 OOM              96
    //      35 OOM           1,716
    //      60 OOM          12,230
    checkJoinNWay(5); // LoptOptimizeJoinRule disabled; takes about .4s
    checkJoinNWay(9); // LoptOptimizeJoinRule enabled; takes about 0.04s
    checkJoinNWay(35); // takes about 2s
    if (OptiqAssert.ENABLE_SLOW) {
      checkJoinNWay(60); // takes about 15s
    }
  }

  private void checkJoinNWay(int n) throws Exception {
    final StringBuilder buf = new StringBuilder();
    buf.append("select *");
    for (int i = 0; i < n; i++) {
      buf.append(i == 0 ? "\nfrom " : ",\n ")
          .append("\"depts\" as d").append(i);
    }
    for (int i = 1; i < n; i++) {
      buf.append(i == 1 ? "\nwhere" : "\nand").append(" d")
          .append(i).append(".\"deptno\" = d")
          .append(i - 1).append(".\"deptno\"");
    }
    Planner planner =
        getPlanner(null, Programs.heuristicJoinOrder(Programs.RULE_SET, false));
    SqlNode parse = planner.parse(buf.toString());

    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), containsString(
        "EnumerableJoinRel(condition=[=($0, $3)], joinType=[inner])"));
  }

  /** Plans a 3-table join query on the FoodMart schema. The ideal plan is not
   * bushy, but nevertheless exercises the bushy-join heuristic optimizer. */
  @Test public void testAlmostBushy() throws Exception {
    checkBushy("select *\n"
        + "from \"sales_fact_1997\" as s\n"
        + "  join \"customer\" as c using (\"customer_id\")\n"
        + "  join \"product\" as p using (\"product_id\")\n"
        + "where c.\"city\" = 'San Francisco'\n"
        + "and p.\"brand_name\" = 'Washington'",
        "EnumerableProjectRel(product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], customer_id0=[$8], account_num=[$9], lname=[$10], fname=[$11], mi=[$12], address1=[$13], address2=[$14], address3=[$15], address4=[$16], city=[$17], state_province=[$18], postal_code=[$19], country=[$20], customer_region_id=[$21], phone1=[$22], phone2=[$23], birthdate=[$24], marital_status=[$25], yearly_income=[$26], gender=[$27], total_children=[$28], num_children_at_home=[$29], education=[$30], date_accnt_opened=[$31], member_card=[$32], occupation=[$33], houseowner=[$34], num_cars_owned=[$35], fullname=[$36], product_class_id=[$37], product_id0=[$38], brand_name=[$39], product_name=[$40], SKU=[$41], SRP=[$42], gross_weight=[$43], net_weight=[$44], recyclable_package=[$45], low_fat=[$46], units_per_case=[$47], cases_per_pallet=[$48], shelf_width=[$49], shelf_height=[$50], shelf_depth=[$51])\n"
        + "  EnumerableProjectRel($f0=[$44], $f1=[$45], $f2=[$46], $f3=[$47], $f4=[$48], $f5=[$49], $f6=[$50], $f7=[$51], $f8=[$15], $f9=[$16], $f10=[$17], $f11=[$18], $f12=[$19], $f13=[$20], $f14=[$21], $f15=[$22], $f16=[$23], $f17=[$24], $f18=[$25], $f19=[$26], $f20=[$27], $f21=[$28], $f22=[$29], $f23=[$30], $f24=[$31], $f25=[$32], $f26=[$33], $f27=[$34], $f28=[$35], $f29=[$36], $f30=[$37], $f31=[$38], $f32=[$39], $f33=[$40], $f34=[$41], $f35=[$42], $f36=[$43], $f37=[$0], $f38=[$1], $f39=[$2], $f40=[$3], $f41=[$4], $f42=[$5], $f43=[$6], $f44=[$7], $f45=[$8], $f46=[$9], $f47=[$10], $f48=[$11], $f49=[$12], $f50=[$13], $f51=[$14])\n"
        + "    EnumerableJoinRel(condition=[=($1, $44)], joinType=[inner])\n"
        + "      EnumerableFilterRel(condition=[=($2, 'Washington')])\n"
        + "        EnumerableTableAccessRel(table=[[foodmart2, product]])\n"
        + "      EnumerableJoinRel(condition=[=($0, $31)], joinType=[inner])\n"
        + "        EnumerableFilterRel(condition=[=($9, 'San Francisco')])\n"
        + "          EnumerableTableAccessRel(table=[[foodmart2, customer]])\n"
        + "        EnumerableTableAccessRel(table=[[foodmart2, sales_fact_1997]])\n");
  }

  /** Plans a 4-table join query on the FoodMart schema.
   *
   * <p>The ideal plan is bushy:
   *   customer x (product_class x  product x sales)
   * which would be written
   *   (customer x ((product_class x product) x sales))
   * if you don't assume 'x' is left-associative. */
  @Test public void testBushy() throws Exception {
    checkBushy("select *\n"
        + "from \"sales_fact_1997\" as s\n"
        + "  join \"customer\" as c using (\"customer_id\")\n"
        + "  join \"product\" as p using (\"product_id\")\n"
        + "  join \"product_class\" as pc using (\"product_class_id\")\n"
        + "where c.\"city\" = 'San Francisco'\n"
        + "and p.\"brand_name\" = 'Washington'",
        "EnumerableProjectRel(product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], customer_id0=[$8], account_num=[$9], lname=[$10], fname=[$11], mi=[$12], address1=[$13], address2=[$14], address3=[$15], address4=[$16], city=[$17], state_province=[$18], postal_code=[$19], country=[$20], customer_region_id=[$21], phone1=[$22], phone2=[$23], birthdate=[$24], marital_status=[$25], yearly_income=[$26], gender=[$27], total_children=[$28], num_children_at_home=[$29], education=[$30], date_accnt_opened=[$31], member_card=[$32], occupation=[$33], houseowner=[$34], num_cars_owned=[$35], fullname=[$36], product_class_id=[$37], product_id0=[$38], brand_name=[$39], product_name=[$40], SKU=[$41], SRP=[$42], gross_weight=[$43], net_weight=[$44], recyclable_package=[$45], low_fat=[$46], units_per_case=[$47], cases_per_pallet=[$48], shelf_width=[$49], shelf_height=[$50], shelf_depth=[$51], product_class_id0=[$52], product_subcategory=[$53], product_category=[$54], product_department=[$55], product_family=[$56])\n"
        + "  EnumerableProjectRel($f0=[$49], $f1=[$50], $f2=[$51], $f3=[$52], $f4=[$53], $f5=[$54], $f6=[$55], $f7=[$56], $f8=[$0], $f9=[$1], $f10=[$2], $f11=[$3], $f12=[$4], $f13=[$5], $f14=[$6], $f15=[$7], $f16=[$8], $f17=[$9], $f18=[$10], $f19=[$11], $f20=[$12], $f21=[$13], $f22=[$14], $f23=[$15], $f24=[$16], $f25=[$17], $f26=[$18], $f27=[$19], $f28=[$20], $f29=[$21], $f30=[$22], $f31=[$23], $f32=[$24], $f33=[$25], $f34=[$26], $f35=[$27], $f36=[$28], $f37=[$34], $f38=[$35], $f39=[$36], $f40=[$37], $f41=[$38], $f42=[$39], $f43=[$40], $f44=[$41], $f45=[$42], $f46=[$43], $f47=[$44], $f48=[$45], $f49=[$46], $f50=[$47], $f51=[$48], $f52=[$29], $f53=[$30], $f54=[$31], $f55=[$32], $f56=[$33])\n"
        + "    EnumerableJoinRel(condition=[=($0, $51)], joinType=[inner])\n"
        + "      EnumerableFilterRel(condition=[=($9, 'San Francisco')])\n"
        + "        EnumerableTableAccessRel(table=[[foodmart2, customer]])\n"
        + "      EnumerableJoinRel(condition=[=($6, $20)], joinType=[inner])\n"
        + "        EnumerableJoinRel(condition=[=($0, $5)], joinType=[inner])\n"
        + "          EnumerableTableAccessRel(table=[[foodmart2, product_class]])\n"
        + "          EnumerableFilterRel(condition=[=($2, 'Washington')])\n"
        + "            EnumerableTableAccessRel(table=[[foodmart2, product]])\n"
        + "        EnumerableTableAccessRel(table=[[foodmart2, sales_fact_1997]])\n");
  }

  /** Plans a 5-table join query on the FoodMart schema. The ideal plan is
   * bushy: store x (customer x (product_class x product x sales)). */
  @Test public void testBushy5() throws Exception {
    checkBushy("select *\n"
        + "from \"sales_fact_1997\" as s\n"
        + "  join \"customer\" as c using (\"customer_id\")\n"
        + "  join \"product\" as p using (\"product_id\")\n"
        + "  join \"product_class\" as pc using (\"product_class_id\")\n"
        + "  join \"store\" as st using (\"store_id\")\n"
        + "where c.\"city\" = 'San Francisco'\n",
        "EnumerableProjectRel(product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], customer_id0=[$8], account_num=[$9], lname=[$10], fname=[$11], mi=[$12], address1=[$13], address2=[$14], address3=[$15], address4=[$16], city=[$17], state_province=[$18], postal_code=[$19], country=[$20], customer_region_id=[$21], phone1=[$22], phone2=[$23], birthdate=[$24], marital_status=[$25], yearly_income=[$26], gender=[$27], total_children=[$28], num_children_at_home=[$29], education=[$30], date_accnt_opened=[$31], member_card=[$32], occupation=[$33], houseowner=[$34], num_cars_owned=[$35], fullname=[$36], product_class_id=[$37], product_id0=[$38], brand_name=[$39], product_name=[$40], SKU=[$41], SRP=[$42], gross_weight=[$43], net_weight=[$44], recyclable_package=[$45], low_fat=[$46], units_per_case=[$47], cases_per_pallet=[$48], shelf_width=[$49], shelf_height=[$50], shelf_depth=[$51], product_class_id0=[$52], product_subcategory=[$53], product_category=[$54], product_department=[$55], product_family=[$56], store_id0=[$57], store_type=[$58], region_id=[$59], store_name=[$60], store_number=[$61], store_street_address=[$62], store_city=[$63], store_state=[$64], store_postal_code=[$65], store_country=[$66], store_manager=[$67], store_phone=[$68], store_fax=[$69], first_opened_date=[$70], last_remodel_date=[$71], store_sqft=[$72], grocery_sqft=[$73], frozen_sqft=[$74], meat_sqft=[$75], coffee_bar=[$76], video_store=[$77], salad_bar=[$78], prepared_food=[$79], florist=[$80])\n"
        + "  EnumerableProjectRel($f0=[$73], $f1=[$74], $f2=[$75], $f3=[$76], $f4=[$77], $f5=[$78], $f6=[$79], $f7=[$80], $f8=[$24], $f9=[$25], $f10=[$26], $f11=[$27], $f12=[$28], $f13=[$29], $f14=[$30], $f15=[$31], $f16=[$32], $f17=[$33], $f18=[$34], $f19=[$35], $f20=[$36], $f21=[$37], $f22=[$38], $f23=[$39], $f24=[$40], $f25=[$41], $f26=[$42], $f27=[$43], $f28=[$44], $f29=[$45], $f30=[$46], $f31=[$47], $f32=[$48], $f33=[$49], $f34=[$50], $f35=[$51], $f36=[$52], $f37=[$58], $f38=[$59], $f39=[$60], $f40=[$61], $f41=[$62], $f42=[$63], $f43=[$64], $f44=[$65], $f45=[$66], $f46=[$67], $f47=[$68], $f48=[$69], $f49=[$70], $f50=[$71], $f51=[$72], $f52=[$53], $f53=[$54], $f54=[$55], $f55=[$56], $f56=[$57], $f57=[$0], $f58=[$1], $f59=[$2], $f60=[$3], $f61=[$4], $f62=[$5], $f63=[$6], $f64=[$7], $f65=[$8], $f66=[$9], $f67=[$10], $f68=[$11], $f69=[$12], $f70=[$13], $f71=[$14], $f72=[$15], $f73=[$16], $f74=[$17], $f75=[$18], $f76=[$19], $f77=[$20], $f78=[$21], $f79=[$22], $f80=[$23])\n"
        + "    EnumerableJoinRel(condition=[=($0, $77)], joinType=[inner])\n"
        + "      EnumerableTableAccessRel(table=[[foodmart2, store]])\n"
        + "      EnumerableJoinRel(condition=[=($0, $51)], joinType=[inner])\n"
        + "        EnumerableFilterRel(condition=[=($9, 'San Francisco')])\n"
        + "          EnumerableTableAccessRel(table=[[foodmart2, customer]])\n"
        + "        EnumerableJoinRel(condition=[=($6, $20)], joinType=[inner])\n"
        + "          EnumerableJoinRel(condition=[=($0, $5)], joinType=[inner])\n"
        + "            EnumerableTableAccessRel(table=[[foodmart2, product_class]])\n"
        + "            EnumerableTableAccessRel(table=[[foodmart2, product]])\n"
        + "          EnumerableTableAccessRel(table=[[foodmart2, sales_fact_1997]])\n");
  }

  /** Tests the bushy join algorithm where one table does not join to
   * anything. */
  @Test public void testBushyCrossJoin() throws Exception {
    checkBushy("select * from \"sales_fact_1997\"\n"
        + "join \"customer\" using (\"customer_id\")\n"
        + "cross join \"department\"",
        "EnumerableProjectRel(product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], customer_id0=[$8], account_num=[$9], lname=[$10], fname=[$11], mi=[$12], address1=[$13], address2=[$14], address3=[$15], address4=[$16], city=[$17], state_province=[$18], postal_code=[$19], country=[$20], customer_region_id=[$21], phone1=[$22], phone2=[$23], birthdate=[$24], marital_status=[$25], yearly_income=[$26], gender=[$27], total_children=[$28], num_children_at_home=[$29], education=[$30], date_accnt_opened=[$31], member_card=[$32], occupation=[$33], houseowner=[$34], num_cars_owned=[$35], fullname=[$36], department_id=[$37], department_description=[$38])\n"
        + "  EnumerableProjectRel($f0=[$31], $f1=[$32], $f2=[$33], $f3=[$34], $f4=[$35], $f5=[$36], $f6=[$37], $f7=[$38], $f8=[$2], $f9=[$3], $f10=[$4], $f11=[$5], $f12=[$6], $f13=[$7], $f14=[$8], $f15=[$9], $f16=[$10], $f17=[$11], $f18=[$12], $f19=[$13], $f20=[$14], $f21=[$15], $f22=[$16], $f23=[$17], $f24=[$18], $f25=[$19], $f26=[$20], $f27=[$21], $f28=[$22], $f29=[$23], $f30=[$24], $f31=[$25], $f32=[$26], $f33=[$27], $f34=[$28], $f35=[$29], $f36=[$30], $f37=[$0], $f38=[$1])\n"
        + "    EnumerableJoinRel(condition=[true], joinType=[inner])\n"
        + "      EnumerableTableAccessRel(table=[[foodmart2, department]])\n"
        + "      EnumerableJoinRel(condition=[=($0, $31)], joinType=[inner])\n"
        + "        EnumerableTableAccessRel(table=[[foodmart2, customer]])\n"
        + "        EnumerableTableAccessRel(table=[[foodmart2, sales_fact_1997]])");
  }

  /** Tests the bushy join algorithm against a query where not all tables have a
   * join condition to the others. */
  @Test public void testBushyCrossJoin2() throws Exception {
    checkBushy("select * from \"sales_fact_1997\"\n"
        + "join \"customer\" using (\"customer_id\")\n"
        + "cross join \"department\"\n"
        + "join \"employee\" using (\"department_id\")",
        "EnumerableProjectRel(product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], customer_id0=[$8], account_num=[$9], lname=[$10], fname=[$11], mi=[$12], address1=[$13], address2=[$14], address3=[$15], address4=[$16], city=[$17], state_province=[$18], postal_code=[$19], country=[$20], customer_region_id=[$21], phone1=[$22], phone2=[$23], birthdate=[$24], marital_status=[$25], yearly_income=[$26], gender=[$27], total_children=[$28], num_children_at_home=[$29], education=[$30], date_accnt_opened=[$31], member_card=[$32], occupation=[$33], houseowner=[$34], num_cars_owned=[$35], fullname=[$36], department_id=[$37], department_description=[$38], employee_id=[$39], full_name=[$40], first_name=[$41], last_name=[$42], position_id=[$43], position_title=[$44], store_id0=[$45], department_id0=[$46], birth_date=[$47], hire_date=[$48], end_date=[$49], salary=[$50], supervisor_id=[$51], education_level=[$52], marital_status0=[$53], gender0=[$54], management_role=[$55])\n"
        + "  EnumerableProjectRel($f0=[$48], $f1=[$49], $f2=[$50], $f3=[$51], $f4=[$52], $f5=[$53], $f6=[$54], $f7=[$55], $f8=[$19], $f9=[$20], $f10=[$21], $f11=[$22], $f12=[$23], $f13=[$24], $f14=[$25], $f15=[$26], $f16=[$27], $f17=[$28], $f18=[$29], $f19=[$30], $f20=[$31], $f21=[$32], $f22=[$33], $f23=[$34], $f24=[$35], $f25=[$36], $f26=[$37], $f27=[$38], $f28=[$39], $f29=[$40], $f30=[$41], $f31=[$42], $f32=[$43], $f33=[$44], $f34=[$45], $f35=[$46], $f36=[$47], $f37=[$0], $f38=[$1], $f39=[$2], $f40=[$3], $f41=[$4], $f42=[$5], $f43=[$6], $f44=[$7], $f45=[$8], $f46=[$9], $f47=[$10], $f48=[$11], $f49=[$12], $f50=[$13], $f51=[$14], $f52=[$15], $f53=[$16], $f54=[$17], $f55=[$18])\n"
        + "    EnumerableJoinRel(condition=[true], joinType=[inner])\n"
        + "      EnumerableJoinRel(condition=[=($0, $9)], joinType=[inner])\n"
        + "        EnumerableTableAccessRel(table=[[foodmart2, department]])\n"
        + "        EnumerableTableAccessRel(table=[[foodmart2, employee]])\n"
        + "      EnumerableJoinRel(condition=[=($0, $31)], joinType=[inner])\n"
        + "        EnumerableTableAccessRel(table=[[foodmart2, customer]])\n"
        + "        EnumerableTableAccessRel(table=[[foodmart2, sales_fact_1997]])\n");
  }

  /** Checks that a query returns a particular plan, using a planner with
   * OptimizeBushyJoinRule enabled. */
  private void checkBushy(String sql, String expected) throws Exception {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .lex(Lex.ORACLE)
        .defaultSchema(
            OptiqAssert.addSchema(rootSchema,
                OptiqAssert.SchemaSpec.CLONE_FOODMART))
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true))
        .build();
    Planner planner = Frameworks.getPlanner(config);
    SqlNode parse = planner.parse(sql);

    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), containsString(expected));
  }

  /**
   * Rule to convert a {@link EnumerableProjectRel} to an
   * {@link JdbcProjectRel}.
   */
  private class MockJdbcProjectRule extends ConverterRule {
    private MockJdbcProjectRule(JdbcConvention out) {
      super(EnumerableProjectRel.class, EnumerableConvention.INSTANCE, out,
          "MockJdbcProjectRule");
    }

    public RelNode convert(RelNode rel) {
      final EnumerableProjectRel project = (EnumerableProjectRel) rel;

      return new JdbcProjectRel(
          rel.getCluster(),
          rel.getTraitSet().replace(getOutConvention()),
          convert(project.getChild(),
              project.getChild().getTraitSet().replace(getOutConvention())),
          project.getProjects(),
          project.getRowType(),
          ProjectRelBase.Flags.BOXED);
    }
  }

  /**
   * Rule to convert a {@link JavaRules.EnumerableTableAccessRel} to an
   * {@link MockJdbcTableScan}.
   */
  private class MockJdbcTableRule extends ConverterRule {
    private MockJdbcTableRule(JdbcConvention out) {
      super(JavaRules.EnumerableTableAccessRel.class,
          EnumerableConvention.INSTANCE, out, "MockJdbcTableRule");
    }

    public RelNode convert(RelNode rel) {
      final JavaRules.EnumerableTableAccessRel scan =
          (JavaRules.EnumerableTableAccessRel) rel;
      return new MockJdbcTableScan(scan.getCluster(),
          scan.getTable(),
          (JdbcConvention) getOutConvention());
    }
  }

  /**
   * Relational expression representing a "mock" scan of a table in a
   * JDBC data source.
   */
  private class MockJdbcTableScan extends TableAccessRelBase
      implements JdbcRel {

    public MockJdbcTableScan(RelOptCluster cluster, RelOptTable table,
        JdbcConvention jdbcConvention) {
      super(cluster, cluster.traitSetOf(jdbcConvention), table);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new MockJdbcTableScan(getCluster(), table,
          (JdbcConvention) getConvention());
    }

    @Override public void register(RelOptPlanner planner) {
      final JdbcConvention out = (JdbcConvention) getConvention();
      for (RelOptRule rule : JdbcRules.rules(out)) {
        planner.addRule(rule);
      }
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return null;
    }
  }

  /**
   * Test to determine whether de-correlation correctly removes CorrelatorRel.
   */
  @Test public void testOldJoinStyleDeCorrelation() throws Exception {
    assertFalse(
        checkTpchQuery("select\n p.`pPartkey`\n"
            + "from\n"
            + "  `tpch`.`part` p,\n"
            + "  `tpch`.`partsupp` ps1\n"
            + "where\n"
            + "  p.`pPartkey` = ps1.`psPartkey`\n"
            + "  and ps1.`psSupplyCost` = (\n"
            + "    select\n"
            + "      min(ps.`psSupplyCost`)\n"
            + "    from\n"
            + "      `tpch`.`partsupp` ps\n"
            + "    where\n"
            + "      p.`pPartkey` = ps.`psPartkey`\n"
            + "  )\n")
            .contains("CorrelatorRel"));
  }

  public String checkTpchQuery(String tpchTestQuery) throws Exception {
    final SchemaPlus schema =
        Frameworks.createRootSchema(true).add("tpch",
            new ReflectiveSchema(new TpchSchema()));

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .lex(Lex.MYSQL)
        .defaultSchema(schema)
        .programs(Programs.ofRules(Programs.RULE_SET))
        .build();
    Planner p = Frameworks.getPlanner(config);
    SqlNode n = p.parse(tpchTestQuery);
    n = p.validate(n);
    RelNode r = p.convert(n);
    String plan = RelOptUtil.toString(r);
    p.close();
    return plan;
  }

  /** User-defined aggregate function. */
  public static class MyCountAggFunction extends SqlAggFunction {
    public MyCountAggFunction() {
      super("MY_COUNT", SqlKind.OTHER_FUNCTION, ReturnTypes.BIGINT, null,
          OperandTypes.ANY, SqlFunctionCategory.NUMERIC);
    }

    public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
      return ImmutableList.of(typeFactory.createSqlType(SqlTypeName.ANY));
    }

    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
      return typeFactory.createSqlType(SqlTypeName.BIGINT);
    }

    public RelDataType deriveType(SqlValidator validator,
        SqlValidatorScope scope, SqlCall call) {
      // Check for COUNT(*) function.  If it is we don't
      // want to try and derive the "*"
      if (call.isCountStar()) {
        return validator.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
      }
      return super.deriveType(validator, scope, call);
    }
  }
}

// End PlannerTest.java
