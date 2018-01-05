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

import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.CoerceInputsRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * HepPlannerTest is a unit test for {@link HepPlanner}. See
 * {@link RelOptRulesTest} for an explanation of how to add tests; the tests in
 * this class are targeted at exercising the planner, and use specific rules for
 * convenience only, whereas the tests in that class are targeted at exercising
 * specific rules, and use the planner for convenience only. Hence the split.
 */
public class HepPlannerTest extends RelOptTestBase {
  //~ Static fields/initializers ---------------------------------------------

  private static final String UNION_TREE =
      "(select name from dept union select ename from emp)"
      + " union (select ename from bonus)";

  private static final String COMPLEX_UNION_TREE = "select * from (\n"
      + "  select ENAME, 50011895 as cat_id, '1' as cat_name, 1 as require_free_postage, 0 as require_15return, 0 as require_48hour,1 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50011895 union all\n"
      + "  select ENAME, 50013023 as cat_id, '2' as cat_name, 0 as require_free_postage, 0 as require_15return, 0 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013023 union all\n"
      + "  select ENAME, 50013032 as cat_id, '3' as cat_name, 0 as require_free_postage, 0 as require_15return, 0 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013032 union all\n"
      + "  select ENAME, 50013024 as cat_id, '4' as cat_name, 0 as require_free_postage, 0 as require_15return, 0 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013024 union all\n"
      + "  select ENAME, 50004204 as cat_id, '5' as cat_name, 0 as require_free_postage, 0 as require_15return, 0 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50004204 union all\n"
      + "  select ENAME, 50013043 as cat_id, '6' as cat_name, 0 as require_free_postage, 0 as require_15return, 0 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013043 union all\n"
      + "  select ENAME, 290903 as cat_id, '7' as cat_name, 1 as require_free_postage, 0 as require_15return, 0 as require_48hour,1 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 290903 union all\n"
      + "  select ENAME, 50008261 as cat_id, '8' as cat_name, 1 as require_free_postage, 0 as require_15return, 0 as require_48hour,1 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50008261 union all\n"
      + "  select ENAME, 124478013 as cat_id, '9' as cat_name, 0 as require_free_postage, 0 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 124478013 union all\n"
      + "  select ENAME, 124472005 as cat_id, '10' as cat_name, 0 as require_free_postage, 0 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 124472005 union all\n"
      + "  select ENAME, 50013475 as cat_id, '11' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013475 union all\n"
      + "  select ENAME, 50018263 as cat_id, '12' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50018263 union all\n"
      + "  select ENAME, 50013498 as cat_id, '13' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013498 union all\n"
      + "  select ENAME, 350511 as cat_id, '14' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 350511 union all\n"
      + "  select ENAME, 50019790 as cat_id, '15' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50019790 union all\n"
      + "  select ENAME, 50015382 as cat_id, '16' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50015382 union all\n"
      + "  select ENAME, 350503 as cat_id, '17' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 350503 union all\n"
      + "  select ENAME, 350401 as cat_id, '18' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 350401 union all\n"
      + "  select ENAME, 50015560 as cat_id, '19' as cat_name, 0 as require_free_postage, 0 as require_15return, 0 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50015560 union all\n"
      + "  select ENAME, 122658003 as cat_id, '20' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 122658003 union all\n"
      + "  select ENAME, 122716008 as cat_id, '21' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 122716008 union all\n"
      + "  select ENAME, 50018406 as cat_id, '22' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50018406 union all\n"
      + "  select ENAME, 50018407 as cat_id, '23' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50018407 union all\n"
      + "  select ENAME, 50024678 as cat_id, '24' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50024678 union all\n"
      + "  select ENAME, 50022290 as cat_id, '25' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022290 union all\n"
      + "  select ENAME, 50020072 as cat_id, '26' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020072 union all\n"
      + "  select ENAME, 50024679 as cat_id, '27' as cat_name, 1 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50024679 union all\n"
      + "  select ENAME, 50013326 as cat_id, '28' as cat_name, 1 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013326 union all\n"
      + "  select ENAME, 50020032 as cat_id, '19' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020032 union all\n"
      + "  select ENAME, 50022273 as cat_id, '30' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022273 union all\n"
      + "  select ENAME, 50013511 as cat_id, '31' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013511 union all\n"
      + "  select ENAME, 122694006 as cat_id, '32' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 122694006 union all\n"
      + "  select ENAME, 50019940 as cat_id, '33' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50019940 union all\n"
      + "  select ENAME, 50022288 as cat_id, '34' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022288 union all\n"
      + "  select ENAME, 50020069 as cat_id, '35' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020069 union all\n"
      + "  select ENAME, 50021800 as cat_id, '36' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50021800 union all\n"
      + "  select ENAME, 50024684 as cat_id, '37' as cat_name, 1 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50024684 union all\n"
      + "  select ENAME, 50024676 as cat_id, '38' as cat_name, 1 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50024676 union all\n"
      + "  select ENAME, 50020070 as cat_id, '39' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020070 union all\n"
      + "  select ENAME, 50020058 as cat_id, '40' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020058 union all\n"
      + "  select ENAME, 50019938 as cat_id, '41' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50019938 union all\n"
      + "  select ENAME, 122686009 as cat_id, '42' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 122686009 union all\n"
      + "  select ENAME, 50022286 as cat_id, '43' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022286 union all\n"
      + "  select ENAME, 122692007 as cat_id, '44' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 122692007 union all\n"
      + "  select ENAME, 50020059 as cat_id, '45' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020059 union all\n"
      + "  select ENAME, 50006050 as cat_id, '45' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50006050 union all\n"
      + "  select ENAME, 122718006 as cat_id, '47' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 122718006 union all\n"
      + "  select ENAME, 50022652 as cat_id, '48' as cat_name, 1 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022652 union all\n"
      + "  select ENAME, 50024685 as cat_id, '49' as cat_name, 1 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50024685 union all\n"
      + "  select ENAME, 50020104 as cat_id, '50' as cat_name, 1 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020104 union all\n"
      + "  select ENAME, 50013500 as cat_id, '51' as cat_name, 1 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013500 union all\n"
      + "  select ENAME, 50003558 as cat_id, '52' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50003558 union all\n"
      + "  select ENAME, 50020061 as cat_id, '53' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020061 union all\n"
      + "  select ENAME, 122656012 as cat_id, '54' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 122656012 union all\n"
      + "  select ENAME, 50024812 as cat_id, '55' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50024812 union all\n"
      + "  select ENAME, 50022287 as cat_id, '56' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022287 union all\n"
      + "  select ENAME, 50020107 as cat_id, '57' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020107 union all\n"
      + "  select ENAME, 50019842 as cat_id, '58' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50019842 union all\n"
      + "  select ENAME, 50020106 as cat_id, '59' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020106 union all\n"
      + "  select ENAME, 50020071 as cat_id, '60' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020071 union all\n"
      + "  select ENAME, 50019939 as cat_id, '61' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50019939 union all\n"
      + "  select ENAME, 50020034 as cat_id, '62' as cat_name, 1 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020034 union all\n"
      + "  select ENAME, 50020025 as cat_id, '63' as cat_name, 1 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020025 union all\n"
      + "  select ENAME, 50022293 as cat_id, '64' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022293 union all\n"
      + "  select ENAME, 50022279 as cat_id, '65' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022279 union all\n"
      + "  select ENAME, 50013818 as cat_id, '66' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013818 union all\n"
      + "  select ENAME, 50020060 as cat_id, '67' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020060 union all\n"
      + "  select ENAME, 50020062 as cat_id, '68' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020062 union all\n"
      + "  select ENAME, 50022276 as cat_id, '69' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022276 union all\n"
      + "  select ENAME, 50022280 as cat_id, '70' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022280 union all\n"
      + "  select ENAME, 50020619 as cat_id, '71' as cat_name, 1 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020619 union all\n"
      + "  select ENAME, 50013347 as cat_id, '72' as cat_name, 1 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013347 union all\n"
      + "  select ENAME, 50008698 as cat_id, '73' as cat_name, 1 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50008698 union all\n"
      + "  select ENAME, 50013334 as cat_id, '74' as cat_name, 1 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013334 union all\n"
      + "  select ENAME, 50024810 as cat_id, '75' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50024810 union all\n"
      + "  select ENAME, 50019936 as cat_id, '76' as cat_name, 1 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50019936 union all\n"
      + "  select ENAME, 50024813 as cat_id, '77' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50024813 union all\n"
      + "  select ENAME, 50020959 as cat_id, '78' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020959 union all\n"
      + "  select ENAME, 124474002 as cat_id, '79' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 124474002 union all\n"
      + "  select ENAME, 50019853 as cat_id, '80' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50019853 union all\n"
      + "  select ENAME, 50019837 as cat_id, '81' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50019837 union all\n"
      + "  select ENAME, 50022289 as cat_id, '82' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022289 union all\n"
      + "  select ENAME, 50022278 as cat_id, '83' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022278 union all\n"
      + "  select ENAME, 50024690 as cat_id, '84' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50024690 union all\n"
      + "  select ENAME, 50592002 as cat_id, '85' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50592002 union all\n"
      + "  select ENAME, 50013342 as cat_id, '86' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013342 union all\n"
      + "  select ENAME, 50022296 as cat_id, '87' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022296 union all\n"
      + "  select ENAME, 123456001 as cat_id, '88' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 123456001 union all\n"
      + "  select ENAME, 50022298 as cat_id, '89' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022298 union all\n"
      + "  select ENAME, 50022274 as cat_id, '90' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022274 union all\n"
      + "  select ENAME, 50006046 as cat_id, '91' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50006046 union all\n"
      + "  select ENAME, 50020676 as cat_id, '92' as cat_name, 1 as require_free_postage, 0 as require_15return, 0 as require_48hour,1 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020676 union all\n"
      + "  select ENAME, 50020678 as cat_id, '93' as cat_name, 1 as require_free_postage, 0 as require_15return, 0 as require_48hour,1 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020678 union all\n"
      + "  select ENAME, 121398012 as cat_id, '94' as cat_name, 1 as require_free_postage, 0 as require_15return, 0 as require_48hour,1 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 121398012 union all\n"
      + "  select ENAME, 50020720 as cat_id, '95' as cat_name, 1 as require_free_postage, 0 as require_15return, 0 as require_48hour,1 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50020720 union all\n"
      + "  select ENAME, 50001714 as cat_id, '96' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50001714 union all\n"
      + "  select ENAME, 50008905 as cat_id, '97' as cat_name, 1 as require_free_postage, 0 as require_15return, 1 as require_48hour,1 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50008905 union all\n"
      + "  select ENAME, 50008904 as cat_id, '98' as cat_name, 1 as require_free_postage, 0 as require_15return, 1 as require_48hour,1 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50008904 union all\n"
      + "  select ENAME, 50022358 as cat_id, '99' as cat_name, 0 as require_free_postage, 0 as require_15return, 0 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022358 union all\n"
      + "  select ENAME, 50022371 as cat_id, '100' as cat_name, 0 as require_free_postage, 0 as require_15return, 0 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022371\n"
      + ") a";

  //~ Methods ----------------------------------------------------------------

  protected DiffRepository getDiffRepos() {
    return DiffRepository.lookup(HepPlannerTest.class);
  }

  @Test public void testRuleClass() throws Exception {
    // Verify that an entire class of rules can be applied.

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleClass(CoerceInputsRule.class);

    HepPlanner planner =
        new HepPlanner(
            programBuilder.build());

    planner.addRule(
        new CoerceInputsRule(LogicalUnion.class, false,
            RelFactories.LOGICAL_BUILDER));
    planner.addRule(
        new CoerceInputsRule(LogicalIntersect.class, false,
            RelFactories.LOGICAL_BUILDER));

    checkPlanning(planner,
        "(select name from dept union select ename from emp)"
            + " intersect (select fname from customer.contact)");
  }

  @Test public void testRuleDescription() throws Exception {
    // Verify that a rule can be applied via its description.

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleByDescription("FilterToCalcRule");

    HepPlanner planner =
        new HepPlanner(
            programBuilder.build());

    planner.addRule(FilterToCalcRule.INSTANCE);

    checkPlanning(
        planner,
        "select name from sales.dept where deptno=12");
  }

  @Test public void testMatchLimitOneTopDown() throws Exception {
    // Verify that only the top union gets rewritten.

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
    programBuilder.addMatchLimit(1);
    programBuilder.addRuleInstance(UnionToDistinctRule.INSTANCE);

    checkPlanning(
        programBuilder.build(), UNION_TREE);
  }

  @Test public void testMatchLimitOneBottomUp() throws Exception {
    // Verify that only the bottom union gets rewritten.

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchLimit(1);
    programBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    programBuilder.addRuleInstance(UnionToDistinctRule.INSTANCE);

    checkPlanning(
        programBuilder.build(), UNION_TREE);
  }

  @Test public void testMatchUntilFixpoint() throws Exception {
    // Verify that both unions get rewritten.

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchLimit(HepProgram.MATCH_UNTIL_FIXPOINT);
    programBuilder.addRuleInstance(UnionToDistinctRule.INSTANCE);

    checkPlanning(
        programBuilder.build(), UNION_TREE);
  }

  @Test public void testReplaceCommonSubexpression() throws Exception {
    // Note that here it may look like the rule is firing
    // twice, but actually it's only firing once on the
    // common sub-expression.  The purpose of this test
    // is to make sure the planner can deal with
    // rewriting something used as a common sub-expression
    // twice by the same parent (the join in this case).

    checkPlanning(
        ProjectRemoveRule.INSTANCE,
        "select d1.deptno from (select * from dept) d1,"
            + " (select * from dept) d2");
  }

  /** Tests that if two relational expressions are equivalent, the planner
   * notices, and only applies the rule once. */
  @Test public void testCommonSubExpression() {
    // In the following,
    //   (select 1 from dept where abs(-1)=20)
    // occurs twice, but it's a common sub-expression, so the rule should only
    // apply once.
    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleInstance(FilterToCalcRule.INSTANCE);

    final HepTestListener listener = new HepTestListener(0);
    HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.addListener(listener);

    final String sql = "(select 1 from dept where abs(-1)=20)\n"
        + "union all\n"
        + "(select 1 from dept where abs(-1)=20)";
    planner.setRoot(tester.convertSqlToRel(sql).rel);
    RelNode bestRel = planner.findBestExp();

    assertThat(bestRel.getInput(0).equals(bestRel.getInput(1)), is(true));
    assertThat(listener.getApplyTimes() == 1, is(true));
  }

  @Test public void testSubprogram() throws Exception {
    // Verify that subprogram gets re-executed until fixpoint.
    // In this case, the first time through we limit it to generate
    // only one calc; the second time through it will generate
    // a second calc, and then merge them.
    HepProgramBuilder subprogramBuilder = HepProgram.builder();
    subprogramBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
    subprogramBuilder.addMatchLimit(1);
    subprogramBuilder.addRuleInstance(ProjectToCalcRule.INSTANCE);
    subprogramBuilder.addRuleInstance(CalcMergeRule.INSTANCE);

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addSubprogram(subprogramBuilder.build());

    checkPlanning(
        programBuilder.build(),
        "select upper(ename) from (select lower(ename) as ename from emp)");
  }

  @Test public void testGroup() throws Exception {
    // Verify simultaneous application of a group of rules.
    // Intentionally add them in the wrong order to make sure
    // that order doesn't matter within the group.
    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addGroupBegin();
    programBuilder.addRuleInstance(CalcMergeRule.INSTANCE);
    programBuilder.addRuleInstance(ProjectToCalcRule.INSTANCE);
    programBuilder.addRuleInstance(FilterToCalcRule.INSTANCE);
    programBuilder.addGroupEnd();

    checkPlanning(
        programBuilder.build(),
        "select upper(name) from dept where deptno=20");
  }

  @Test public void testGC() throws Exception {
    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
    programBuilder.addRuleInstance(CalcMergeRule.INSTANCE);
    programBuilder.addRuleInstance(ProjectToCalcRule.INSTANCE);
    programBuilder.addRuleInstance(FilterToCalcRule.INSTANCE);

    HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.setRoot(
        tester.convertSqlToRel("select upper(name) from dept where deptno=20").rel);
    planner.findBestExp();
    // Reuse of HepPlanner (should trigger GC).
    planner.setRoot(
        tester.convertSqlToRel("select upper(name) from dept where deptno=20").rel);
    planner.findBestExp();
  }

  @Test public void testRuleApplyCount() {
    final long applyTimes1 = checkRuleApplyCount(HepMatchOrder.ARBITRARY);
    assertThat(applyTimes1, is(5451L));

    final long applyTimes2 = checkRuleApplyCount(HepMatchOrder.DEPTH_FIRST);
    assertThat(applyTimes2, is(403L));

    // DEPTH_FIRST has 10x fewer matches than ARBITRARY
    assertThat(applyTimes1 > applyTimes2 * 10, is(true));
  }

  private long checkRuleApplyCount(HepMatchOrder matchOrder) {
    final HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchOrder(matchOrder);
    programBuilder.addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE);
    programBuilder.addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE);

    final HepTestListener listener = new HepTestListener(0);
    HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.addListener(listener);
    planner.setRoot(tester.convertSqlToRel(COMPLEX_UNION_TREE).rel);
    planner.findBestExp();
    return listener.getApplyTimes();
  }

  /** Listener for HepPlannerTest; counts how many times rules fire. */
  private class HepTestListener implements RelOptListener {
    private long applyTimes;

    HepTestListener(long applyTimes) {
      this.applyTimes = applyTimes;
    }

    long getApplyTimes() {
      return applyTimes;
    }

    @Override public void relEquivalenceFound(RelEquivalenceEvent event) {
    }

    @Override public void ruleAttempted(RuleAttemptedEvent event) {
      if (event.isBefore()) {
        ++applyTimes;
      }
    }

    @Override public void ruleProductionSucceeded(RuleProductionEvent event) {
    }

    @Override public void relDiscarded(RelDiscardedEvent event) {
    }

    @Override public void relChosen(RelChosenEvent event) {
    }
  }
}

// End HepPlannerTest.java
