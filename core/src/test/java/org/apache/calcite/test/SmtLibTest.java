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


import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgramBuilderBase;
import org.apache.calcite.test.verifier.RexToSymbolicColumn;
import org.apache.calcite.test.verifier.SymbolicColumn;

import com.microsoft.z3.BoolExpr;
import com.microsoft.z3.Context;
import com.microsoft.z3.Expr;
import com.microsoft.z3.Solver;
import com.microsoft.z3.Status;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Testing for integrating z3 with calcite to verify equivalence.
 **/

public class SmtLibTest extends RexProgramBuilderBase {
  static boolean z3isLoad ;
  /**
   * Check z3 dynamic library exits before run all other tests.
   */
  @BeforeAll static void testZ3Lib() {
    try {
      Context z3Context = new Context();
      Expr x = z3Context.mkIntConst("x");
      Expr one = z3Context.mkInt(1);
      BoolExpr eq = z3Context.mkEq(x, one);
      Solver solver = z3Context.mkSolver();
      solver.add(eq);
      solver.check();
      z3isLoad = true;
    }catch (Error e){
      z3isLoad = false;
    }
  }

  /** Converts a SQL string to a relational expression using mock schema. */
  private RelNode toRel(String sql) {
    final SqlToRelTestBase test = new SqlToRelTestBase() {
    };
    return test.createTester().convertSqlToRel(sql).rel;
  }

  /** Converts a where condition string to a RexNode using mock schema on emp table. */
  private RexNode toRex(String cond) {
    final String sql = "select *\n"
        + "from emp where"
        + cond;
    final RelNode relNode = toRel(sql);
    LogicalProject project = (LogicalProject) relNode;
    LogicalFilter filter = (LogicalFilter) project.getInput();
    return filter.getCondition();
  }

  private boolean checkEqual(String cond1, String cond2) {
    RexNode rexNode1 = toRex(cond1);
    RexNode rexNode2 = toRex(cond2);
    Context z3Context = new Context();
    List<SymbolicColumn> inputSymbolicColumns = new ArrayList<>();
    /** mock emp table, 8 columns with int type, since we only support numerical type for now **/
    for (int i = 0; i < 8; i++) {
      SymbolicColumn inputColumn = SymbolicColumn.mkNewSymbolicColumn(z3Context, tInt());
      inputSymbolicColumns.add(inputColumn);
    }
    List<BoolExpr> env = new ArrayList<>();
    SymbolicColumn symbolicCond1 =
        RexToSymbolicColumn.rexToColumn(rexNode1, inputSymbolicColumns, z3Context, env);
    SymbolicColumn symbolicCond2 =
        RexToSymbolicColumn.rexToColumn(rexNode2, inputSymbolicColumns, z3Context, env);

    return SymbolicColumn.checkCondEq(symbolicCond1, symbolicCond2, env, z3Context);
  }

  @Test void rexNodeEq1() {
    assumeTrue(z3isLoad);
    final String cond1 = " empno > 10 and deptno = 5";
    final String cond2 = " empno + deptno > 15 and deptno = 5";
    assertEquals(checkEqual(cond1, cond2), true);
  }

  @Test void rexNodeNotEq1() {
    assumeTrue(z3isLoad);
    final String cond1 = " empno > 10 and deptno = 10";
    final String cond2 = " empno + deptno > 15 and deptno = 10";
    assertEquals(checkEqual(cond1, cond2), false);
  }
}
