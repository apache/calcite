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

import org.apache.calcite.adapter.clone.ArrayTableTest;
import org.apache.calcite.jdbc.CalciteRemoteDriverTest;
import org.apache.calcite.plan.RelOptPlanReaderTest;
import org.apache.calcite.plan.RelOptUtilTest;
import org.apache.calcite.plan.RelWriterTest;
import org.apache.calcite.plan.volcano.TraitPropagationTest;
import org.apache.calcite.plan.volcano.VolcanoPlannerTest;
import org.apache.calcite.plan.volcano.VolcanoPlannerTraitTest;
import org.apache.calcite.rel.RelCollationTest;
import org.apache.calcite.rel.rel2sql.RelToSqlConverterTest;
import org.apache.calcite.rex.RexExecutorTest;
import org.apache.calcite.runtime.BinarySearchTest;
import org.apache.calcite.runtime.EnumerablesTest;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.apache.calcite.sql.parser.SqlUnParserTest;
import org.apache.calcite.sql.test.SqlAdvisorTest;
import org.apache.calcite.sql.test.SqlOperatorTest;
import org.apache.calcite.sql.test.SqlPrettyWriterTest;
import org.apache.calcite.sql.test.SqlTypeNameTest;
import org.apache.calcite.test.enumerable.EnumerableCorrelateTest;
import org.apache.calcite.tools.FrameworksTest;
import org.apache.calcite.tools.PlannerTest;
import org.apache.calcite.util.BitSetsTest;
import org.apache.calcite.util.ChunkListTest;
import org.apache.calcite.util.ImmutableBitSetTest;
import org.apache.calcite.util.PartiallyOrderedSetTest;
import org.apache.calcite.util.PermutationTestCase;
import org.apache.calcite.util.ReflectVisitorTest;
import org.apache.calcite.util.UtilTest;
import org.apache.calcite.util.graph.DirectedGraphTest;
import org.apache.calcite.util.mapping.MappingTest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Calcite test suite.
 *
 * <p>Tests are sorted by approximate running time. The suite runs the fastest
 * tests first, so that regressions can be discovered as fast as possible.
 * Most unit tests run very quickly, and are scheduled before system tests
 * (which are slower but more likely to break because they have more
 * dependencies). Slow unit tests that don't break often are scheduled last.</p>
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // very fast tests (under 0.1s)
    ArrayTableTest.class,
    BitSetsTest.class,
    ImmutableBitSetTest.class,
    DirectedGraphTest.class,
    ReflectVisitorTest.class,
    RelOptUtilTest.class,
    RelCollationTest.class,
    UtilTest.class,
    MappingTest.class,
    CalciteResourceTest.class,
    FilteratorTest.class,
    PermutationTestCase.class,
    SqlFunctionsTest.class,
    SqlTypeNameTest.class,
    ModelTest.class,
    SqlValidatorFeatureTest.class,
    VolcanoPlannerTraitTest.class,
    InterpreterTest.class,
    VolcanoPlannerTest.class,
    HepPlannerTest.class,
    TraitPropagationTest.class,
    RelWriterTest.class,
    RexProgramTest.class,
    RexTransformerTest.class,
    BinarySearchTest.class,
    EnumerablesTest.class,
    ExceptionMessageTest.class,
    InduceGroupingTypeTest.class,
    RelOptPlanReaderTest.class,

    // medium tests (above 0.1s)
    SqlParserTest.class,
    SqlUnParserTest.class,
    SqlPrettyWriterTest.class,
    SqlValidatorTest.class,
    SqlAdvisorTest.class,
    RelMetadataTest.class,
    RelOptRulesTest.class,
    ScannableTableTest.class,
    RexExecutorTest.class,
    SqlLimitsTest.class,
    JdbcFrontLinqBackTest.class,
    JdbcFrontJdbcBackTest.class,
    SqlToRelConverterTest.class,
    RelToSqlConverterTest.class,
    SqlOperatorTest.class,
    ChunkListTest.class,
    FrameworksTest.class,
    EnumerableCorrelateTest.class,

    // slow tests (above 1s)
    UdfTest.class,
    PlannerTest.class,
    RelBuilderTest.class,
    PigRelBuilderTest.class,
    RexImplicationCheckerTest.class,
    MaterializationTest.class,
    JdbcAdapterTest.class,
    LinqFrontJdbcBackTest.class,
    JdbcFrontJdbcBackLinqMiddleTest.class,
    CalciteSqlOperatorTest.class,
    LatticeTest.class,
    ReflectiveSchemaTest.class,
    JdbcTest.class,
    CalciteRemoteDriverTest.class,
    StreamTest.class,

    // test cases
    TableInRootSchemaTest.class,
    RelMdColumnOriginsTest.class,
    MultiJdbcSchemaJoinTest.class,
    SqlLineTest.class,

    // slow tests that don't break often
    SqlToRelConverterExtendedTest.class,
    PartiallyOrderedSetTest.class,

    // system tests and benchmarks (very slow, but usually only run if
    // '-Dcalcite.test.slow=true' is specified)
    FoodmartTest.class
})
public class CalciteSuite {
}

// End CalciteSuite.java
