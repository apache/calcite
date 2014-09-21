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
package net.hydromatic.optiq.test;

import net.hydromatic.optiq.impl.clone.ArrayTableTest;
import net.hydromatic.optiq.runtime.BinarySearchTest;
import net.hydromatic.optiq.runtime.EnumerablesTest;
import net.hydromatic.optiq.tools.FrameworksTest;
import net.hydromatic.optiq.tools.PlannerTest;
import net.hydromatic.optiq.util.BitSetsTest;
import net.hydromatic.optiq.util.PartiallyOrderedSetTest;
import net.hydromatic.optiq.util.graph.DirectedGraphTest;

import org.eigenbase.relopt.RelOptUtilTest;
import org.eigenbase.relopt.RelWriterTest;
import org.eigenbase.relopt.volcano.VolcanoPlannerTest;
import org.eigenbase.relopt.volcano.VolcanoPlannerTraitTest;
import org.eigenbase.rex.RexExecutorTest;
import org.eigenbase.sql.parser.SqlParserTest;
import org.eigenbase.sql.test.*;
import org.eigenbase.test.*;
import org.eigenbase.util.*;
import org.eigenbase.util.mapping.MappingTest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Optiq test suite.
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
    DirectedGraphTest.class,
    ReflectVisitorTest.class,
    RelOptUtilTest.class,
    UtilTest.class,
    MappingTest.class,
    EigenbaseResourceTest.class,
    FilteratorTest.class,
    PermutationTestCase.class,
    SqlFunctionsTest.class,
    SqlTypeNameTest.class,
    ModelTest.class,
    SqlValidatorFeatureTest.class,
    VolcanoPlannerTraitTest.class,
    InterpreterTest.class,
    VolcanoPlannerTest.class,
    SargTest.class,
    SqlPrettyWriterTest.class,
    RelWriterTest.class,
    RexProgramTest.class,
    BinarySearchTest.class,
    EnumerablesTest.class,

    // medium tests (above 0.1s)
    SqlParserTest.class,
    SqlValidatorTest.class,
    SqlAdvisorTest.class,
    RexTransformerTest.class,
    RelMetadataTest.class,
    HepPlannerTest.class,
    RelOptRulesTest.class,
    RexExecutorTest.class,
    MaterializationTest.class,
    LatticeTest.class,
    SqlLimitsTest.class,
    LinqFrontJdbcBackTest.class,
    JdbcFrontLinqBackTest.class,
    JdbcFrontJdbcBackTest.class,
    SqlToRelConverterTest.class,
    SqlOperatorTest.class,
    RexTransformerTest.class,
    ChunkListTest.class,
    FrameworksTest.class,
    PlannerTest.class,
    ExceptionMessageTest.class,

    // slow tests (above 1s)
    JdbcAdapterTest.class,
    JdbcFrontJdbcBackLinqMiddleTest.class,
    OptiqSqlOperatorTest.class,
    ReflectiveSchemaTest.class,
    JdbcTest.class,

    // test cases
    TableInRootSchemaTest.class,
    MultiJdbcSchemaJoinTest.class,

    // slow tests that don't break often
    SqlToRelConverterExtendedTest.class,
    PartiallyOrderedSetTest.class,

    // system tests and benchmarks (very slow, but usually only run if
    // '-Doptiq.test.slow=true' is specified)
    FoodmartTest.class
})
public class OptiqSuite {
}

// End OptiqSuite.java
