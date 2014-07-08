/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.optiq.test;

import org.apache.optiq.impl.clone.ArrayTableTest;
import org.apache.optiq.runtime.BinarySearchTest;
import org.apache.optiq.tools.FrameworksTest;
import org.apache.optiq.tools.PlannerTest;
import org.apache.optiq.tools.SqlRunTest;
import org.apache.optiq.util.PartiallyOrderedSetTest;
import org.apache.optiq.util.graph.DirectedGraphTest;

import org.apache.optiq.relopt.RelOptUtilTest;
import org.apache.optiq.relopt.RelWriterTest;
import org.apache.optiq.relopt.volcano.VolcanoPlannerTest;
import org.apache.optiq.relopt.volcano.VolcanoPlannerTraitTest;
import org.apache.optiq.rex.RexExecutorTest;
import org.apache.optiq.sql.parser.SqlParserTest;
import org.apache.optiq.sql.test.*;
import org.apache.optiq.util.*;
import org.apache.optiq.util.mapping.MappingTest;

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
    VolcanoPlannerTest.class,
    SargTest.class,
    SqlPrettyWriterTest.class,
    RelWriterTest.class,
    RexProgramTest.class,
    BinarySearchTest.class,

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
    SqlRunTest.class,
    PartiallyOrderedSetTest.class
})
public class OptiqSuite {
}

// End OptiqSuite.java
