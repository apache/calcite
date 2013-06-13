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
package net.hydromatic.optiq.test;

import net.hydromatic.optiq.impl.clone.ArrayTableTest;

import org.eigenbase.relopt.RelOptUtilTest;
import org.eigenbase.relopt.volcano.VolcanoPlannerTest;
import org.eigenbase.relopt.volcano.VolcanoPlannerTraitTest;
import org.eigenbase.sql.parser.SqlParserTest;
import org.eigenbase.sql.test.*;
import org.eigenbase.test.*;

import org.eigenbase.util.*;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Optiq test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ArrayTableTest.class,
    ArrayQueueTest.class,
    GraphTest.class,
    ReflectVisitorTest.class,
    RelOptUtilTest.class,
    UtilTest.class,
    EigenbaseResourceTest.class,
    FilteratorTest.class,
    OptionsListTest.class,
    ConnectStringParserTest.class,
    SqlParserTest.class,
    SqlValidatorTest.class,
    SqlValidatorFeatureTest.class,
    SqlAdvisorTest.class,
    SqlPrettyWriterTest.class,
    RexProgramTest.class,
    RexTransformerTest.class,
    RelMetadataTest.class,
    VolcanoPlannerTraitTest.class,
    VolcanoPlannerTest.class,
    HepPlannerTest.class,
    RelOptRulesTest.class,
    SargTest.class,
    SqlLimitsTest.class,
    PermutationTestCase.class,
    ReflectiveSchemaTest.class,
    LinqFrontJdbcBackTest.class,
    JdbcFrontLinqBackTest.class,
    JdbcFrontJdbcBackLinqMiddleTest.class,
    JdbcFrontJdbcBackTest.class,
    SqlToRelConverterTest.class,
    SqlFunctionsTest.class,
    SqlTypeNameTest.class,
    SqlOperatorTest.class,
    OptiqSqlOperatorTest.class,
    ModelTest.class,
    RexProgramTest.class,
    RexTransformerTest.class,
    JdbcAdapterTest.class,
    MongoAdapterTest.class,
    JdbcTest.class
})
public class OptiqSuite {
}

// End OptiqSuite.java
