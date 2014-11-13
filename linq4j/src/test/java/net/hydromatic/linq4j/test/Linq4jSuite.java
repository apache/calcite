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
package net.hydromatic.linq4j.test;

import net.hydromatic.linq4j.expressions.TypeTest;
import net.hydromatic.linq4j.function.FunctionTest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;


/**
 * Suite of all Linq4j tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    PrimitiveTest.class,
    Linq4jTest.class,
    ExpressionTest.class,
    OptimizerTest.class,
    InlinerTest.class,
    DeterministicTest.class,
    BlockBuilderTest.class,
    FunctionTest.class,
    TypeTest.class
})
public class Linq4jSuite {
}

// End Linq4jSuite.java
