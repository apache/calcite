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

import junit.framework.TestCase;

import static net.hydromatic.optiq.test.OptiqAssert.assertThat;

/**
 * Tests for a JDBC front-end and JDBC back-end.
 *
 * <p>The idea is that as much as possible of the query is pushed down
 * to the JDBC data source, in the form of a large (and hopefully efficient)
 * SQL statement.</p>
 *
 * @see JdbcFrontJdbcBackLinqMiddleTest
 *
 * @author jhyde
 */
public class JdbcFrontJdbcBackTest extends TestCase {
    public void testWhere2() {
        assertThat()
            .with(OptiqAssert.Config.JDBC_FOODMART2)
            .query("select * from \"foodmart\".\"days\" where \"day\" < 3")
            .returns(
                "day=1; week_day=Sunday\n"
                + "day=2; week_day=Monday\n");
    }
}

// End JdbcFrontJdbcBackTest.java
