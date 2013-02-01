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

import net.hydromatic.optiq.runtime.SqlFunctions;

import junit.framework.TestCase;

import static net.hydromatic.optiq.runtime.SqlFunctions.*;

/**
 * Unit test for the methods in {@link SqlFunctions} that implement SQL
 * functions.
 */
public class SqlFunctionsTest extends TestCase {
    /** Test for {@link SqlFunctions#rtrim}. */
    public void testRtrim() {
        assertEquals("", rtrim(""));
        assertEquals("", rtrim("    "));
        assertEquals("   x", rtrim("   x  "));
        assertEquals("   x", rtrim("   x "));
        assertEquals("   x y", rtrim("   x y "));
        assertEquals("   x", rtrim("   x"));
        assertEquals("x", rtrim("x"));
    }
}

// End SqlFunctionsTest.java
