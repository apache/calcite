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
package org.eigenbase.sql.test;

import org.eigenbase.sql.validate.*;
import org.eigenbase.test.*;


/**
 * Concrete subclass of {@link SqlOperatorTests} which checks against
 *
 * @author Julian Hyde
 * @since July 7, 2005
 */
public class SqlOperatorTest
    extends SqlOperatorTests
{
    //~ Instance fields --------------------------------------------------------

    private SqlTester tester =
        (SqlTester) new SqlValidatorTestCase("dummy").getTester(
            SqlConformance.Default);

    //~ Constructors -----------------------------------------------------------

    public SqlOperatorTest(String testName)
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    protected SqlTester getTester()
    {
        return tester;
    }
}

// End SqlOperatorTest.java
