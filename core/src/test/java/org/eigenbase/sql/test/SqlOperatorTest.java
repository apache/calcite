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
 * Concrete subclass of {@link SqlOperatorBaseTest} which checks against
 * a {@link SqlValidator}. Tests that involve execution trivially succeed.
 */
public class SqlOperatorTest
    extends SqlOperatorBaseTest
{
  private static final SqlTester DEFAULT_TESTER =
      (SqlTester) new SqlValidatorTestCase(null)
          .getTester(SqlConformance.Default);

    /** Creates a SqlOperatorTest. */
    public SqlOperatorTest() {
        super(false, DEFAULT_TESTER);
    }
}

// End SqlOperatorTest.java
