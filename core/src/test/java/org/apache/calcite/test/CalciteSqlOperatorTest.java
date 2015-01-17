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

import org.apache.calcite.sql.test.SqlOperatorBaseTest;
import org.apache.calcite.sql.test.SqlTester;

import java.sql.Connection;

/**
 * Embodiment of {@link org.apache.calcite.sql.test.SqlOperatorBaseTest}
 * that generates SQL statements and executes them using Calcite.
 */
public class CalciteSqlOperatorTest extends SqlOperatorBaseTest {
  private static final ThreadLocal<Connection> LOCAL =
      new ThreadLocal<Connection>() {
        @Override protected Connection initialValue() {
          try {
            return CalciteAssert.that().with(
                CalciteAssert.SchemaSpec.HR).connect();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };

  private static SqlTester getHrTester() {
    return tester(LOCAL.get());
  }

  public CalciteSqlOperatorTest() {
    super(false, getHrTester());
  }
}

// End CalciteSqlOperatorTest.java
