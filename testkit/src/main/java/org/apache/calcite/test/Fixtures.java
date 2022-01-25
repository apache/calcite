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

import org.apache.calcite.sql.parser.SqlParserFixture;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.apache.calcite.sql.test.SqlOperatorFixture;

/** Fluent test fixtures for typical Calcite tests (parser, validator,
 * sql-to-rel and rel-rules) that can easily be used in dependent projects. */
public class Fixtures {
  private Fixtures() {}

  /** Creates a fixture for parser tests. */
  public static SqlParserFixture forParser() {
    return new SqlParserTest().fixture();
  }

  /** Creates a fixture for validation tests. */
  public static SqlValidatorFixture forValidator() {
    return SqlValidatorTestCase.FIXTURE;
  }

  /** Creates a fixture for SQL-to-Rel tests. */
  public static SqlToRelFixture forSqlToRel() {
    return SqlToRelFixture.DEFAULT;
  }

  /** Creates a fixture for rule tests. */
  public static RelOptFixture forRules() {
    return RelOptFixture.DEFAULT;
  }

  /** Creates a fixture for operator tests. */
  public static SqlOperatorFixture forOperators(boolean execute) {
    return execute
        ? SqlOperatorFixtureImpl.DEFAULT.withTester(t -> SqlOperatorTest.TESTER)
        : SqlOperatorFixtureImpl.DEFAULT;
  }

  /** Creates a fixture for metadata tests. */
  public static RelMetadataFixture forMetadata() {
    return RelMetadataFixture.DEFAULT;
  }
}
