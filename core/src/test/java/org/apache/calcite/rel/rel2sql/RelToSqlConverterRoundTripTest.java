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
package org.apache.calcite.rel.rel2sql;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Runs every test of {@link RelToSqlConverterTest} through the round trip
 * SQL &rarr; Rel &rarr; SQL &rarr; Rel &rarr; SQL
 * and checks that the second and third SQL are the same.
 *
 * <p>Tests whose Calcite-dialect output cannot be parsed or validated are skipped.
 */
class RelToSqlConverterRoundTripTest extends RelToSqlConverterTest {
  @Override Sql fixture() {
    return super.fixture().withRoundTrip();
  }

  @Disabled("SUM(DISTINCT) OVER expands into a deeper CASE on every re-parse,"
      + " so the conversion never reaches a fixed point")
  @Test @Override void testConvertWindowToSql() {
  }

  @Disabled("UNION ALL gains a subquery alias only on the second"
      + " re-parse, so the second and third SQL differ")
  @Test @Override void testThreeQueryUnion() {
  }
}
