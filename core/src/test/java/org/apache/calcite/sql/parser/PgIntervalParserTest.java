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
package org.apache.calcite.sql.parser;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for {@link PgIntervalParser}.
 */
class PgIntervalParserTest {
  @Test void testParse() {
    assertEquals(
        PgIntervalParser.parse("10 milliseconds"),
        new PgInterval(true, 0, 0, 0, 0, 0, 0, 10));

    assertEquals(
        PgIntervalParser.parse(
            "1 year 2 months 3 weeks 4 days 5 hours 6 minutes 7 seconds 8 milliseconds"),
        new PgInterval(true, 1, 2, 3 * 7 + 4, 5, 6, 7, 8));
  }
}
