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
package org.apache.calcite.unparse;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.util.TimestampWithTimeZoneString;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link SqlNode}.
 */
public class TimestampLiteralSqlNodeTest {

  @Test void testTimestampLiteralSqlNode() {
    final SqlNode node = SqlParserUtil.parseTimestampLiteral("TIMESTAMP '2020-05-21 11:20:01.4321'",
        SqlParserPos.ZERO);
    final String expectedSqlNode = "TIMESTAMP '2020-05-21 11:20:01.4321'";

    assertEquals(node.toString(), expectedSqlNode);
  }

/** Added support to create SqlNode for TIMESTAMP WITH TIME ZONE literal.
 *
 * Current Behaviour: Hardcoded precision value.
 * To-Do:
 *  Need to add support to calculate precision from input and get expected count of precision.
 * */
  @Test void testTimestampWithTimeZoneLiteralSqlNode() {
    TimestampWithTimeZoneString timestampWithTimeZoneString = new TimestampWithTimeZoneString(
        "2020-05-21 11:20:01.4321 GMT-05:00");

    final SqlNode node = SqlLiteral.createTimestampWithTimeZone(timestampWithTimeZoneString, 6,
        SqlParserPos.ZERO);
    final String expectedSqlNode = "TIMESTAMP '2020-05-21 11:20:01.4321 GMT-05:00'";

    assertEquals(node.toString(), expectedSqlNode);
  }
}
