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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link SqlParserPos}.
 */
public class SqlParserPosTest {
  /** Tests {@link SqlParserPos#endsImmediatelyBefore(SqlParserPos)}. */
  @Test void testEndsImmediatelyBefore() {
    // A single-character position ends immediately before the one in the next
    // column, like the two '>' of a '>>' token.
    final SqlParserPos col1 = new SqlParserPos(1, 1);
    final SqlParserPos col2 = new SqlParserPos(1, 2);
    assertThat(col1.endsImmediatelyBefore(col2), is(true));

    // The relation is directional, not symmetric.
    assertThat(col2.endsImmediatelyBefore(col1), is(false));

    // A gap between the positions (e.g. whitespace, like '> >') does not
    // qualify.
    final SqlParserPos col3 = new SqlParserPos(1, 3);
    assertThat(col1.endsImmediatelyBefore(col3), is(false));

    // A position does not end immediately before itself.
    assertThat(col1.endsImmediatelyBefore(col1), is(false));

    // A multi-column position ends immediately before the position that starts
    // one column after it ends.
    final SqlParserPos cols1To2 = new SqlParserPos(1, 1, 1, 2);
    assertThat(cols1To2.endsImmediatelyBefore(col3), is(true));
    assertThat(cols1To2.endsImmediatelyBefore(col2), is(false));

    // Positions on different lines never qualify.
    final SqlParserPos line2 = new SqlParserPos(2, 1);
    assertThat(new SqlParserPos(1, 5).endsImmediatelyBefore(line2), is(false));
  }
}
