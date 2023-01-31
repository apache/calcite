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
package org.apache.calcite.util;

import org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.calcite.util.format.FormatElement;
import org.apache.calcite.util.format.FormatModels;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Unit test for {@link FormatModels}.
 */
public class FormatModelsTest {

  private static final Map<String, FormatElement> PARSE_MAP =
      BigQuerySqlDialect.DEFAULT.getFormatElementMap();

  private static final FormatModels PARSER =
      FormatModels.create(PARSE_MAP);

  private void assertThatFormatElementParse(String fmtStr, Matcher<List<String>> expected) {
    List<FormatElement> parseResult = PARSER.parse(fmtStr);
    List<String> stringResults = new ArrayList<>();
    for (FormatElement ele : parseResult) {
      ele.flatten(i -> stringResults.add(i.toString()));
    }
    assertThat(stringResults, expected);
  }

  @Test void testSingleElement() {
    assertThatFormatElementParse("%j", is(Arrays.asList("DDD")));
  }

  @Test void testMultipleElements() {
    assertThatFormatElementParse("%b-%d-%Y", is(Arrays.asList("MON", "-", "DD", "-", "YYYY")));
  }

  @Test void testArbitraryText() {
    assertThatFormatElementParse("%jtext%b", is(Arrays.asList("DDD", "text", "MON")));
  }

  @Test void testAliasText() {
    assertThatFormatElementParse("%R", is(Arrays.asList("HH24", ":", "MI")));
  }
}
