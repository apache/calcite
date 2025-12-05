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

import org.apache.calcite.util.format.FormatElement;
import org.apache.calcite.util.format.FormatModel;
import org.apache.calcite.util.format.FormatModels;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.test.Matchers.isListOf;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit test for {@link FormatModel}.
 */
public class FormatModelTest {

  private void assertThatFormatElementParse(String formatString,
      Matcher<? super List<String>> matcher) {
    List<FormatElement> elements = FormatModels.BIG_QUERY.parse(formatString);
    List<String> stringResults = new ArrayList<>();
    for (FormatElement element : elements) {
      element.flatten(i -> stringResults.add(i.toString()));
    }
    assertThat(stringResults, matcher);
  }

  @Test void testSingleElement() {
    assertThatFormatElementParse("%j", isListOf("DDD"));
  }

  @Test void testMultipleElements() {
    assertThatFormatElementParse("%b-%d-%Y",
        isListOf("Mon", "-", "DD", "-", "pctY"));
  }

  @Test void testArbitraryText() {
    assertThatFormatElementParse("%jtext%b",
        isListOf("DDD", "text", "Mon"));
  }

  @Test void testAliasText() {
    assertThatFormatElementParse("%R",
        isListOf("HH24", ":", "MI"));
  }
}
