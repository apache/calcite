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

import org.apache.calcite.sql.FormatModel;
import org.apache.calcite.util.format.FormatModelElement;
import org.apache.calcite.util.format.FormatModelUtil;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for {@link FormatModelUtil}.
 */
public class FormatModelUtilTest {

  private static final ImmutableMap TEST_MAP = FormatModel.BIG_QUERY_FORMAT_ELEMENT_PARSE_MAP;

  private void assertFormatElementParse(String fmtStr, List<String> expected) {
    StringBuilder buf = new StringBuilder();
    ArrayList<String> collector = new ArrayList<>();
    List<FormatModelElement> result = FormatModelUtil.parse(fmtStr, TEST_MAP);
    List<String> tokens = tokens(collector, result);
    String literal = literal(buf, result);
    assertEquals(expected, tokens);
    assertEquals(fmtStr, literal);
  }

  private List<String> tokens(List<String> collector, List<FormatModelElement> fmtElements) {
    fmtElements.forEach(ele -> {
      if (ele.isAlias()) {
        tokens(collector, ele.getElements());
      } else {
        collector.add(ele.getToken());
      }
    });
    return collector;
  }

  private String literal(StringBuilder buf, List<FormatModelElement> fmtElements) {
    fmtElements.forEach(ele -> buf.append(ele.getLiteral()));
    return buf.toString();
  }

  @Test void testSingleElement() {
    assertFormatElementParse("%j", Arrays.asList("DDD"));
  }

  @Test void testMultipleElements() {
    assertFormatElementParse("%b-%d-%Y", Arrays.asList("MON", "LITERAL", "DD", "LITERAL", "YYYY"));
  }

  @Test void testArbitraryText() {
    assertFormatElementParse("%jtext%k", Arrays.asList("DDD", "LITERAL", "HH24"));
  }

  @Test void testAliasText() {
    assertFormatElementParse("%R", Arrays.asList("HH24", "LITERAL", "MI"));
  }
}
