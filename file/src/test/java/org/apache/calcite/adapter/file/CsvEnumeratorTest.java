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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test for the {@link CsvEnumerator}.
 */
@SuppressWarnings("SameParameterValue")
class CsvEnumeratorTest {

  @Test void testParseDecimalScaleRounding() {
    checkParse("123.45", 5, 2, "123.45");
    checkParse("123.455", 5, 2, "123.46");
    checkParse("-123.455", 5, 2, "-123.46");
    checkParse("123.454", 5, 2, "123.45");
    checkParse("-123.454", 5, 2, "-123.45");
  }

  private static void checkParse(String s, int precision, int scale,
      String expected) {
    assertThat(CsvEnumerator.parseDecimal(precision, scale, s),
        is(new BigDecimal(expected)));
  }

  @Test void testParseDecimalPrecisionExceeded() {
    checkThrows(4, 0, "1e+5");
    checkThrows(4, 0, "-1e+5");
    checkThrows(4, 0, "12345");
    checkThrows(4, 0, "-12345");
    checkThrows(4, 2, "123.45");
    checkThrows(4, 2, "-123.45");
  }

  private static void checkThrows(int precision, int scale, String s) {
    assertThrows(IllegalArgumentException.class,
        () -> CsvEnumerator.parseDecimal(precision, scale, s));
  }
}
