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

import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.runtime.XmlFunctions;
import org.apache.calcite.util.BuiltInMethod;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit test for the methods in {@link SqlFunctions} that implement Xml processing functions.
 */
public class SqlXmlFunctionsTest {

  @Test
  public void testExtractValue() {
    assertExtractValue("<a>ccc<b>ddd</b></a>", "/a", is("ccc"));

    String input = "<a>ccc<b>ddd</b></a>";
    String message = "Illegal error behavior "
        + "'javax.xml.xpath.XPathExpressionException: javax.xml.transform.TransformerException: "
        + "A location path was expected, but the following token was encountered:  #' "
        + "EXTRACTVALUE: document: '" + input + "', xpath expression: '#'";
    CalciteException expected = new CalciteException(message, null);
    assertExtractValueFailed(input, "#", errorMatches(expected));
  }

  private void assertExtractValue(String input, String xpath,
      Matcher<? super String> matcher) {
    assertThat(invocationDesc(BuiltInMethod.EXTRACTVALUE.getMethodName(), input),
        XmlFunctions.extractValue(input, xpath),
        matcher);
  }

  private String invocationDesc(String methodName, Object... args) {
    return methodName + "(" + String.join(", ",
        Arrays.stream(args)
            .map(Objects::toString)
            .collect(Collectors.toList())) + ")";
  }

  private void assertExtractValueFailed(String input, String xpath,
      Matcher<? super Throwable> matcher) {
    assertFailed(invocationDesc(BuiltInMethod.EXTRACTVALUE.getMethodName(), input, xpath),
        () -> XmlFunctions.extractValue(input, xpath),
        matcher);
  }

  private void assertFailed(String invocationDesc, Supplier<?> supplier,
      Matcher<? super Throwable> matcher) {
    try {
      supplier.get();
      fail("expect exception, but not: " + invocationDesc);
    } catch (Throwable t) {
      assertThat(invocationDesc, t, matcher);
    }
  }

  private Matcher<? super Throwable> errorMatches(Throwable expected) {
    return new BaseMatcher<Throwable>() {
      @Override public boolean matches(Object item) {
        if (!(item instanceof Throwable)) {
          return false;
        }
        Throwable error = (Throwable) item;
        return expected != null
            && Objects.equals(error.getClass(), expected.getClass())
            && Objects.equals(error.getMessage(), expected.getMessage());
      }

      @Override public void describeTo(Description description) {
        description.appendText("is ").appendText(expected.toString());
      }
    };
  }
}

// End SqlXmlFunctionsTest.java
