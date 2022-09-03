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

import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit test for the methods in {@link SqlFunctions} that implement Xml processing functions.
 */
class SqlXmlFunctionsTest {

  private static final String XML = "<document>string</document>";
  private static final String XSLT =
      "<xsl:stylesheet xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\"></xsl:stylesheet>";
  private static final String DOCUMENT_PATH = "/document";
  private static @Nullable String xmlExternalEntity = null;
  private static @Nullable String xsltExternalEntity = null;

  @BeforeAll public static void setup() throws Exception {
    final Path testFile = Files.createTempFile("foo", "temp");
    testFile.toFile().deleteOnExit();
    final String filePath = "file:///" + testFile.toAbsolutePath();
    xmlExternalEntity = "<!DOCTYPE document [ <!ENTITY entity SYSTEM \"" + filePath
        + "\"> ]><document>&entity;</document>";
    xsltExternalEntity = "<!DOCTYPE document [ <!ENTITY entity SYSTEM \"" + filePath
        + "\"> ]><xsl:stylesheet xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">&entity;</xsl:stylesheet>";
  }

  @Test void testExtractValue() {
    assertExtractValue("<a>ccc<b>ddd</b></a>", "/a", is("ccc"));

    String input = "<a>ccc<b>ddd</b></a>";
    String message = "Invalid input for EXTRACTVALUE: xml: '" + input + "', xpath expression: '#'";
    CalciteException expected = new CalciteException(message, null);
    assertExtractValueFailed(input, "#", Matchers.expectThrowable(expected));
  }

  @Test void testExtractValueExternalEntity() {
    String message = "Invalid input for EXTRACTVALUE: xml: '"
        + xmlExternalEntity + "', xpath expression: '" + DOCUMENT_PATH + "'";
    CalciteException expected = new CalciteException(message, null);
    assertExtractValueFailed(xmlExternalEntity, DOCUMENT_PATH,
        Matchers.expectThrowable(expected));
  }

  @Test void testExistsNodeExternalEntity() {
    String message = "Invalid input for EXISTSNODE xpath: '"
        + DOCUMENT_PATH + "', namespace: '" + null + "'";
    CalciteException expected = new CalciteException(message, null);
    assertExistsNodeFailed(xmlExternalEntity, DOCUMENT_PATH, null,
        Matchers.expectThrowable(expected));
  }

  @Test void testXmlTransformExternalEntity() {
    String message = "Invalid input for XMLTRANSFORM xml: '" + xmlExternalEntity + "'";
    CalciteException expected = new CalciteException(message, null);
    assertXmlTransformFailed(xmlExternalEntity, XSLT, Matchers.expectThrowable(expected));
  }

  @Test void testXmlTransformExternalEntityXslt() {
    String message = "Illegal xslt specified : '" + xsltExternalEntity + "'";
    CalciteException expected = new CalciteException(message, null);
    assertXmlTransformFailed(XML, xsltExternalEntity, Matchers.expectThrowable(expected));
  }

  @Test void testXmlTransform() {
    assertXmlTransform(null, "", nullValue());
    assertXmlTransform("", null, nullValue());

    String xslt = "<";
    String message = "Illegal xslt specified : '" + xslt + "'";
    CalciteException expected = new CalciteException(message, null);
    assertXmlTransformFailed("", xslt, Matchers.expectThrowable(expected));
  }

  @Test void testExtractXml() {
    assertExtractXml(null, "", null, nullValue());
    assertExtractXml("", null, null, nullValue());

    String xpath = "<";
    String namespace = "a";
    String message =
        "Invalid input for EXTRACT xpath: '" + xpath + "', namespace: '" + namespace + "'";
    CalciteException expected = new CalciteException(message, null);
    assertExtractXmlFailed("", xpath, namespace, Matchers.expectThrowable(expected));
  }


  @Test void testExistsNode() {
    assertExistsNode(null, "", null, nullValue());
    assertExistsNode("", null, null, nullValue());

    String xpath = "<";
    String namespace = "a";
    String message =
        "Invalid input for EXISTSNODE xpath: '" + xpath + "', namespace: '" + namespace + "'";
    CalciteException expected = new CalciteException(message, null);
    assertExistsNodeFailed("", xpath, namespace, Matchers.expectThrowable(expected));
  }

  private void assertExistsNode(String xml, String xpath, String namespace,
      Matcher<? super Integer> matcher) {
    String methodDesc = BuiltInMethod.EXISTS_NODE.getMethodName()
        + "(" + String.join(", ", xml, xpath, namespace) + ")";
    assertThat(methodDesc, XmlFunctions.existsNode(xml, xpath, namespace), matcher);
  }

  private void assertExistsNodeFailed(String xml, String xpath, String namespace,
      Matcher<? super Throwable> matcher) {
    String methodDesc = BuiltInMethod.EXISTS_NODE.getMethodName()
        + "(" + String.join(", ", xml, xpath, namespace) + ")";
    assertFailed(methodDesc, () -> XmlFunctions.existsNode(xml, xpath, namespace), matcher);
  }

  private void assertExtractXml(String xml, String xpath, String namespace,
      Matcher<? super String> matcher) {
    String methodDesc = BuiltInMethod.EXTRACT_XML.getMethodName()
        + "(" + String.join(", ", xml, xpath, namespace) + ")";
    assertThat(methodDesc, XmlFunctions.extractXml(xml, xpath, namespace), matcher);
  }

  private void assertExtractXmlFailed(String xml, String xpath, String namespace,
      Matcher<? super Throwable> matcher) {
    String methodDesc = BuiltInMethod.EXTRACT_XML.getMethodName()
        + "(" + String.join(", ", xml, xpath, namespace) + ")";
    assertFailed(methodDesc, () -> XmlFunctions.extractXml(xml, xpath, namespace), matcher);
  }

  private void assertXmlTransform(String xml, String xslt,
      Matcher<? super String> matcher) {
    String methodDesc =
        BuiltInMethod.XML_TRANSFORM.getMethodName() + "(" + String.join(", ", xml, xslt) + ")";
    assertThat(methodDesc, XmlFunctions.xmlTransform(xml, xslt), matcher);
  }

  private void assertXmlTransformFailed(String xml, String xslt,
      Matcher<? super Throwable> matcher) {
    String methodDesc =
        BuiltInMethod.XML_TRANSFORM.getMethodName() + "(" + String.join(", ", xml, xslt) + ")";
    assertFailed(methodDesc, () -> XmlFunctions.xmlTransform(xml, xslt), matcher);
  }

  private void assertExtractValue(String input, String xpath,
      Matcher<? super String> matcher) {
    String extractMethodDesc =
        BuiltInMethod.EXTRACT_VALUE.getMethodName() + "(" + String.join(", ", input) + ")";
    assertThat(extractMethodDesc, XmlFunctions.extractValue(input, xpath), matcher);
  }

  private void assertExtractValueFailed(String input, String xpath,
      Matcher<? super Throwable> matcher) {
    String extractMethodDesc =
        BuiltInMethod.EXTRACT_VALUE.getMethodName() + "(" + String.join(", ", input, xpath) + ")";
    assertFailed(extractMethodDesc, () -> XmlFunctions.extractValue(input, xpath), matcher);
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
}
