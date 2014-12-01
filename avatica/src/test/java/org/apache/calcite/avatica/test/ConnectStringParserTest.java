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
package org.apache.calcite.avatica.test;

import org.apache.calcite.avatica.ConnectStringParser;

import org.junit.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Unit test for JDBC connect string parser, {@link ConnectStringParser}. The
 * ConnectStringParser is adapted from code in Mondrian, but most of the tests
 * below were unfortunately "reinvented" prior to having the Mondrian unit tests
 * in hand.
 */
public class ConnectStringParserTest {
  /**
   * Tests simple connect string. Adapted from Mondrian tests.
   */
  @Test public void testSimpleStrings() throws Throwable {
    Properties props = ConnectStringParser.parse("foo=x;bar=y;foo=z");
    assertEquals(
        "bar",
        "y",
        props.get("bar"));
    assertNull(
        "BAR",
        props.get("BAR")); // case-sensitive, unlike Mondrian
    assertEquals(
        "last foo",
        "z",
        props.get("foo"));
    assertNull(
        "key=\" bar\"",
        props.get(" bar"));
    assertNull(
        "bogus key",
        props.get("kipper"));
    assertEquals(
        "param count",
        2,
        props.size());

    String synth = ConnectStringParser.getParamString(props);
    Properties synthProps = ConnectStringParser.parse(synth);
    assertEquals("reversible", props, synthProps);
  }

  /**
   * Tests complex connect strings. Adapted directly from Mondrian tests.
   */
  @Test public void testComplexStrings() throws Throwable {
    Properties props =
        ConnectStringParser.parse("normalProp=value;"
            + "emptyValue=;"
            + " spaceBeforeProp=abc;"
            + " spaceBeforeAndAfterProp =def;"
            + " space in prop = foo bar ;"
            + "equalsInValue=foo=bar;"
            + "semiInProp;Name=value;"
            + " singleQuotedValue = 'single quoted value ending in space ' ;"
            + " doubleQuotedValue = "
            + "\"=double quoted value preceded by equals\" ;"
            + " singleQuotedValueWithSemi = 'one; two';"
            + " singleQuotedValueWithSpecials = 'one; two \"three''four=five'");

    assertEquals(
        "param count",
        11,
        props.size());

    String value;
    value = (String) props.get("normalProp");
    assertEquals("value", value);
    value = (String) props.get("emptyValue");
    assertEquals("", value); // empty string, not null!
    value = (String) props.get("spaceBeforeProp");
    assertEquals("abc", value);
    value = (String) props.get("spaceBeforeAndAfterProp");
    assertEquals("def", value);
    value = (String) props.get("space in prop");
    assertEquals(value, "foo bar");
    value = (String) props.get("equalsInValue");
    assertEquals("foo=bar", value);
    value = (String) props.get("semiInProp;Name");
    assertEquals("value", value);
    value = (String) props.get("singleQuotedValue");
    assertEquals("single quoted value ending in space ", value);
    value = (String) props.get("doubleQuotedValue");
    assertEquals("=double quoted value preceded by equals", value);
    value = (String) props.get("singleQuotedValueWithSemi");
    assertEquals(value, "one; two");
    value = (String) props.get("singleQuotedValueWithSpecials");
    assertEquals(value, "one; two \"three'four=five");
  }

  /**
   * Tests for specific errors thrown by the parser.
   */
  @Test public void testConnectStringErrors() throws Throwable {
    // force some parsing errors
    try {
      ConnectStringParser.parse("key='can't parse'");
      fail("quoted value ended too soon");
    } catch (SQLException e) {
      assertExceptionMatches(e, ".*quoted value ended.*position 9.*");
    }

    try {
      ConnectStringParser.parse("key='\"can''t parse\"");
      fail("unterminated quoted value");
    } catch (SQLException e) {
      assertExceptionMatches(e, ".*unterminated quoted value.*");
    }
  }

  /**
   * Tests most of the examples from the <a
   * href="http://msdn.microsoft.com/library/default.asp?url=/library/en-us/oledb/htm/oledbconnectionstringsyntax.asp">
   * OLE DB spec</a>. Omitted are cases for Window handles, returning multiple
   * values, and special handling of "Provider" keyword.
   */
  @Test public void testOleDbExamples() throws Throwable {
    // test the parser with examples from OLE DB documentation
    Quad[] quads = {
      // {reason for test, key, val, string to parse},
      new Quad(
          "printable chars",
          "Jet OLE DB:System Database", "c:\\system.mda",
          "Jet OLE DB:System Database=c:\\system.mda"),
      new Quad(
          "key embedded semi",
          "Authentication;Info", "Column 5",
          "Authentication;Info=Column 5"),
      new Quad(
          "key embedded equal",
          "Verification=Security", "True",
          "Verification==Security=True"),
      new Quad(
          "key many equals",
          "Many==One", "Valid",
          "Many====One=Valid"),
      new Quad(
          "key too many equal",
          "TooMany=", "False",
          "TooMany===False"),
      new Quad(
          "value embedded quote and semi",
          "ExtProps", "Data Source='localhost';Key Two='value 2'",
          "ExtProps=\"Data Source='localhost';Key Two='value 2'\""),
      new Quad(
          "value embedded double quote and semi",
          "ExtProps", "Integrated Security=\"SSPI\";Key Two=\"value 2\"",
          "ExtProps='Integrated Security=\"SSPI\";Key Two=\"value 2\"'"),
      new Quad(
          "value double quoted",
          "DataSchema", "\"MyCustTable\"",
          "DataSchema='\"MyCustTable\"'"),
      new Quad(
          "value single quoted",
          "DataSchema", "'MyCustTable'",
          "DataSchema=\"'MyCustTable'\""),
      new Quad(
          "value double quoted double trouble",
          "Caption", "\"Company's \"new\" customer\"",
          "Caption=\"\"\"Company's \"\"new\"\" customer\"\"\""),
      new Quad(
          "value single quoted double trouble",
          "Caption", "\"Company's \"new\" customer\"",
          "Caption='\"Company''s \"new\" customer\"'"),
      new Quad(
          "embedded blanks and trim",
          "My Keyword", "My Value",
          " My Keyword = My Value ;MyNextValue=Value"),
      new Quad(
          "value single quotes preserve blanks",
          "My Keyword", " My Value ",
          " My Keyword =' My Value ';MyNextValue=Value"),
      new Quad(
          "value double quotes preserve blanks",
          "My Keyword", " My Value ",
          " My Keyword =\" My Value \";MyNextValue=Value"),
      new Quad(
          "last redundant key wins",
          "SomeKey", "NextValue",
          "SomeKey=FirstValue;SomeKey=NextValue"),
    };
    for (Quad quad : quads) {
      Properties props = ConnectStringParser.parse(quad.str);

      assertEquals(quad.why, quad.val, props.get(quad.key));
      String synth = ConnectStringParser.getParamString(props);

      try {
        assertEquals("reversible " + quad.why, quad.str, synth);
      } catch (Throwable e) {
        // it's OK that the strings don't match as long as the
        // two strings parse out the same way and are thus
        // "semantically reversible"
        Properties synthProps = ConnectStringParser.parse(synth);
        assertEquals("equivalent " + quad.why, props, synthProps);
      }
    }
  }

  static void assertExceptionMatches(
      Throwable e,
      String expectedPattern) {
    if (e == null) {
      fail("Expected an error which matches pattern '" + expectedPattern + "'");
    }
    String msg = e.toString();
    if (!msg.matches(expectedPattern)) {
      fail("Got a different error '" + msg + "' than expected '"
          + expectedPattern + "'");
    }
  }

  /** Collection of values comprising a test. */
  static class Quad {
    private final String why;
    private final String key;
    private final String val;
    private final String str;

    Quad(String why, String key, String val, String str) {
      this.why = why;
      this.key = key;
      this.val = val;
      this.str = str;
    }
  }
}

// End ConnectStringParserTest.java
