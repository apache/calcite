/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.test;

import net.hydromatic.optiq.runtime.SqlFunctions;
import net.hydromatic.optiq.runtime.Utilities;

import junit.framework.TestCase;

import java.util.*;

import static net.hydromatic.optiq.runtime.SqlFunctions.*;

/**
 * Unit test for the methods in {@link SqlFunctions} that implement SQL
 * functions.
 */
public class SqlFunctionsTest extends TestCase {
  public void testCharLength() {
    assertEquals(3, charLength("xyz"));
  }

  public void testConcat() {
    assertEquals("a bcd", concat("a b", "cd"));
    // The code generator will ensure that nulls are never passed in. If we
    // pass in null, it is treated like the string "null", as the following
    // tests show. Not the desired behavior for SQL.
    assertEquals("anull", concat("a", null));
    assertEquals("nullnull", concat(null, null));
    assertEquals("nullb", concat(null, "b"));
  }

  public void testLower() {
    assertEquals("a bcd", lower("A bCd"));
  }

  public void testUpper() {
    assertEquals("A BCD", upper("A bCd"));
  }

  public void testInitcap() {
    assertEquals("Aa", initcap("aA"));
    assertEquals("Zz", initcap("zz"));
    assertEquals("Az", initcap("AZ"));
    assertEquals("Try A Little  ", initcap("tRy a littlE  "));
    assertEquals("Won'T It?No", initcap("won't it?no"));
    assertEquals("1a", initcap("1A"));
    assertEquals(" B0123b", initcap(" b0123B"));
  }

  public void testLesser() {
    assertEquals("a", lesser("a", "bc"));
    assertEquals("ac", lesser("bc", "ac"));
    try {
      Object o = lesser("a", null);
      fail("Expected NPE, got " + o);
    } catch (NullPointerException e) {
      // ok
    }
    assertEquals("a", lesser(null, "a"));
    assertNull(lesser((String) null, null));
  }

  public void testGreater() {
    assertEquals("bc", greater("a", "bc"));
    assertEquals("bc", greater("bc", "ac"));
    try {
      Object o = greater("a", null);
      fail("Expected NPE, got " + o);
    } catch (NullPointerException e) {
      // ok
    }
    assertEquals("a", greater(null, "a"));
    assertNull(greater((String) null, null));
  }

  /** Test for {@link SqlFunctions#rtrim}. */
  public void testRtrim() {
    assertEquals("", rtrim(""));
    assertEquals("", rtrim("    "));
    assertEquals("   x", rtrim("   x  "));
    assertEquals("   x", rtrim("   x "));
    assertEquals("   x y", rtrim("   x y "));
    assertEquals("   x", rtrim("   x"));
    assertEquals("x", rtrim("x"));
  }

  /** Test for {@link SqlFunctions#ltrim}. */
  public void testLtrim() {
    assertEquals("", ltrim(""));
    assertEquals("", ltrim("    "));
    assertEquals("x  ", ltrim("   x  "));
    assertEquals("x ", ltrim("   x "));
    assertEquals("x y ", ltrim("x y "));
    assertEquals("x", ltrim("   x"));
    assertEquals("x", ltrim("x"));
  }

  /** Test for {@link SqlFunctions#trim}. */
  public void testTrim() {
    assertEquals("", trim(""));
    assertEquals("", trim("    "));
    assertEquals("x", trim("   x  "));
    assertEquals("x", trim("   x "));
    assertEquals("x y", trim("   x y "));
    assertEquals("x", trim("   x"));
    assertEquals("x", trim("x"));
  }

  public void testUnixDateToString() {
    // Verify these using the "date" command. E.g.
    // $ date -u --date="@$(expr 10957 \* 86400)"
    // Sat Jan  1 00:00:00 UTC 2000
    assertEquals("2000-01-01", unixDateToString(10957));

    assertEquals("1970-01-01", unixDateToString(0));
    assertEquals("1970-01-02", unixDateToString(1));
    assertEquals("1971-01-01", unixDateToString(365));
    assertEquals("1972-01-01", unixDateToString(730));
    assertEquals("1972-02-28", unixDateToString(788));
    assertEquals("1972-02-29", unixDateToString(789));
    assertEquals("1972-03-01", unixDateToString(790));

    assertEquals("1969-01-01", unixDateToString(-365));
    assertEquals("2000-01-01", unixDateToString(10957));
    assertEquals("2000-02-28", unixDateToString(11015));
    assertEquals("2000-02-29", unixDateToString(11016));
    assertEquals("2000-03-01", unixDateToString(11017));
    assertEquals("1945-02-24", unixDateToString(-9077));
  }

  public void testYmdToUnixDate() {
    assertEquals(0, ymdToUnixDate(1970, 1, 1));
    assertEquals(365, ymdToUnixDate(1971, 1, 1));
    assertEquals(-365, ymdToUnixDate(1969, 1, 1));
    assertEquals(11017, ymdToUnixDate(2000, 3, 1));
    assertEquals(-9077, ymdToUnixDate(1945, 2, 24));
  }

  public void testDateToString() {
    assertEquals("1970-01-01", unixDateToString(0));
    assertEquals("1971-02-03", unixDateToString(0 + 365 + 31 + 2));
  }

  public void testTimeToString() {
    assertEquals("00:00:00", unixTimeToString(0));
    assertEquals("23:59:59", unixTimeToString(86400000 - 1));
  }

  public void testTimestampToString() {
    // ISO format would be "1970-01-01T00:00:00" but SQL format is different
    assertEquals("1970-01-01 00:00:00", unixTimestampToString(0));
    assertEquals(
        "1970-02-01 23:59:59",
        unixTimestampToString(86400000L * 32L - 1L));
  }

  /** Unit test for
   * {@link Utilities#compare(java.util.List, java.util.List)}. */
  public void testCompare() {
    final List<String> ac = Arrays.asList("a", "c");
    final List<String> abc = Arrays.asList("a", "b", "c");
    final List<String> a = Arrays.asList("a");
    final List<String> empty = Collections.emptyList();
    assertEquals(0, Utilities.compare(ac, ac));
    assertEquals(0, Utilities.compare(ac, new ArrayList<String>(ac)));
    assertEquals(-1, Utilities.compare(a, ac));
    assertEquals(-1, Utilities.compare(empty, ac));
    assertEquals(1, Utilities.compare(ac, a));
    assertEquals(1, Utilities.compare(ac, abc));
    assertEquals(1, Utilities.compare(ac, empty));
    assertEquals(0, Utilities.compare(empty, empty));
  }

  public void testTruncateLong() {
    assertEquals(12000L, SqlFunctions.truncate(12345L, 1000L));
    assertEquals(12000L, SqlFunctions.truncate(12000L, 1000L));
    assertEquals(12000L, SqlFunctions.truncate(12001L, 1000L));
    assertEquals(11000L, SqlFunctions.truncate(11999L, 1000L));

    assertEquals(-13000L, SqlFunctions.truncate(-12345L, 1000L));
    assertEquals(-12000L, SqlFunctions.truncate(-12000L, 1000L));
    assertEquals(-13000L, SqlFunctions.truncate(-12001L, 1000L));
    assertEquals(-12000L, SqlFunctions.truncate(-11999L, 1000L));
  }

  public void testTruncateInt() {
    assertEquals(12000, SqlFunctions.truncate(12345, 1000));
    assertEquals(12000, SqlFunctions.truncate(12000, 1000));
    assertEquals(12000, SqlFunctions.truncate(12001, 1000));
    assertEquals(11000, SqlFunctions.truncate(11999, 1000));

    assertEquals(-13000, SqlFunctions.truncate(-12345, 1000));
    assertEquals(-12000, SqlFunctions.truncate(-12000, 1000));
    assertEquals(-13000, SqlFunctions.truncate(-12001, 1000));
    assertEquals(-12000, SqlFunctions.truncate(-11999, 1000));

    assertEquals(12000, SqlFunctions.round(12345, 1000));
    assertEquals(13000, SqlFunctions.round(12845, 1000));
    assertEquals(-12000, SqlFunctions.round(-12345, 1000));
    assertEquals(-13000, SqlFunctions.round(-12845, 1000));
  }
}

// End SqlFunctionsTest.java
