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
package org.apache.optiq.relopt;

import org.apache.optiq.reltype.*;
import org.apache.optiq.sql.type.*;
import org.apache.optiq.util.*;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit test for {@link RelOptUtil} and other classes in this package.
 */
public class RelOptUtilTest {
  //~ Constructors -----------------------------------------------------------

  public RelOptUtilTest() {
  }

  //~ Methods ----------------------------------------------------------------

  @Test public void testTypeDump() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl();
    RelDataType t1 =
        typeFactory.builder()
            .add("f0", SqlTypeName.DECIMAL, 5, 2)
            .add("f1", SqlTypeName.VARCHAR, 10)
            .build();
    TestUtil.assertEqualsVerbose(
        TestUtil.fold(
            "f0 DECIMAL(5, 2) NOT NULL,",
            "f1 VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL"),
        Util.toLinux(RelOptUtil.dumpType(t1) + "\n"));

    RelDataType t2 =
        typeFactory.builder()
            .add("f0", t1)
            .add("f1", typeFactory.createMultisetType(t1, -1))
            .build();
    TestUtil.assertEqualsVerbose(
        TestUtil.fold(
            "f0 RECORD (",
            "  f0 DECIMAL(5, 2) NOT NULL,",
            "  f1 VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL) NOT NULL,",
            "f1 RECORD (",
            "  f0 DECIMAL(5, 2) NOT NULL,",
            "  f1 VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL) NOT NULL MULTISET NOT NULL"),
        Util.toLinux(RelOptUtil.dumpType(t2) + "\n"));
  }

  /**
   * Tests the rules for how we name rules.
   */
  @Test public void testRuleGuessDescription() {
    assertEquals("Bar", RelOptRule.guessDescription("com.foo.Bar"));
    assertEquals("Baz", RelOptRule.guessDescription("com.flatten.Bar$Baz"));

    // yields "1" (which as an integer is an invalid
    try {
      Util.discard(RelOptRule.guessDescription("com.foo.Bar$1"));
      fail("expected exception");
    } catch (RuntimeException e) {
      assertEquals(
          "Derived description of rule class com.foo.Bar$1 is an "
          + "integer, not valid. Supply a description manually.",
          e.getMessage());
    }
  }
}

// End RelOptUtilTest.java
