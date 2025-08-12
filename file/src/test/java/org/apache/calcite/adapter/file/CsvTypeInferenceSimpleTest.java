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
import java.util.regex.Pattern;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Simple test to debug regex patterns.
 */
public class CsvTypeInferenceSimpleTest {

  @Test
  void testIntegerPattern() {
    Pattern INTEGER_PATTERN = Pattern.compile("^-?\\d+$");
    
    // Test values that should match
    assertTrue(INTEGER_PATTERN.matcher("1").matches(), "Should match '1'");
    assertTrue(INTEGER_PATTERN.matcher("123").matches(), "Should match '123'");
    assertTrue(INTEGER_PATTERN.matcher("-456").matches(), "Should match '-456'");
    assertTrue(INTEGER_PATTERN.matcher("0").matches(), "Should match '0'");
    
    // Test values that should NOT match
    assertFalse(INTEGER_PATTERN.matcher("1.0").matches(), "Should not match '1.0'");
    assertFalse(INTEGER_PATTERN.matcher("abc").matches(), "Should not match 'abc'");
    assertFalse(INTEGER_PATTERN.matcher("").matches(), "Should not match empty string");
    assertFalse(INTEGER_PATTERN.matcher(" 1").matches(), "Should not match ' 1' with leading space");
    assertFalse(INTEGER_PATTERN.matcher("1 ").matches(), "Should not match '1 ' with trailing space");
    
    // The actual values from CSV
    String val1 = "1";
    String val2 = "2";
    String val3 = "3";
    
    System.out.println("Testing actual CSV values:");
    System.out.println("'1' matches: " + INTEGER_PATTERN.matcher(val1).matches());
    System.out.println("'2' matches: " + INTEGER_PATTERN.matcher(val2).matches());
    System.out.println("'3' matches: " + INTEGER_PATTERN.matcher(val3).matches());
    
    // Check what happens after trim
    String withSpaces = " 1 ";
    System.out.println("' 1 ' matches: " + INTEGER_PATTERN.matcher(withSpaces).matches());
    System.out.println("' 1 '.trim() matches: " + INTEGER_PATTERN.matcher(withSpaces.trim()).matches());
  }
  
  @Test
  void testBooleanPattern() {
    Pattern BOOLEAN_PATTERN = Pattern.compile("^(true|false|TRUE|FALSE|True|False|0|1)$");
    
    assertTrue(BOOLEAN_PATTERN.matcher("true").matches());
    assertTrue(BOOLEAN_PATTERN.matcher("false").matches());
    assertTrue(BOOLEAN_PATTERN.matcher("TRUE").matches());
    assertTrue(BOOLEAN_PATTERN.matcher("FALSE").matches());
    assertTrue(BOOLEAN_PATTERN.matcher("0").matches());
    assertTrue(BOOLEAN_PATTERN.matcher("1").matches());
    
    assertFalse(BOOLEAN_PATTERN.matcher("2").matches());
    assertFalse(BOOLEAN_PATTERN.matcher("yes").matches());
  }
}