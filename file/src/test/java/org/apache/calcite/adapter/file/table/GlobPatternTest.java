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
import org.junit.jupiter.api.Tag;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for understanding glob pattern behavior.
 */
@Tag("unit")
public class GlobPatternTest {

  @Test void testGlobPatterns() {
    // Test various glob patterns
    PathMatcher matcher1 = FileSystems.getDefault().getPathMatcher("glob:**");
    PathMatcher matcher2 = FileSystems.getDefault().getPathMatcher("glob:**/*");
    PathMatcher matcher3 = FileSystems.getDefault().getPathMatcher("glob:*");
    PathMatcher matcher4 = FileSystems.getDefault().getPathMatcher("glob:{*,**/*}");

    // Test paths
    Path rootFile = Paths.get("file.csv");
    Path subFile = Paths.get("sub/file.csv");
    Path nestedFile = Paths.get("sub/nested/file.csv");

    // Test ** pattern
    System.out.println("Testing pattern '**':");
    System.out.println("  'file.csv' matches: " + matcher1.matches(rootFile));
    System.out.println("  'sub/file.csv' matches: " + matcher1.matches(subFile));
    System.out.println("  'sub/nested/file.csv' matches: " + matcher1.matches(nestedFile));

    assertTrue(matcher1.matches(rootFile), "'**' should match root files");
    assertTrue(matcher1.matches(subFile), "'**' should match subdirectory files");
    assertTrue(matcher1.matches(nestedFile), "'**' should match nested files");

    // Test **/* pattern
    System.out.println("\nTesting pattern '**/*':");
    System.out.println("  'file.csv' matches: " + matcher2.matches(rootFile));
    System.out.println("  'sub/file.csv' matches: " + matcher2.matches(subFile));
    System.out.println("  'sub/nested/file.csv' matches: " + matcher2.matches(nestedFile));

    assertFalse(matcher2.matches(rootFile), "'**/*' should NOT match root files");
    assertTrue(matcher2.matches(subFile), "'**/*' should match subdirectory files");
    assertTrue(matcher2.matches(nestedFile), "'**/*' should match nested files");

    // Test * pattern
    System.out.println("\nTesting pattern '*':");
    System.out.println("  'file.csv' matches: " + matcher3.matches(rootFile));
    System.out.println("  'sub/file.csv' matches: " + matcher3.matches(subFile));

    assertTrue(matcher3.matches(rootFile), "'*' should match root files");
    assertFalse(matcher3.matches(subFile), "'*' should NOT match subdirectory files");

    // Test combined pattern
    System.out.println("\nTesting pattern '{*,**/*}':");
    System.out.println("  'file.csv' matches: " + matcher4.matches(rootFile));
    System.out.println("  'sub/file.csv' matches: " + matcher4.matches(subFile));

    assertTrue(matcher4.matches(rootFile), "'{*,**/*}' should match root files");
    assertTrue(matcher4.matches(subFile), "'{*,**/*}' should match subdirectory files");
  }
}
