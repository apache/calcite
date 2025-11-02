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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;

/**
 * Tests checking generated actual XML version.
 *
 * <p>Test case for
 * <a href="https://issues.apache.org/jira/browse/CALCITE-7261">[CALCITE-7261]
 * DiffRepository generation xml doesn't respect alphabetical order</a>.
 */
public class DiffRepositoryGeneratedFileTest {
  private static final DiffRepository REPO =
      DiffRepository.lookup(DiffRepositoryGeneratedFileTest.class);

  @AfterAll
  static void tearDown() throws IOException {
    final String fileContent =
        String.join("\n", Files.readAllLines(Paths.get(REPO.logFilePath())));
    assertThat(
        fileContent, endsWith(
            "<Root>\n"
            +  "  <TestCase name=\"testDiff\">\n"
            +  "    <Resource name=\"resource\">\n"
            +  "      <![CDATA[diff]]>\n"
            +  "    </Resource>\n"
            +  "  </TestCase>\n"
            +  "  <TestCase name=\"testNull\">\n"
            +  "    <Resource name=\"resource\">\n"
            +  "      <![CDATA[null]]>\n"
            +  "    </Resource>\n"
            +  "  </TestCase>\n"
            +  "  <TestCase name=\"testmulti\">\n"
            +  "    <Resource name=\"resource\">\n"
            +  "      <![CDATA[multi]]>\n"
            +  "    </Resource>\n"
            +  "  </TestCase>\n"
            +  "</Root>"));
  }

  @Test void testDiff() {
    REPO.set("resource", "diff");
  }

  // Here method name was intentionally written in lower case to highlight
  // discrepancy between checking logic and generated logic
  @Test void testmulti() {
    REPO.set("resource", "multi");
  }

  @Test void testNull() {
    REPO.set("resource", "null");
  }
}
