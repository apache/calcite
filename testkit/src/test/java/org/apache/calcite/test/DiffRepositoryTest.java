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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link DiffRepository} class.
 */
public class DiffRepositoryTest {

  @Test void testAssertEqualsUpdatesLogFileUponFailure() throws IOException {
    DiffRepository r = DiffRepository.lookup(DiffRepositoryTest.class);
    final String actual = "Random sentence not present in resources file";
    boolean assertPassed = false;
    try {
      r.assertEquals("content", "${content}", actual);
      assertPassed = true;
    } catch (AssertionError e) {
      String logContent = String.join("", Files.readAllLines(Paths.get(r.logFilePath())));
      assertThat(logContent, containsString(actual));
    }
    assertThat("First assertion must always fail", assertPassed, is(false));
  }

  @Test void testMethodOnlyExistsInXml() {
    boolean assertPassed = false;
    DiffRepository r = DiffRepository.lookup(DiffRepositoryTest.class);
    final String actual = "testMethodOnlyExistsInXml1";
    try {
      r.checkActualAndReferenceFiles();
      assertPassed = true;
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString(actual));
    }
    assertThat("First assertion must always fail", assertPassed, is(false));
  }
}
