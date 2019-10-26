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

import org.apache.calcite.rel.RelNode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test for converting a Pig script file.
 */
public class PigScriptTest extends PigRelTestBase {
  private static String projectRootDir;
  private static String dataFile;

  @BeforeClass
  public static void setUpOnce() throws IOException {
    projectRootDir = System.getProperty("user.dir");
    dataFile = projectRootDir + "/src/test/resources/input.data";
    List<String> lines = Arrays.asList("yahoo 10", "twitter 3", "facebook 10",
        "yahoo 15", "facebook 5", "twitter 2");
    Files.write(Paths.get(dataFile), lines, StandardCharsets.UTF_8);
  }

  @AfterClass
  public static void testClean() throws IOException {
    Files.delete(Paths.get(dataFile));
  }

  @Test
  public void testReadScript() throws IOException {
    Map<String, String> params = new HashMap<>();
    params.put("input", dataFile);
    params.put("output", "outputFile");

    final String pigFile = projectRootDir + "/src/test/resources/testPig.pig";
    final RelNode rel = converter.pigScript2Rel(pigFile, params, true).get(0);

    final String dataFile = projectRootDir + "/src/test/resources/input.data";
    String expectedPlan = ""
        + "LogicalSort(sort0=[$1], dir0=[DESC], fetch=[5])\n"
        + "  LogicalProject(query=[$0], count=[CAST($1):BIGINT])\n"
        + "    LogicalAggregate(group=[{0}], agg#0=[SUM($1)])\n"
        + "      LogicalTableScan(table=[[" + dataFile + "]])\n";

    assertThat(rel, hasTree(expectedPlan));
  }
}

// End PigScriptTest.java
