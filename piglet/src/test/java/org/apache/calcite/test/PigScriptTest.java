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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Util;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test for converting a pig script file.
 */
public class PigScriptTest extends PigRelTestBase {
  static String projectRootDir;

  @BeforeClass
  public static void setUpOnce() {
    projectRootDir = System.getProperty("user.dir");
  }

  @Test
  public void testReadScript() throws IOException {
    Map<String, String> params = new HashMap<>();
    String table = projectRootDir + "/src/test/resources/input.data";

    params.put("input", table);
    params.put("output", "outputFile");

    RelNode rel = converter.pigScript2Rel(projectRootDir + "/src/test/resources/testPig.pig",
        params, true).get(0);

    String epxectedPlan = ""
                              + "LogicalSort(sort0=[$1], dir0=[DESC], fetch=[5])\n"
                              + "  LogicalProject(query=[$0], count=[CAST($1):BIGINT])\n"
                              + "    LogicalAggregate(group=[{0}], agg#0=[SUM($1)])\n"
                              + "      LogicalTableScan(table=[[" + projectRootDir
                              + "/src/test/resources/input.data]])\n";

    assertThat(Util.toLinux(RelOptUtil.toString(rel)), is(epxectedPlan));
  }
}

// End PigScriptTest.java
