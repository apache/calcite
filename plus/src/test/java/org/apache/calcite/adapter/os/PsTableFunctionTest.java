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
package org.apache.calcite.adapter.os;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for the ps (process status) table function.
 */
class PsTableFunctionTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6388">[CALCITE-6388]
   * PsTableFunction throws NumberFormatException when the 'user' column has spaces</a>.
   */
  @Test void testPsInfoParsing() {
    final List<String> input = new ArrayList<>();
    input.add("startup user     56399     1 56399    0 S      0.0  0.0 410348128   6672 ??"
        + "        3:25PM   0:00.22   501   501      0 /usr/lib exec/trustd");
    input.add("root                 1   107   107    0 Ss     0.0  0.0 410142784   4016 ??"
        + "       11Apr24   0:52.32     0     0      0 "
        + "/System/Library/PrivateFrameworks/Uninstall.framework/Resources/uninstalld");
    input.add("user.name     1  1661  1661    0 S      0.7  0.2 412094800  75232 ??       "
        + "11Apr24 325:33.63 775020228 775020228      0 "
        + "/System/Library/CoreServices/ControlCenter app/Contents/MacOS/ControlCenter");

    final List<List<Object>> output =
        ImmutableList.of(
            Arrays.asList("startup user", 56399, 1, 56399, 0, "S", 0, 0, "410348128", "6672", "??",
            "3:25PM", 220L, "501", "501", "0", "/usr/lib exec/trustd"),
        Arrays.asList("root", 1, 107, 107, 0, "Ss", 0, 0, "410142784", "4016", "??",
            "11Apr24", 52320L, "0", "0", "0",
            "/System/Library/PrivateFrameworks/Uninstall.framework/Resources/uninstalld"),
        Arrays.asList("user.name", 1, 1661, 1661, 0, "S", 7, 2, "412094800", "75232", "??",
            "11Apr24", 19533630L, "775020228", "775020228", "0",
            "/System/Library/CoreServices/ControlCenter app/Contents/MacOS/ControlCenter"));

    final Map<String, List<Object>> testValues = new HashMap<>();
    for (int i = 0; i < input.size(); i++) {
      testValues.put(input.get(i), output.get(i));
    }

    final PsTableFunction.LineParser psLineParser = new PsTableFunction.LineParser();
    for (Map.Entry<String, List<Object>> e : testValues.entrySet()) {
      assertThat(psLineParser.apply(e.getKey()), is(e.getValue().toArray()));
    }
  }
}
