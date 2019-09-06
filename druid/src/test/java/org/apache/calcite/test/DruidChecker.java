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

import org.apache.calcite.adapter.druid.DruidQuery;

import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * A consumer that checks that a particular Druid query is generated to implement a query.
 */
class DruidChecker implements Consumer<List> {
  private final String[] lines;
  private final boolean replaceSingleWithDoubleQuotes;

  DruidChecker(String... lines) {
    this(true, lines);
  }

  DruidChecker(boolean replaceSingleWithDoubleQuotes, String... lines) {
    this.replaceSingleWithDoubleQuotes = replaceSingleWithDoubleQuotes;
    this.lines = lines;
  }

  @Override public void accept(final List list) {
    assertThat(list.size(), is(1));
    DruidQuery.QuerySpec querySpec = (DruidQuery.QuerySpec) list.get(0);
    for (String line : lines) {
      final String s =
          replaceSingleWithDoubleQuotes ? line.replace('\'', '"') : line;
      assertThat(querySpec.getQueryString(null, -1), containsString(s));
    }
  }
}

// End DruidChecker.java
