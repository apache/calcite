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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test of {@link SqlNode} and other SQL AST classes.
 */
class SqlNodeTest {
  @Test void testSqlNodeList() {
    SqlNodeList emptyList = new SqlNodeList(SqlParserPos.ZERO);
    checkLists(emptyList, emptyList.getList(), 0);
  }

  private void checkLists(List<SqlNode> list0, List<SqlNode> list1,
      int depth) {
    assertThat(list0.hashCode(), is(list1.hashCode()));
    assertThat(list0.equals(list1), is(true));
    assertThat(list0.size(), is(list1.size()));
    assertThat(list0.isEmpty(), is(list1.isEmpty()));
    if (!list0.isEmpty()) {
      assertThat(list0.get(0), sameInstance(list1.get(0)));
      assertThat(Util.last(list0), sameInstance(Util.last(list1)));
      if (depth == 0) {
        checkLists(Util.skip(list0, 1), Util.skip(list1, 1), depth + 1);
      }
    }
    assertThat(collect(list0), is(list1));
    assertThat(collect(list1), is(list0));
  }

  private static <E> List<E> collect(Iterable<E> iterable) {
    final List<E> list = new ArrayList<>();
    for (E e: iterable) {
      list.add(e);
    }
    return list;
  }
}
