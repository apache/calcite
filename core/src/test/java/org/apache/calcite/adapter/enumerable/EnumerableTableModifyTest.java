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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.Linq4j;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for {@link EnumerableTableModify} row-consumption semantics. */
class EnumerableTableModifyTest {

  @Test void testApplyUpdateOneToOneUpdatesOnlyFirstNMatchingRows() {
    final List<Object[]> sink = new ArrayList<>();
    sink.add(new Object[] {1, 10});
    sink.add(new Object[] {1, 10});
    sink.add(new Object[] {1, 10});
    sink.add(new Object[] {2, 20});

    // Source row layout: [original_i, original_j, new_j].
    final List<Object[]> source =
          Arrays.asList(new Object[] {1, 10, 100}, new Object[] {1, 10, 200});

    final long count =
        EnumerableTableModify.applyUpdateOneToOne(Linq4j.asEnumerable(source), sink, 2,
            new int[] {1});

    assertThat(count, is(2L));
    assertThat(toValueRows(sink),
        is(
            Arrays.asList(
            Arrays.asList(1, 100),
            Arrays.asList(1, 200),
            Arrays.asList(1, 10),
            Arrays.asList(2, 20))));
  }

  @Test void testApplyDeleteDoesNotSkipRowsWhenSourceBackedBySink() {
    final List<Object> sink = new ArrayList<>(Arrays.asList(1, 1, 1));
    final List<Object[]> source =
        Arrays.asList(new Object[] {1}, new Object[] {1}, new Object[] {1});

    EnumerableTableModify.applyDeleteRowsByKey(
        Linq4j.asEnumerable(source), sink, row -> new Object[] {row});

    assertThat(sink, is(Collections.emptyList()));
  }

  private static List<List<Object>> toValueRows(List<Object[]> rows) {
    final List<List<Object>> valueRows = new ArrayList<>();
    for (Object[] row : rows) {
      valueRows.add(Arrays.asList(row));
    }
    return valueRows;
  }
}
