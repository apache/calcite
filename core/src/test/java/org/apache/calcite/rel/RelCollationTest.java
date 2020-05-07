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
package org.apache.calcite.rel;

import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.rel.RelCollations.EMPTY;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link RelCollation} and {@link RelFieldCollation}.
 */
class RelCollationTest {
  /** Unit test for {@link RelCollations#contains(List, ImmutableIntList)}. */
  @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
  @Test void testCollationContains() {
    final RelCollation collation21 =
        RelCollations.of(
            new RelFieldCollation(2, RelFieldCollation.Direction.ASCENDING),
            new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING));
    assertThat(RelCollations.contains(collation21, Arrays.asList(2)), is(true));
    assertThat(RelCollations.contains(collation21, Arrays.asList(1)),
        is(false));
    assertThat(RelCollations.contains(collation21, Arrays.asList(0)),
        is(false));
    assertThat(RelCollations.contains(collation21, Arrays.asList(2, 1)),
        is(true));
    assertThat(RelCollations.contains(collation21, Arrays.asList(2, 0)),
        is(false));
    assertThat(RelCollations.contains(collation21, Arrays.asList(2, 1, 3)),
        is(false));
    assertThat(RelCollations.contains(collation21, Arrays.asList()),
        is(true));

    // if there are duplicates in keys, later occurrences are ignored
    assertThat(RelCollations.contains(collation21, Arrays.asList(2, 1, 2)),
        is(true));
    assertThat(RelCollations.contains(collation21, Arrays.asList(2, 1, 1)),
        is(true));
    assertThat(RelCollations.contains(collation21, Arrays.asList(1, 2, 1)),
        is(false));
    assertThat(RelCollations.contains(collation21, Arrays.asList(1, 1)),
        is(false));
    assertThat(RelCollations.contains(collation21, Arrays.asList(2, 2)),
        is(true));

    final RelCollation collation1 =
        RelCollations.of(
            new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING));
    assertThat(RelCollations.contains(collation1, Arrays.asList(1, 1)),
        is(true));
    assertThat(RelCollations.contains(collation1, Arrays.asList(2, 2)),
        is(false));
    assertThat(RelCollations.contains(collation1, Arrays.asList(1, 2, 1)),
        is(false));
    assertThat(RelCollations.contains(collation1, Arrays.asList()),
        is(true));
  }

  /**
   * Unit test for {@link org.apache.calcite.rel.RelCollationImpl#compareTo}.
   */
  @Test void testCollationCompare() {
    assertThat(collation(1, 2).compareTo(collation(1, 2)), equalTo(0));
    assertThat(collation(1, 2).compareTo(collation(1)), equalTo(1));
    assertThat(collation(1).compareTo(collation(1, 2)), equalTo(-1));
    assertThat(collation(1, 3).compareTo(collation(1, 2)), equalTo(1));
    assertThat(collation(0, 3).compareTo(collation(1, 2)), equalTo(-1));
    assertThat(collation().compareTo(collation(0)), equalTo(-1));
    assertThat(collation(1).compareTo(collation()), equalTo(1));
  }

  @Test void testCollationMapping() {
    final int n = 10; // Mapping source count.
    // [0]
    RelCollation collation0 = collation(0);
    assertThat(collation0.apply(mapping(n, 0)), is(collation0));
    assertThat(collation0.apply(mapping(n, 1)), is(EMPTY));
    assertThat(collation0.apply(mapping(n, 0, 1)), is(collation0));
    assertThat(collation0.apply(mapping(n, 1, 0)), is(collation(1)));
    assertThat(collation0.apply(mapping(n, 3, 1, 0)), is(collation(2)));

    // [0,1]
    RelCollation collation01 = collation(0, 1);
    assertThat(collation01.apply(mapping(n, 0)), is(collation(0)));
    assertThat(collation01.apply(mapping(n, 1)), is(EMPTY));
    assertThat(collation01.apply(mapping(n, 2)), is(EMPTY));
    assertThat(collation01.apply(mapping(n, 0, 1)), is(collation01));
    assertThat(collation01.apply(mapping(n, 1, 0)), is(collation(1, 0)));
    assertThat(collation01.apply(mapping(n, 3, 1, 0)), is(collation(2, 1)));
    assertThat(collation01.apply(mapping(n, 3, 2, 0)), is(collation(2)));

    // [2,3,4]
    RelCollation collation234 = collation(2, 3, 4);
    assertThat(collation234.apply(mapping(n, 0)), is(EMPTY));
    assertThat(collation234.apply(mapping(n, 1)), is(EMPTY));
    assertThat(collation234.apply(mapping(n, 2)), is(collation(0)));
    assertThat(collation234.apply(mapping(n, 3)), is(EMPTY));
    assertThat(collation234.apply(mapping(n, 4)), is(EMPTY));
    assertThat(collation234.apply(mapping(n, 5)), is(EMPTY));
    assertThat(collation234.apply(mapping(n, 0, 1, 2)), is(collation(2)));
    assertThat(collation234.apply(mapping(n, 3, 2)), is(collation(1, 0)));
    assertThat(collation234.apply(mapping(n, 3, 2, 4)), is(collation(1, 0, 2)));
    assertThat(collation234.apply(mapping(n, 3, 2, 4)), is(collation(1, 0, 2)));
    assertThat(collation234.apply(mapping(n, 4, 3, 2, 0)), is(collation(2, 1, 0)));
    assertThat(collation234.apply(mapping(n, 3, 4, 0)), is(EMPTY));

    // [9] , 9 < mapping.sourceCount()
    RelCollation collation9 = collation(n - 1);
    assertThat(collation9.apply(mapping(n, 0)), is(EMPTY));
    assertThat(collation9.apply(mapping(n, 1)), is(EMPTY));
    assertThat(collation9.apply(mapping(n, 2)), is(EMPTY));
    assertThat(collation9.apply(mapping(n, n - 1)), is(collation(0)));
  }

  private static RelCollation collation(int... ordinals) {
    final List<RelFieldCollation> list = new ArrayList<>();
    for (int ordinal : ordinals) {
      list.add(new RelFieldCollation(ordinal));
    }
    return RelCollations.of(list);
  }

  private static Mapping mapping(int sourceCount, int... sources) {
    return Mappings.target(ImmutableIntList.of(sources), sourceCount);
  }
}
