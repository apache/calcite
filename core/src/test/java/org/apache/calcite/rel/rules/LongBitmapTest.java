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
package org.apache.calcite.rel.rules;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit test for {@link LongBitmap}.
 */
class LongBitmapTest {
  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7956">[CALCITE-7956]
   * LongBitmap.SubsetIterator produces OOM in DpHyp wide joins</a>. */
  @Test void testSubsetIteratorOnWideBitmap() {
    final long bitmap = LongBitmap.newBitmapBetween(0, 30);
    final Iterator<Long> subsets = new LongBitmap.SubsetIterator(bitmap).iterator();
    assertThat(subsets.next(), is(1L));
  }

  /** Unit test for {@link LongBitmap.SubsetIterator#iterator()}. */
  @Test void testSubsetIteratorOrder() {
    assertThat(subsets(0L), is(Collections.emptyList()));
    assertThat(subsets(0b1101L),
        is(Arrays.asList(0b1L, 0b100L, 0b101L, 0b1000L, 0b1001L, 0b1100L, 0b1101L)));
  }

  /** Unit test for {@link LongBitmap.SubsetIterator#reset()}. */
  @Test void testSubsetIteratorReset() {
    final LongBitmap.SubsetIterator subsetIterator =
        new LongBitmap.SubsetIterator(0b1101L);
    final Iterator<Long> iterator = subsetIterator.iterator();
    iterator.next();
    iterator.next();
    subsetIterator.reset();
    assertThat(toList(subsetIterator), is(subsets(0b1101L)));
  }

  private static List<Long> subsets(long bitmap) {
    return toList(new LongBitmap.SubsetIterator(bitmap));
  }

  private static List<Long> toList(Iterable<Long> iterable) {
    final List<Long> list = new ArrayList<>();
    iterable.forEach(list::add);
    return list;
  }
}
