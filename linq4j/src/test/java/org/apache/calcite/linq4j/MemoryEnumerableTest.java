/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.linq4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MemoryEnumerableTest {

  @Test
  public void testHistoryAndFuture() {
    final Enumerable<Integer> input = Linq4j.asEnumerable(IntStream.range(0, 100).boxed().collect(Collectors.toList()));

    final MemoryEnumerable<Integer> integers = new MemoryEnumerable<>(input, 5, 1);
    final Enumerator<MemoryFactory.Memory<Integer>> enumerator = integers.enumerator();

    final List<MemoryFactory.Memory<Integer>> results = new ArrayList<>();
    while (enumerator.moveNext()) {
      final MemoryFactory.Memory<Integer> current = enumerator.current();
      results.add(current);
    }

    assertEquals(100, results.size());
    // First entry
    assertEquals( 0, (int)results.get(0).get());
    assertEquals( 1, (int)results.get(0).get(1));
    assertNull(results.get(0).get(-2));
    // Last entry
    assertEquals( 99, (int)results.get(99).get());
    assertEquals( 97, (int)results.get(99).get(-2));
    assertNull(results.get(99).get(1));
  }

  @Test
  public void testFiniteInteger() {
    final MemoryEnumerable.FiniteInteger finiteInteger = new MemoryEnumerable.FiniteInteger(4, 5);
    assertEquals("4 (5)", finiteInteger.toString());

    final MemoryEnumerable.FiniteInteger plus = finiteInteger.plus(1);
    assertEquals("0 (5)", plus.toString());

    final MemoryEnumerable.FiniteInteger minus = finiteInteger.plus(-6);
    assertEquals("3 (5)", minus.toString());
  }

}
