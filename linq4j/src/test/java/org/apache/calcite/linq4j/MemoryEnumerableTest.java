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
package org.apache.calcite.linq4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/** Tests for {@link org.apache.calcite.linq4j.MemoryEnumerable} */
public class MemoryEnumerableTest {

  @Test public void testHistoryAndFuture() {
    final Enumerable<Integer> input =
        Linq4j.asEnumerable(IntStream.range(0, 100)
            .boxed().collect(Collectors.toList()));

    final MemoryEnumerable<Integer> integers = new MemoryEnumerable<>(input, 5, 1);
    final Enumerator<MemoryFactory.Memory<Integer>> enumerator = integers.enumerator();

    final List<MemoryFactory.Memory<Integer>> results = new ArrayList<>();
    while (enumerator.moveNext()) {
      final MemoryFactory.Memory<Integer> current = enumerator.current();
      results.add(current);
    }

    assertThat(results.size(), is(100));
    // First entry
    assertThat((int) results.get(0).get(), is(0));
    assertThat((int) results.get(0).get(1), is(1));
    assertThat(results.get(0).get(-2), nullValue());
    // Last entry
    assertThat((int) results.get(99).get(), is(99));
    assertThat((int) results.get(99).get(-2), is(97));
    assertThat(results.get(99).get(1), nullValue());
  }

  @Test public void testModularInteger() {
    final ModularInteger modularInteger = new ModularInteger(4, 5);
    assertThat(modularInteger.toString(), is("4 mod 5"));

    final ModularInteger plus = modularInteger.plus(1);
    assertThat(plus.toString(), is("0 mod 5"));

    final ModularInteger minus = modularInteger.plus(-6);
    assertThat(minus.toString(), is("3 mod 5"));
  }
}

// End MemoryEnumerableTest.java
