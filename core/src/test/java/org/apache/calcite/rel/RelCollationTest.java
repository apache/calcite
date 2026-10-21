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

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.apache.calcite.rel.RelCollations.EMPTY;
import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.calcite.rel.RelFieldCollation.Direction.CLUSTERED;
import static org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING;
import static org.apache.calcite.rel.RelFieldCollation.Direction.STRICTLY_ASCENDING;
import static org.apache.calcite.rel.RelFieldCollation.Direction.STRICTLY_DESCENDING;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
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
            new RelFieldCollation(2, ASCENDING),
            new RelFieldCollation(1, DESCENDING));
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
            new RelFieldCollation(1, DESCENDING));
    assertThat(RelCollations.contains(collation1, Arrays.asList(1, 1)),
        is(true));
    assertThat(RelCollations.contains(collation1, Arrays.asList(2, 2)),
        is(false));
    assertThat(RelCollations.contains(collation1, Arrays.asList(1, 2, 1)),
        is(false));
    assertThat(RelCollations.contains(collation1, Arrays.asList()),
        is(true));
  }

  /** Unit test for {@link RelCollations#collationsContainKeysOrderless(List, List)}. */
  @Test void testCollationsContainKeysOrderless() {
    final List<RelCollation> collations = Lists.newArrayList(collation(2, 3, 1));
    assertThat(
        RelCollations.collationsContainKeysOrderless(
        collations, Arrays.asList(2, 2)), is(true));
    assertThat(
        RelCollations.collationsContainKeysOrderless(
        collations, Arrays.asList(2, 3)), is(true));
    assertThat(
        RelCollations.collationsContainKeysOrderless(
        collations, Arrays.asList(3, 2)), is(true));
    assertThat(
        RelCollations.collationsContainKeysOrderless(
        collations, Arrays.asList(3, 2, 1)), is(true));
    assertThat(
        RelCollations.collationsContainKeysOrderless(
        collations, Arrays.asList(3, 2, 1, 0)), is(false));
    assertThat(
        RelCollations.collationsContainKeysOrderless(
        collations, Arrays.asList(2, 3, 0)), is(false));
    assertThat(
        RelCollations.collationsContainKeysOrderless(
        collations, Arrays.asList(1)), is(false));
    assertThat(
        RelCollations.collationsContainKeysOrderless(
        collations, Arrays.asList(3, 1)), is(false));
    assertThat(
        RelCollations.collationsContainKeysOrderless(
        collations, Arrays.asList(0)), is(false));
  }

  /** Unit test for {@link RelCollations#keysContainCollationsOrderless(List, List)}. */
  @Test void testKeysContainCollationsOrderless() {
    final List<Integer> keys = Arrays.asList(2, 3, 1);
    assertThat(
        RelCollations.keysContainCollationsOrderless(
            keys, Lists.newArrayList(collation(2, 2))), is(true));
    assertThat(
        RelCollations.keysContainCollationsOrderless(
            keys, Lists.newArrayList(collation(2, 3))), is(true));
    assertThat(
        RelCollations.keysContainCollationsOrderless(
            keys, Lists.newArrayList(collation(3, 2))), is(true));
    assertThat(
        RelCollations.keysContainCollationsOrderless(
            keys, Lists.newArrayList(collation(3, 2, 1))), is(true));
    assertThat(
        RelCollations.keysContainCollationsOrderless(
            keys, Lists.newArrayList(collation(3, 2, 1, 0))), is(false));
    assertThat(
        RelCollations.keysContainCollationsOrderless(
            keys, Lists.newArrayList(collation(2, 3, 0))), is(false));
    assertThat(
        RelCollations.keysContainCollationsOrderless(
            keys, Lists.newArrayList(collation(1))), is(true));
    assertThat(
        RelCollations.keysContainCollationsOrderless(
            keys, Lists.newArrayList(collation(3, 1))), is(true));
    assertThat(
        RelCollations.keysContainCollationsOrderless(
            keys, Lists.newArrayList(collation(0))), is(false));
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

  /**
   * Unit test for {@link RelFieldCollation.Direction#reverse()}.
   */
  @Test void testDirectionReverse() {
    assertThat(ASCENDING.reverse(), is(DESCENDING));
    assertThat(DESCENDING.reverse(), is(ASCENDING));
    assertThat(STRICTLY_ASCENDING.reverse(), is(STRICTLY_DESCENDING));
    assertThat(STRICTLY_DESCENDING.reverse(), is(STRICTLY_ASCENDING));
    assertThat(CLUSTERED.reverse(), is(CLUSTERED));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7872">[CALCITE-7872]
   * RelCollations and RelCollationImpl can deadlock during class
   * initialization</a>.
   *
   * <p>Tests that the deprecated constants {@code RelCollationImpl.EMPTY} and
   * {@code RelCollationImpl.PRESERVE} are the same objects as
   * {@link RelCollations#EMPTY} and {@link RelCollations#PRESERVE}, whichever
   * of the two classes is initialized first. They used to be null if
   * {@code RelCollations} was initialized first. */
  @Test void testDeprecatedConstantsInEitherInitializationOrder()
      throws Exception {
    final List<Class<?>> classes =
        Arrays.asList(RelCollations.class, RelCollationImpl.class);
    for (List<Class<?>> order : Arrays.asList(classes, Lists.reverse(classes))) {
      final ClassLoader loader = new IsolatingClassLoader();
      for (Class<?> c : order) {
        Class.forName(c.getName(), true, loader);
      }
      final Class<?> relCollations =
          loader.loadClass(RelCollations.class.getName());
      final Class<?> relCollationImpl =
          loader.loadClass(RelCollationImpl.class.getName());
      for (String name : Arrays.asList("EMPTY", "PRESERVE")) {
        final Object expected = relCollations.getField(name).get(null);
        assertThat(expected, notNullValue());
        assertThat(relCollationImpl.getField(name).get(null),
            sameInstance(expected));
      }
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7872">[CALCITE-7872]
   * RelCollations and RelCollationImpl can deadlock during class
   * initialization</a>.
   *
   * <p>Tests that two threads can initialize {@link RelCollations} and
   * {@link RelCollationImpl} at the same time. The static initializers of the
   * two classes used to depend on each other, so the threads deadlocked. */
  @Test void testConcurrentClassInitialization() throws Exception {
    for (int i = 0; i < 5; i++) {
      final ClassLoader loader = new IsolatingClassLoader();
      final CyclicBarrier barrier = new CyclicBarrier(2);
      final List<FutureTask<Class<?>>> tasks = new ArrayList<>();
      for (Class<?> c
          : Arrays.asList(RelCollations.class, RelCollationImpl.class)) {
        final FutureTask<Class<?>> task =
            new FutureTask<>(() -> {
              barrier.await();
              return Class.forName(c.getName(), true, loader);
            });
        final Thread thread = new Thread(task);
        // A thread stuck in a static initializer cannot be stopped
        thread.setDaemon(true);
        thread.start();
        tasks.add(task);
      }
      for (FutureTask<Class<?>> task : tasks) {
        // Throws TimeoutException if the threads deadlock
        assertThat(task.get(10, TimeUnit.SECONDS), notNullValue());
      }
    }
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

  /** Class loader that defines its own copy of each Calcite class, so that the
   * static initializers of those classes run again, and delegates other
   * classes to its parent. */
  private static class IsolatingClassLoader extends ClassLoader {
    private final ClassLoader parent;

    IsolatingClassLoader() {
      this(RelCollationTest.class.getClassLoader());
    }

    private IsolatingClassLoader(ClassLoader parent) {
      super(parent);
      this.parent = parent;
    }

    @Override protected Class<?> loadClass(String name, boolean resolve)
        throws ClassNotFoundException {
      if (!name.startsWith("org.apache.calcite.")) {
        return super.loadClass(name, resolve);
      }
      synchronized (getClassLoadingLock(name)) {
        Class<?> c = findLoadedClass(name);
        if (c == null) {
          final String resource = name.replace('.', '/') + ".class";
          try (InputStream in = parent.getResourceAsStream(resource)) {
            if (in == null) {
              throw new ClassNotFoundException(name);
            }
            final byte[] bytes = ByteStreams.toByteArray(in);
            c = defineClass(name, bytes, 0, bytes.length);
          } catch (IOException e) {
            throw new ClassNotFoundException(name, e);
          }
        }
        if (resolve) {
          resolveClass(c);
        }
        return c;
      }
    }
  }
}
