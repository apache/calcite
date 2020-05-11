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
package org.apache.calcite.util.mapping;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for mappings.
 *
 * @see Mapping
 * @see Mappings
 */
class MappingTest {
  @Test void testMappings() {
    assertTrue(Mappings.isIdentity(Mappings.createIdentity(0)));
    assertTrue(Mappings.isIdentity(Mappings.createIdentity(5)));
    assertFalse(
        Mappings.isIdentity(
            Mappings.create(MappingType.PARTIAL_SURJECTION, 3, 4)));
    assertFalse(
        Mappings.isIdentity(
            Mappings.create(MappingType.PARTIAL_SURJECTION, 3, 3)));
    assertFalse(
        Mappings.isIdentity(
            Mappings.create(MappingType.PARTIAL_SURJECTION, 4, 4)));

    Mapping identity = Mappings.createIdentity(5);
    assertThat(identity.getTargetCount(), equalTo(5));
    assertThat(identity.getSourceCount(), equalTo(5));
    assertThat(identity.getTarget(0), equalTo(0));
    assertThat(identity.getTarget(1), equalTo(1));
    assertThat(identity.getTarget(4), equalTo(4));
    assertThat(identity.getSource(0), equalTo(0));
    assertThat(identity.getSource(1), equalTo(1));
    assertThat(identity.getSource(4), equalTo(4));
    assertThat(identity.getTargetOpt(4), equalTo(4));
    assertThat(identity.getSourceOpt(4), equalTo(4));

    assertThrows(IndexOutOfBoundsException.class, () -> identity.getSourceOpt(5));
    assertThrows(IndexOutOfBoundsException.class, () -> identity.getSource(5));
    assertThrows(IndexOutOfBoundsException.class, () -> identity.getTargetOpt(5));
    assertThrows(IndexOutOfBoundsException.class, () -> identity.getTarget(5));
    assertThrows(IndexOutOfBoundsException.class, () -> identity.getSourceOpt(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> identity.getSource(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> identity.getTargetOpt(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> identity.getTarget(-1));

    Mapping infiniteIdentity = Mappings.createIdentity(-1);
    assertThrows(IndexOutOfBoundsException.class, () -> infiniteIdentity.getTarget(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> infiniteIdentity.getSource(-2));
    assertThrows(IndexOutOfBoundsException.class, () -> infiniteIdentity.getTargetOpt(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> infiniteIdentity.getSourceOpt(-2));
    assertThat(infiniteIdentity.getTarget(100), equalTo(100));
    assertThat(infiniteIdentity.getSource(100), equalTo(100));
  }

  /**
   * Unit test for {@link Mappings#createShiftMapping}.
   */
  @Test void testMappingsCreateShiftMapping() {
    assertEquals(
        "[size=5, sourceCount=20, targetCount=13, elements=[6:3, 7:4, 15:10, 16:11, 17:12]]",
        Mappings.createShiftMapping(
            20,
            3, 6, 2,
            10, 15, 3).toString());

    // no triples makes for a mapping with 0 targets, 20 sources, but still
    // valid
    Mappings.TargetMapping mapping =
        Mappings.createShiftMapping(
            20);
    assertEquals("[size=0, sourceCount=20, targetCount=0, elements=[]]",
        mapping.toString());
    assertEquals(20, mapping.getSourceCount());
    assertEquals(0, mapping.getTargetCount());
  }

  /**
   * Unit test for {@link Mappings#append}.
   */
  @Test void testMappingsAppend() {
    assertTrue(
        Mappings.isIdentity(
            Mappings.append(
                Mappings.createIdentity(3),
                Mappings.createIdentity(2))));
    Mapping mapping0 = Mappings.create(MappingType.PARTIAL_SURJECTION, 5, 3);
    mapping0.set(0, 2);
    mapping0.set(3, 1);
    mapping0.set(4, 0);
    assertEquals(
        "[size=5, sourceCount=7, targetCount=5, elements=[0:2, 3:1, 4:0, 5:3, 6:4]]",
        Mappings.append(mapping0, Mappings.createIdentity(2)).toString());
  }

  /**
   * Unit test for {@link Mappings#offsetSource}.
   */
  @Test void testMappingsOffsetSource() {
    final Mappings.TargetMapping mapping =
        Mappings.target(ImmutableMap.of(0, 5, 1, 7), 2, 8);
    assertEquals(
        "[size=2, sourceCount=2, targetCount=8, elements=[0:5, 1:7]]",
        mapping.toString());
    assertEquals(2, mapping.getSourceCount());
    assertEquals(8, mapping.getTargetCount());

    final Mappings.TargetMapping mapping1 =
        Mappings.offsetSource(mapping, 3, 5);
    assertEquals(
        "[size=2, sourceCount=5, targetCount=8, elements=[3:5, 4:7]]",
        mapping1.toString());
    assertEquals(5, mapping1.getSourceCount());
    assertEquals(8, mapping1.getTargetCount());

    // mapping that extends RHS
    final Mappings.TargetMapping mapping2 =
        Mappings.offsetSource(mapping, 3, 15);
    assertEquals(
        "[size=2, sourceCount=15, targetCount=8, elements=[3:5, 4:7]]",
        mapping2.toString());
    assertEquals(15, mapping2.getSourceCount());
    assertEquals(8, mapping2.getTargetCount());

    assertThrows(IllegalArgumentException.class, () -> Mappings.offsetSource(mapping, 3, 4));
  }

  /** Unit test for {@link Mappings#source(List, int)}
   * and its converse, {@link Mappings#asList(Mappings.TargetMapping)}. */
  @Test void testSource() {
    List<Integer> targets = Arrays.asList(3, 1, 4, 5, 8);
    final Mapping mapping = Mappings.source(targets, 10);
    assertThat(mapping.getTarget(0), equalTo(3));
    assertThat(mapping.getTarget(1), equalTo(1));
    assertThat(mapping.getTarget(2), equalTo(4));
    assertThat(mapping.getTargetCount(), equalTo(10));
    assertThat(mapping.getSourceCount(), equalTo(5));

    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getTargetOpt(5));
    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getTargetOpt(10));
    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getTarget(10));
    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getTargetOpt(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getTarget(-1));

    final List<Integer> integers = Mappings.asList(mapping);
    assertThat(integers, equalTo(targets));

    final Mapping inverse = mapping.inverse();
    assertThat(inverse.toString(),
        equalTo(
            "[size=5, sourceCount=10, targetCount=5, elements=[1:1, 3:0, 4:2, 5:3, 8:4]]"));
  }

  /** Unit test for {@link Mappings#target(List, int)}. */
  @Test void testTarget() {
    List<Integer> sources = Arrays.asList(3, 1, 4, 5, 8);
    final Mapping mapping = Mappings.target(sources, 10);
    assertThat(mapping.getTarget(3), equalTo(0));
    assertThat(mapping.getTarget(1), equalTo(1));
    assertThat(mapping.getTarget(4), equalTo(2));

    assertThrows(Mappings.NoElementException.class, () -> mapping.getTarget(0));

    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getTargetOpt(10));
    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getTarget(10));
    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getTargetOpt(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getTarget(-1));

    assertThat(mapping.getTargetCount(), equalTo(5));
    assertThat(mapping.getSourceCount(), equalTo(10));

    final List<Integer> integers = Mappings.asList(mapping);
    assertThat(integers,
        equalTo(Arrays.asList(null, 1, null, 0, 2, 3, null, null, 4, null)));
  }

  /** Unit test for {@link Mappings#bijection(List)}. */
  @Test void testBijection() {
    List<Integer> targets = Arrays.asList(3, 0, 1, 2);
    final Mapping mapping = Mappings.bijection(targets);
    assertThat(mapping.size(), equalTo(4));
    assertThat(mapping.getTarget(0), equalTo(3));
    assertThat(mapping.getTarget(1), equalTo(0));
    assertThat(mapping.getTarget(2), equalTo(1));
    assertThat(mapping.getTarget(3), equalTo(2));
    assertThat(mapping.getTargetOpt(3), equalTo(2));
    assertThat(mapping.getSource(3), equalTo(0));
    assertThat(mapping.getSourceOpt(3), equalTo(0));

    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getSourceOpt(4));
    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getSource(4));
    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getTargetOpt(4));
    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getTarget(4));
    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getSourceOpt(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getSource(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getTargetOpt(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> mapping.getTarget(-1));

    assertThat(mapping.getTargetCount(), equalTo(4));
    assertThat(mapping.getSourceCount(), equalTo(4));
    assertThat(mapping.toString(), equalTo("[3, 0, 1, 2]"));
    assertThat(mapping.inverse().toString(), equalTo("[1, 2, 3, 0]"));

    // empty is OK
    final Mapping empty = Mappings.bijection(Collections.emptyList());
    assertThat(empty.size(), equalTo(0));
    assertThat(empty.iterator().hasNext(), equalTo(false));
    assertThat(empty.toString(), equalTo("[]"));

    assertThrows(Exception.class, () -> Mappings.bijection(Arrays.asList(0, 5, 1)),
        "target out of range");
    assertThrows(Exception.class, () -> Mappings.bijection(Arrays.asList(1, 0, 1)),
        "more than one permutation element maps to position 1");
  }
}
