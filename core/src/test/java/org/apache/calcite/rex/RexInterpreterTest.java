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
package org.apache.calcite.rex;

import org.apache.calcite.rel.metadata.NullSentinel;

import org.junit.jupiter.api.Test;

import static org.apache.calcite.rex.RexInterpreter.evaluate;

import static org.junit.jupiter.api.Assertions.assertEquals;

import static java.util.Collections.emptyMap;

/**
 * Test for {@link RexInterpreter}.
 */
public final class RexInterpreterTest extends RexProgramBuilderBase {

  @Test void testSearchWithNumerics() {
    RexNode l100_35 = literal(100.35);
    RexNode l222_34 = literal(222.34);
    RexNode l350_34 = literal(350.34);
    RexNode l100 = literal(100);
    RexNode l200 = literal(200);
    RexNode l300 = literal(300);
    // Exact with exact numerics types
    assertEquals(true, evaluate(in(l100, l100, l200), emptyMap()));
    assertEquals(false, evaluate(in(l300, l100, l200), emptyMap()));
    assertEquals(true, evaluate(in(l200, nullInt, l200), emptyMap()));
    assertEquals(NullSentinel.INSTANCE, evaluate(in(l100, nullInt, l200), emptyMap()));
    assertEquals(NullSentinel.INSTANCE, evaluate(in(nullInt, l100, l200), emptyMap()));
    // Approximate with exact numerics types
    assertEquals(false, evaluate(in(l100_35, l100, l200), emptyMap()));
    assertEquals(NullSentinel.INSTANCE, evaluate(in(l100_35, nullInt, l200), emptyMap()));
    assertEquals(NullSentinel.INSTANCE, evaluate(in(nullDouble, l100, l200), emptyMap()));
    // Approximate with approximate numerics types
    assertEquals(true, evaluate(in(l100_35, l100_35, l222_34), emptyMap()));
    assertEquals(false, evaluate(in(l350_34, l100_35, l222_34), emptyMap()));
    assertEquals(true, evaluate(in(l100_35, l100_35, nullDouble), emptyMap()));
    assertEquals(NullSentinel.INSTANCE, evaluate(in(l100_35, nullDouble, l222_34), emptyMap()));
    assertEquals(NullSentinel.INSTANCE, evaluate(in(nullDouble, l100_35, l222_34), emptyMap()));
    // Exact with approximate numerics types
    assertEquals(false, evaluate(in(l100, l100_35, l222_34), emptyMap()));
    assertEquals(NullSentinel.INSTANCE, evaluate(in(l100, nullDouble, l222_34), emptyMap()));
    assertEquals(NullSentinel.INSTANCE, evaluate(in(nullInt, l100_35, l222_34), emptyMap()));
  }
}
