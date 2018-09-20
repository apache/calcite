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
package org.apache.calcite.avatica.metrics.dropwizard;

import com.codahale.metrics.Counter;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link DropwizardCounter}.
 */
public class DropwizardCounterTest {

  private Counter counter;

  @Before public void setup() {
    this.counter = new Counter();
  }

  @Test public void testCounting() {
    DropwizardCounter dwCounter = new DropwizardCounter(counter);

    dwCounter.increment();
    assertEquals(1L, counter.getCount());
    dwCounter.increment();
    assertEquals(2L, counter.getCount());
    dwCounter.increment(2L);
    assertEquals(4L, counter.getCount());
    dwCounter.increment(-1L);
    assertEquals(3L, counter.getCount());

    dwCounter.decrement();
    assertEquals(2L, counter.getCount());
    dwCounter.decrement();
    assertEquals(1L, counter.getCount());
    dwCounter.decrement(4L);
    assertEquals(-3L, counter.getCount());
    dwCounter.decrement(-3L);
    assertEquals(0L, counter.getCount());
  }

}

// End DropwizardCounterTest.java
