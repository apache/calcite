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

import com.codahale.metrics.Histogram;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test class for {@link DropwizardHistogram}.
 */
public class DropwizardHistogramTest {

  private Histogram histogram;

  @Before public void setup() {
    this.histogram = Mockito.mock(Histogram.class);
  }

  @Test public void test() {
    DropwizardHistogram dwHistogram = new DropwizardHistogram(histogram);

    dwHistogram.update(10);

    dwHistogram.update(100L);

    Mockito.verify(histogram).update(10);
    Mockito.verify(histogram).update(100L);
  }

}

// End DropwizardHistogramTest.java
