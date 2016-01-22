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
package org.apache.calcite.avatica.metrics.dropwizard3;

import org.apache.calcite.avatica.metrics.dropwizard3.DropwizardTimer.DropwizardContext;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test class for {@link DropwizardTimer}
 */
public class DropwizardTimerTest {

  private Timer timer;
  private Context context;

  @Before public void setup() {
    this.timer = Mockito.mock(Timer.class);
    this.context = Mockito.mock(Context.class);
  }

  @Test public void test() {
    DropwizardTimer dwTimer = new DropwizardTimer(timer);

    Mockito.when(timer.time()).thenReturn(context);

    DropwizardContext dwContext = dwTimer.start();

    dwContext.close();

    Mockito.verify(timer).time();
    Mockito.verify(context).stop();
  }

}

// End DropwizardTimerTest.java
