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
package org.apache.calcite.access;

import org.junit.Assert;
import org.junit.Test;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * Testing against thread local implementation of fairy
 */
public class CalcitePrincipalFairyTest {

  private final CalcitePrincipalFairy tested = CalcitePrincipalFairy.INSTANCE;

  @Test public void testSouldRegisterPrincipalOnCurrentThreadOnly() {
    // given
    CalcitePrincipal principal = mock(CalcitePrincipal.class);
    given(principal.getName()).willReturn("SOME_USER");
    // when
    tested.register(principal);
    // then
    Assert.assertEquals("Principal should be the same", principal, tested.get());
    new Thread() {
      @Override public void run() {
        Assert.assertNull("Principal should be empty in another thread", tested.get());
      }

    }.start();
  }
}

// End CalcitePrincipalFairyTest.java
