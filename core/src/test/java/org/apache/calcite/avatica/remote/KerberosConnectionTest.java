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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.remote.KerberosConnection.RenewalTask;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.Locale;
import java.util.Map.Entry;

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.nullable;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test case for KerberosConnection
 */
public class KerberosConnectionTest {

  @Test(expected = NullPointerException.class) public void testNullArgs() {
    new KerberosConnection(null, null);
  }

  @Test public void testThreadConfiguration() {
    KerberosConnection krbUtil = new KerberosConnection("foo", new File("/bar.keytab"));
    Subject subject = new Subject();
    LoginContext context = Mockito.mock(LoginContext.class);

    Entry<RenewalTask, Thread> entry = krbUtil.createRenewalThread(context, subject, 10);
    assertNotNull("RenewalTask should not be null", entry.getKey());
    Thread t = entry.getValue();
    assertTrue("Thread name should contain 'Avatica', but is '" + t.getName() + "'",
        t.getName().contains("Avatica"));
    assertTrue(t.isDaemon());
    assertNotNull(t.getUncaughtExceptionHandler());
  }

  @Test public void noPreviousContextOnLogin() throws Exception {
    KerberosConnection krbUtil = mock(KerberosConnection.class);
    Subject subject = new Subject();
    Subject loggedInSubject = new Subject();
    Configuration conf = mock(Configuration.class);
    LoginContext context = mock(LoginContext.class);

    // Call the real login(LoginContext, Configuration, Subject) method
    when(krbUtil.login(nullable(LoginContext.class), any(Configuration.class), any(Subject.class)))
        .thenCallRealMethod();
    // Return a fake LoginContext
    when(krbUtil.createLoginContext(conf)).thenReturn(context);
    // Return a fake Subject from that fake LoginContext
    when(context.getSubject()).thenReturn(loggedInSubject);

    Entry<LoginContext, Subject> pair = krbUtil.login(null, conf, subject);

    // Verify we get the fake LoginContext and Subject
    assertEquals(context, pair.getKey());
    assertEquals(loggedInSubject, pair.getValue());

    // login should be called on the LoginContext
    verify(context).login();
  }

  @Test public void previousContextLoggedOut() throws Exception {
    KerberosConnection krbUtil = mock(KerberosConnection.class);
    Subject subject = new Subject();
    Subject loggedInSubject = new Subject();
    Configuration conf = mock(Configuration.class);
    LoginContext originalContext = mock(LoginContext.class);
    LoginContext context = mock(LoginContext.class);

    // Call the real login(LoginContext, Configuration, Subject) method
    when(krbUtil.login(any(LoginContext.class), any(Configuration.class), any(Subject.class)))
        .thenCallRealMethod();
    // Return a fake LoginContext
    when(krbUtil.createLoginContext(conf)).thenReturn(context);
    // Return a fake Subject from that fake LoginContext
    when(context.getSubject()).thenReturn(loggedInSubject);

    Entry<LoginContext, Subject> pair = krbUtil.login(originalContext, conf, subject);

    // Verify we get the fake LoginContext and Subject
    assertEquals(context, pair.getKey());
    assertEquals(loggedInSubject, pair.getValue());

    verify(originalContext).logout();

    // login should be called on the LoginContext
    verify(context).login();
  }

  @Test public void testTicketRenewalTime() {
    RenewalTask renewal = mock(RenewalTask.class);
    when(renewal.shouldRenew(any(long.class), any(long.class), any(long.class)))
        .thenCallRealMethod();

    long start = 0;
    long end = 200;
    long now = 100;
    assertFalse(renewal.shouldRenew(start, end, now));

    // Renewal should happen at 80%
    start = 0;
    end = 100;
    now = 80;
    assertTrue(renewal.shouldRenew(start, end, now));

    start = 5000;
    // One day
    end = start + 1000 * 60 * 60 * 24;
    // Ten minutes prior to expiration
    now = end - 1000 * 60 * 10;
    assertTrue(
        String.format(Locale.ROOT, "start=%d, end=%d, now=%d", start, end, now),
        renewal.shouldRenew(start, end, now));
  }
}

// End KerberosConnectionTest.java
