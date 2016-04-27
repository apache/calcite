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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.Thread.UncaughtExceptionHandler;
import java.security.Principal;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * A utility to perform Kerberos logins and renewals.
 */
public class KerberosConnection {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosConnection.class);

  private static final String IBM_KRB5_LOGIN_MODULE =
      "com.ibm.security.auth.module.Krb5LoginModule";
  private static final String SUN_KRB5_LOGIN_MODULE =
      "com.sun.security.auth.module.Krb5LoginModule";
  private static final String JAAS_CONF_NAME = "AvaticaKeytabConf";
  private static final String RENEWAL_THREAD_NAME = "Avatica Kerberos Renewal Thread";

  /** The percentage of the Kerberos ticket's lifetime which we should start trying to renew it */
  public static final float PERCENT_OF_LIFETIME_TO_RENEW = 0.80f;
  /** How long should we sleep between checks to renew the Kerberos ticket */
  public static final long RENEWAL_PERIOD = 30L;

  private final String principal;
  private final Configuration jaasConf;
  private Subject subject;
  private RenewalTask renewalTask;
  private Thread renewalThread;

  /**
   * Constructs an instance.
   *
   * @param principal The Kerberos principal
   * @param keytab The keytab containing keys for the Kerberos principal
   */
  public KerberosConnection(String principal, File keytab) {
    this.principal = Objects.requireNonNull(principal);
    this.jaasConf = new KeytabJaasConf(principal, Objects.requireNonNull(keytab));
  }

  public synchronized Subject getSubject() {
    return this.subject;
  }

  /**
   * Perform a Kerberos login and launch a daemon thread to periodically perfrom renewals of that
   * Kerberos login. Exceptions are intentionally caught and rethrown as unchecked exceptions as
   * there is nothing Avatica itself can do if the Kerberos login fails.
   *
   * @throws RuntimeException If the Kerberos login fails
   */
  public synchronized void login() {
    final Entry<LoginContext, Subject> securityMaterial = performKerberosLogin();
    subject = securityMaterial.getValue();
    // Launch a thread to periodically perform renewals
    final Entry<RenewalTask, Thread> renewalMaterial = createRenewalThread(
        securityMaterial.getKey(), subject, KerberosConnection.RENEWAL_PERIOD);
    renewalTask = renewalMaterial.getKey();
    renewalThread = renewalMaterial.getValue();
    renewalThread.start();
  }

  /**
   * Performs a Kerberos login given the {@code principal} and {@code keytab}.
   *
   * @return The {@code Subject} and {@code LoginContext} from the successful login.
   * @throws RuntimeException if the login failed
   */
  Entry<LoginContext, Subject> performKerberosLogin() {
    // Loosely based on Apache Kerby's JaasKrbUtil class
    // Synchronized by the caller

    // Create a KerberosPrincipal given the principal.
    final Set<Principal> principals = new HashSet<Principal>();
    principals.add(new KerberosPrincipal(principal));

    final Subject subject = new Subject(false, principals, new HashSet<Object>(),
        new HashSet<Object>());

    try {
      return login(null, jaasConf, subject);
    } catch (Exception e) {
      throw new RuntimeException("Failed to perform Kerberos login");
    }
  }

  /**
   * Performs a kerberos login, possibly logging out first.
   *
   * @param prevContext The LoginContext from the previous login, or null
   * @param conf JAAS Configuration object
   * @param subject The JAAS Subject
   * @return The context and subject from the login
   * @throws LoginException If the login failed.
   */
  Entry<LoginContext, Subject> login(LoginContext prevContext, Configuration conf,
      Subject subject) throws LoginException {
    // Is synchronized by the caller

    // If a context was provided, perform a logout first
    if (null != prevContext) {
      prevContext.logout();
    }

    // Create a LoginContext given the Configuration and Subject
    LoginContext loginContext = createLoginContext(conf);
    // Invoke the login
    loginContext.login();
    // Get the Subject from the context and verify it's non-null (null would imply failure)
    Subject loggedInSubject = loginContext.getSubject();
    if (null == loggedInSubject) {
      throw new RuntimeException("Failed to perform Kerberos login");
    }

    // Send it back to the caller to use with launchRenewalThread
    return new AbstractMap.SimpleEntry<>(loginContext, loggedInSubject);
  }

  // Enables mocking for unit tests
  LoginContext createLoginContext(Configuration conf) throws LoginException {
    return new LoginContext(JAAS_CONF_NAME, subject, null, conf);
  }

  /**
   * Launches a thread to periodically check the current ticket's lifetime and perform a relogin
   * as necessary.
   *
   * @param originalContext The original login's context.
   * @param originalSubject The original login's subject.
   * @param renewalPeriod The amount of time to sleep inbetween checks to renew
   */
  Entry<RenewalTask, Thread> createRenewalThread(LoginContext originalContext,
      Subject originalSubject, long renewalPeriod) {
    RenewalTask task = new RenewalTask(this, originalContext, originalSubject, jaasConf,
        renewalPeriod);
    Thread t = new Thread(task);

    // Don't prevent the JVM from existing
    t.setDaemon(true);
    // Log an error message if this thread somehow dies
    t.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      @Override public void uncaughtException(Thread t, Throwable e) {
        LOG.error("Uncaught exception from Kerberos credential renewal thread", e);
      }
    });
    t.setName(RENEWAL_THREAD_NAME);

    return new AbstractMap.SimpleEntry<>(task, t);
  }

  /**
   * Stops the Kerberos renewal thread if it is still running. If the thread was already started
   * or never started, this method does nothing.
   */
  public void stopRenewalThread() {
    if (null != renewalTask && null != renewalThread) {
      LOG.debug("Informing RenewalTask to gracefully stop and interrupting the renewal thread.");
      renewalTask.asyncStop();

      long now = System.currentTimeMillis();
      long until = now + 5000;
      while (now < until) {
        if (renewalThread.isAlive()) {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }

          now = System.currentTimeMillis();
        } else {
          break;
        }
      }

      if (renewalThread.isAlive()) {
        LOG.warn("Renewal thread failed to gracefully stop, interrupting it");
        renewalThread.interrupt();
        try {
          renewalThread.join(5000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

      // What more could we do?
      if (renewalThread.isAlive()) {
        LOG.warn("Renewal thread failed to gracefully and ungracefully stop, proceeding.");
      }

      renewalTask = null;
      renewalThread = null;
    } else {
      LOG.warn("Renewal thread was never started or already stopped.");
    }
  }

  /**
   * Runnable for performing Kerberos renewals.
   */
  static class RenewalTask implements Runnable {
    private static final Logger RENEWAL_LOG = LoggerFactory.getLogger(RenewalTask.class);
    // Mutable variables -- change as re-login occurs
    private LoginContext context;
    private Subject subject;
    private final KerberosConnection utilInstance;
    private final Configuration conf;
    private final long renewalPeriod;
    private final AtomicBoolean keepRunning = new AtomicBoolean(true);

    public RenewalTask(KerberosConnection utilInstance, LoginContext context, Subject subject,
        Configuration conf, long renewalPeriod) {
      this.utilInstance = Objects.requireNonNull(utilInstance);
      this.context = Objects.requireNonNull(context);
      this.subject = Objects.requireNonNull(subject);
      this.conf = Objects.requireNonNull(conf);
      this.renewalPeriod = renewalPeriod;
    }

    @Override public void run() {
      while (keepRunning.get() && !Thread.currentThread().isInterrupted()) {
        RENEWAL_LOG.debug("Checking if Kerberos ticket should be renewed");
        // The current time
        final long now = System.currentTimeMillis();

        // Find the TGT in the Subject for the principal we were given.
        Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
        KerberosTicket activeTicket = null;
        for (KerberosTicket ticket : tickets) {
          if (isTGSPrincipal(ticket.getServer())) {
            activeTicket = ticket;
            break;
          }
        }

        // If we have no active ticket, immediately renew and check again to make sure we have
        // a valid ticket now.
        if (null == activeTicket) {
          RENEWAL_LOG.debug("There is no active Kerberos ticket, renewing now");
          renew();
          continue;
        }

        // Only renew when we hit a certain threshold of the current ticket's lifetime.
        // We want to limit the number of renewals we have to invoke.
        if (shouldRenew(activeTicket.getStartTime().getTime(),
            activeTicket.getEndTime().getTime(), now)) {
          RENEWAL_LOG.debug("The current ticket should be renewed now");
          renew();
        }

        // Sleep until we check again
        waitForNextCheck(renewalPeriod);
      }
    }

    /**
     * Computes whether or not the ticket should be renewed based on the lifetime of the ticket
     * and the current time.
     *
     * @param start The start time of the ticket's validity in millis
     * @param end The end time of the ticket's validity in millis
     * @param now Milliseconds since the epoch
     * @return True if renewal should occur, false otherwise
     */
    boolean shouldRenew(final long start, final long end, long now) {
      final long lifetime = end - start;
      final long renewAfter = start + (long) (lifetime * PERCENT_OF_LIFETIME_TO_RENEW);
      return now >= renewAfter;
    }

    /**
     * Logout and log back in with the Kerberos identity.
     */
    void renew() {
      try {
        // Lock on the instance of KerberosUtil
        synchronized (utilInstance) {
          Entry<LoginContext, Subject> pair = utilInstance.login(context, conf, subject);
          context = pair.getKey();
          subject = pair.getValue();
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to perform kerberos login");
      }
    }

    /**
     * Wait the given amount of time.
     *
     * @param renewalPeriod The number of milliseconds to wait
     */
    void waitForNextCheck(long renewalPeriod) {
      try {
        Thread.sleep(renewalPeriod);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    void asyncStop() {
      keepRunning.set(false);
    }
  }

  /**
   * Computes if the given {@code principal} is the ticket-granting system's principal ("krbtgt").
   *
   * @param principal A {@link KerberosPrincipal}.
   * @return True if {@code principal} is the TGS principal, false otherwise.
   */
  static boolean isTGSPrincipal(KerberosPrincipal principal) {
    if (principal == null) {
      return false;
    }

    if (principal.getName().equals("krbtgt/" + principal.getRealm() + "@" + principal.getRealm())) {
      return true;
    }

    return false;
  }

  /**
   * Javax Configuration for performing a keytab-based Kerberos login.
   */
  static class KeytabJaasConf extends Configuration {
    private String principal;
    private File keytabFile;

    KeytabJaasConf(String principal, File keytab) {
      this.principal = principal;
      this.keytabFile = keytab;
    }

    @Override public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      HashMap<String, String> options = new HashMap<String, String>();
      options.put("keyTab", keytabFile.getAbsolutePath());
      options.put("principal", principal);
      options.put("useKeyTab", "true");
      options.put("storeKey", "true");
      options.put("doNotPrompt", "true");
      options.put("renewTGT", "false");
      options.put("refreshKrb5Config", "true");
      options.put("isInitiator", "true");

      return new AppConfigurationEntry[] {new AppConfigurationEntry(getKrb5LoginModuleName(),
          AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)};
    }
  }

  /**
   * Returns the KRB5 LoginModule implementation. This is JVM-vendor dependent.
   *
   * @return The class name of the KRB5 LoginModule
   */
  static String getKrb5LoginModuleName() {
    return System.getProperty("java.vendor").contains("IBM") ? IBM_KRB5_LOGIN_MODULE
        : SUN_KRB5_LOGIN_MODULE;
  }
}

// End KerberosConnection.java
