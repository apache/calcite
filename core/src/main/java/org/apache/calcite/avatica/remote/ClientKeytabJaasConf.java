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

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

/**
 * Javax Configuration class which always returns a configuration for our keytab-based
 * login suitable for callers which are acting as initiators (e.g. a client).
 */
public class ClientKeytabJaasConf extends Configuration {
  private static final Logger LOG = LoggerFactory.getLogger(ClientKeytabJaasConf.class);
  private final String principal;
  private final String keytab;

  public ClientKeytabJaasConf(String principal, String keytab) {
    this.principal = principal;
    this.keytab = keytab;
  }

  @Override public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
    Map<String, String> options = new HashMap<String, String>();
    options.put("principal", principal);
    options.put("refreshKrb5Config", "true");
    if (KerberosConnection.isIbmJava()) {
      options.put("useKeytab", keytab);
      options.put("credsType", "both");
    } else {
      options.put("keyTab", keytab);
      options.put("useKeyTab", "true");
      options.put("isInitiator", "true");
      options.put("doNotPrompt", "true");
      options.put("storeKey", "true");
    }

    LOG.debug("JAAS Configuration for client keytab-based Kerberos login: {}", options);

    return new AppConfigurationEntry[] {new AppConfigurationEntry(
        KerberosConnection.getKrb5LoginModuleName(),
        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)};
  }
}

// End ClientKeytabJaasConf.java
