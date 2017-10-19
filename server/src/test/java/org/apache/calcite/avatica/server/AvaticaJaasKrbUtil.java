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
package org.apache.calcite.avatica.server;

import org.apache.calcite.avatica.remote.ClientKeytabJaasConf;

import java.io.File;
import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Copy of JaasKrbUtil from Kerby that works with IBM Java as well as Oracle Java.
 */
public class AvaticaJaasKrbUtil {

  private AvaticaJaasKrbUtil() {}

  public static Subject loginUsingKeytab(
      String principal, File keytabFile) throws LoginException {
    Set<Principal> principals = new HashSet<Principal>();
    principals.add(new KerberosPrincipal(principal));

    Subject subject = new Subject(false, principals,
        new HashSet<Object>(), new HashSet<Object>());

    Configuration conf = useKeytab(principal, keytabFile);
    String confName = "KeytabConf";
    LoginContext loginContext = new LoginContext(confName, subject, null, conf);
    loginContext.login();
    return loginContext.getSubject();
  }

  public static Configuration useKeytab(String principal, File keytabFile) {
    return new ClientKeytabJaasConf(principal, keytabFile.toString());
  }
}

// End AvaticaJaasKrbUtil.java
