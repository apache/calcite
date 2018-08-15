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

import org.eclipse.jetty.security.SpnegoLoginService;
import org.eclipse.jetty.security.SpnegoUserPrincipal;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.B64Code;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Objects;

import javax.security.auth.Subject;
import javax.servlet.ServletRequest;

/**
 * A customization of {@link SpnegoLoginService} which directly specifies the server's
 * principal instead of requiring a file to exist. Known to work with Jetty-9.2.x, any other
 * version would require testing/inspection to ensure the logic is still sound.
 */
public class PropertyBasedSpnegoLoginService extends SpnegoLoginService {
  private static final Logger LOG = LoggerFactory.getLogger(PropertyBasedSpnegoLoginService.class);

  private static final String TARGET_NAME_FIELD_NAME = "_targetName";
  private final String serverPrincipal;

  public PropertyBasedSpnegoLoginService(String realm, String serverPrincipal) {
    super(realm);
    this.serverPrincipal = Objects.requireNonNull(serverPrincipal);
  }

  @Override protected void doStart() throws Exception {
    // Override the parent implementation, setting _targetName to be the serverPrincipal
    // without the need for a one-line file to do the same thing.
    //
    // AbstractLifeCycle's doStart() method does nothing, so we aren't missing any extra logic.
    final Field targetNameField = SpnegoLoginService.class.getDeclaredField(TARGET_NAME_FIELD_NAME);
    targetNameField.setAccessible(true);
    targetNameField.set(this, serverPrincipal);
  }

  @Override public UserIdentity login(String username, Object credentials,
      ServletRequest request) {
    String encodedAuthToken = (String) credentials;
    byte[] authToken = B64Code.decode(encodedAuthToken);

    GSSManager manager = GSSManager.getInstance();
    try {
      // http://java.sun.com/javase/6/docs/technotes/guides/security/jgss/jgss-features.html
      Oid spnegoOid = new Oid("1.3.6.1.5.5.2");
      Oid krb5Oid = new Oid("1.2.840.113554.1.2.2");
      GSSName gssName = manager.createName(serverPrincipal, null);
      // CALCITE-1922 Providing both OIDs is the bug in Jetty we're working around. By specifying
      // only one, we're requiring that clients *must* provide us the SPNEGO OID to authenticate
      // via Kerberos which is wrong. Best as I can tell, the SPNEGO OID is meant as another
      // layer of indirection (essentially is equivalent to setting the Kerberos OID).
      GSSCredential serverCreds = manager.createCredential(gssName,
          GSSCredential.INDEFINITE_LIFETIME, new Oid[] {krb5Oid, spnegoOid},
          GSSCredential.ACCEPT_ONLY);
      GSSContext gContext = manager.createContext(serverCreds);

      if (gContext == null) {
        LOG.debug("SpnegoUserRealm: failed to establish GSSContext");
      } else {
        while (!gContext.isEstablished()) {
          authToken = gContext.acceptSecContext(authToken, 0, authToken.length);
        }
        if (gContext.isEstablished()) {
          String clientName = gContext.getSrcName().toString();
          String role = clientName.substring(clientName.indexOf('@') + 1);

          LOG.debug("SpnegoUserRealm: established a security context");
          LOG.debug("Client Principal is: {}", gContext.getSrcName());
          LOG.debug("Server Principal is: {}", gContext.getTargName());
          LOG.debug("Client Default Role: {}", role);

          SpnegoUserPrincipal user = new SpnegoUserPrincipal(clientName, authToken);

          Subject subject = new Subject();
          subject.getPrincipals().add(user);

          return _identityService.newUserIdentity(subject, user, new String[]{role});
        }
      }
    } catch (GSSException gsse) {
      LOG.warn("Caught GSSException trying to authenticate the client", gsse);
    }

    return null;
  }
}

// End PropertyBasedSpnegoLoginService.java
