---
layout: docs
title: Security
sidebar_title: Security
permalink: /docs/security.html
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

Security is an important topic between clients and the Avatica server. Most JDBC
drivers and databases implement some level of authentication and authorization
for limit what actions clients are allowed to perform.

Similarly, Avatica must limit what users are allowed to connect and interact
with the server. Avatica must primarily deal with authentication while authorization
is deferred to the underlying database. By default, Avatica provides no authentication.
Avatica does have the ability to perform client authentication using Kerberos.

## Kerberos-based authentication

Because Avatica operates over an HTTP interface, the simple and protected GSSAPI
negotiation mechanism ([SPNEGO](https://en.wikipedia.org/wiki/SPNEGO)) is a logical
choice. This mechanism makes use of the "HTTP Negotiate" authentication extension to
communicate with the Kerberos Key Distribution Center (KDC) to authenticate a client.

## Enabling SPNEGO/Kerberos Authentication in servers

The Avatica server can operate either by performing the login using
a JAAS configuration file or login programmatically. By default, authenticated clients
will have queries executed as the Avatica server's kerberos user. [Impersonation](#impersonation)
is the feature which enables actions to be run in the server as the actual end-user.

As a note, it is required that the Kerberos principal in use by the Avatica server
**must** have an primary of `HTTP` (where Kerberos principals are of the form
`primary[/instance]@REALM`). This is specified by [RFC-4559](https://tools.ietf.org/html/rfc4559).

### Programmatic Login

This approach requires no external file configurations and only requires a
keytab file for the principal.

{% highlight java %}
HttpServer server = new HttpServer.Builder()
    .withPort(8765)
    .withHandler(new LocalService(), Driver.Serialization.PROTOBUF)
    .withSpnego("HTTP/host.domain.com@DOMAIN.COM")
    .withAutomaticLogin(
        new File("/etc/security/keytabs/avatica.spnego.keytab"))
    .build();
{% endhighlight %}

### JAAS Configuration File Login

A JAAS configuration file can be set via the system property `java.security.auth.login.config`.
The user must set this property when launching their Java application invoking the Avatica server.
The presence of this file will automatically perform login as necessary in the first use
of the Avatica server. The invocation is nearly the same as the programmatic login.

{% highlight java %}
HttpServer server = new HttpServer.Builder()
    .withPort(8765)
    .withHandler(new LocalService(), Driver.Serialization.PROTOBUF)
    .withSpnego("HTTP/host.domain.com@DOMAIN.COM")
    .build();
{% endhighlight %}

The contents of the JAAS configuration file are very specific:

{% highlight java %}
com.sun.security.jgss.accept  {
  com.sun.security.auth.module.Krb5LoginModule required
  storeKey=true
  useKeyTab=true
  keyTab=/etc/security/keytabs/avatica.spnego.keyTab
  principal=HTTP/host.domain.com@DOMAIN.COM;
};
{% endhighlight %}

Ensure the `keyTab` and `principal` attributes are set correctly for your system.

## Impersonation

Impersonation is a feature of the Avatica server which allows the Avatica clients
to execute the server-side calls (e.g. the underlying JDBC calls). Because the details
on what it means to execute such an operation are dependent on the actual system, a
callback is exposed for downstream integrators to implement.

For example, the following is an example for creating an Apache Hadoop `UserGroupInformation`
"proxy user". This example takes a `UserGroupInformation` object representing the Avatica server's
identity, creates a "proxy user" with the client's username, and performs the action as that
client but using the server's identity.

{% highlight java %}
public class PhoenixDoAsCallback implements DoAsRemoteUserCallback {
  private final UserGroupInformation serverUgi;

  public PhoenixDoAsCallback(UserGroupInformation serverUgi) {
    this.serverUgi = Objects.requireNonNull(serverUgi);
  }

  @Override
  public <T> T doAsRemoteUser(String remoteUserName, String remoteAddress, final Callable<T> action) throws Exception {
    // Proxy this user on top of the server's user (the real user)
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(remoteUserName, serverUgi);

    // Check if this user is allowed to be impersonated.
    // Will throw AuthorizationException if the impersonation as this user is not allowed
    ProxyUsers.authorize(proxyUser, remoteAddress);

    // Execute the actual call as this proxy user
    return proxyUser.doAs(new PrivilegedExceptionAction<T>() {
      @Override
      public T run() throws Exception {
        return action.call();
      }
    });
  }
}
{% endhighlight %}

## Client implementation

Many HTTP client libraries, such as [Apache Commons HttpComponents](https://hc.apache.org/), already have
support for performing SPNEGO authentication. When in doubt, refer to one of
these implementations as it is likely correct.

For information on building this by hand, consult [RFC-4559](https://tools.ietf.org/html/rfc4559)
which describes how the authentication handshake, through use of the "WWW-authenticate"
HTTP header, is used to authenticate a client.
