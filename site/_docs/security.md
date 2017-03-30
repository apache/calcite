---
layout: docs
title: Security
sidebar_title: Security
permalink: /docs/security.html
auth_types:
  - { name: "HTTP Basic", anchor: "http-basic-authentication" }
  - { name: "HTTP Digest", anchor: "http-digest-authentication" }
  - { name: "Kerberos with SPNEGO", anchor: "kerberos-with-spnego-authentication" }
  - { name: "Client implementation", anchor: "client-implementation" }
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
Avatica does have the ability to perform client authentication using Kerberos,
HTTP Basic, and HTTP Digest.

The authentication and authorization provided by Avatica are designed for use
*instead* of the authentication and authorization provided by the underlying database.
The typical `user` and `password` JDBC properties are **always** passed through to
the Avatica server which will cause the server to enforce those credentials. As such,
Avatica's authentication types mentioned here only have relevance when the underlying database's authentication
and authorization features are not used. (The Kerberos/SPNEGO integration is one difference as the impersonation feature
is specifically designed to allow the Kerberos identity to be passed to the database -
new advanced implementations could also follow this same approach if desired).

## Table of Contents
<ul>
  {% for item in page.auth_types %}<li><a href="#{{ item.anchor }}">{{ item.name }}</a></li>{% endfor %}
</ul>


## HTTP Basic Authentication

Avatica supports authentication over [HTTP Basic](https://en.wikipedia.org/wiki/Basic_access_authentication).
This is simple username-password based authentication which is ultimately insecure when
operating over an untrusted network. Basic authentication is only secure when the transport
is encrypted (e.g. TLS) as the credentials are passed in the clear. This authentication is
supplementary to the provided JDBC authentication. If credentials are passed to the database
already, this authentication is unnecessary.

### Enabling Basic Authentication

{% highlight java %}
String propertiesFile = "/path/to/jetty-users.properties";
// All roles allowed
String[] allowedRoles = new String[]  {"*"};
// Only specific roles are allowed
allowedRoles = new String[] { "users", "admins" };
HttpServer server = new HttpServer.Builder()
    .withPort(8765)
    .withHandler(new LocalService(), Driver.Serialization.PROTOBUF)
    .withBasicAuthentication(propertiesFile, allowedRoles)
    .build();
{% endhighlight %}

The properties file must be in a form consumable by Jetty. Each line in this
file is of the form: `username: password[,rolename ...]`

For example:

{% highlight properties %}
bob: b0b5pA55w0rd,users
steve: 5teve5pA55w0rd,users
alice: Al1cepA55w0rd,admins
{% endhighlight %}

Passwords can also be obfuscated as MD5 hashes or oneway cryptography ("CRYPT").
For more information, see the [official Jetty documentation](http://www.eclipse.org/jetty/documentation/current/configuring-security-secure-passwords.html).

## HTTP Digest Authentication

Avatica also supports [HTTP Digest](https://en.wikipedia.org/wiki/Digest_access_authentication).
This is desirable for Avatica as it does not require the use of TLS to secure communication
between the Avatica client and server. It is configured very similarly to HTTP Basic
authentication. This authentication is supplementary to the provided JDBC authentication.
If credentials are passed to the database already, this authentication is unnecessary.

### Enabling Digest Authentication

{% highlight java %}
String propertiesFile = "/path/to/jetty-users.properties";
// All roles allowed
String[] allowedRoles = new String[]  {"*"};
// Only specific roles are allowed
allowedRoles = new String[] { "users", "admins" };
HttpServer server = new HttpServer.Builder()
    .withPort(8765)
    .withHandler(new LocalService(), Driver.Serialization.PROTOBUF)
    .withDigestAuthentication(propertiesFile, allowedRoles)
    .build();
{% endhighlight %}

The properties file must be in a form consumable by Jetty. Each line in this
file is of the form: `username: password[,rolename ...]`

For example:

{% highlight properties %}
bob: b0b5pA55w0rd,users
steve: 5teve5pA55w0rd,users
alice: Al1cepA55w0rd,admins
{% endhighlight %}

Passwords can also be obfuscated as MD5 hashes or oneway cryptography ("CRYPT").
For more information, see the [official Jetty documentation](http://www.eclipse.org/jetty/documentation/current/configuring-security-secure-passwords.html).

## Kerberos with SPNEGO Authentication

Because Avatica operates over an HTTP interface, the simple and protected GSSAPI
negotiation mechanism ([SPNEGO](https://en.wikipedia.org/wiki/SPNEGO)) is a logical
choice. This mechanism makes use of the "HTTP Negotiate" authentication extension to
communicate with the Kerberos Key Distribution Center (KDC) to authenticate a client.

### Enabling SPNEGO/Kerberos Authentication in servers

The Avatica server can operate either by performing the login using
a JAAS configuration file or login programmatically. By default, authenticated clients
will have queries executed as the Avatica server's kerberos user. [Impersonation](#impersonation)
is the feature which enables actions to be run in the server as the actual end-user.

As a note, it is required that the Kerberos principal in use by the Avatica server
**must** have an primary of `HTTP` (where Kerberos principals are of the form
`primary[/instance]@REALM`). This is specified by [RFC-4559](https://tools.ietf.org/html/rfc4559).

#### Programmatic Login

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

#### JAAS Configuration File Login

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

### Impersonation

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
support for performing Basic, Digest, and SPNEGO authentication. When in doubt, refer to one of
these implementations as it is likely correct.

### SPNEGO

For information on building SPNEGO support by hand, consult [RFC-4559](https://tools.ietf.org/html/rfc4559)
which describes how the authentication handshake, through use of the "WWW-authenticate=Negotiate"
HTTP header, is used to authenticate a client.

### Password-based

For both HTTP Basic and Digest authentication, the [avatica_user]({{site.baseurl}}/docs/client_reference.html#avatica-user)
and [avatica_password]({{site.baseurl}}/docs/client_reference.html#avatica-password)
properties are used to identify the client with the server. If the underlying database
(the JDBC driver inside the Avatica server) require their own user and password combination,
these are set via the traditional "user" and "password" properties in the Avatica
JDBC driver. This also implies that adding HTTP-level authentication in Avatica is likely
superfluous.
