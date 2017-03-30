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

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.KerberosCredentials;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import org.ietf.jgss.GSSCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.Principal;
import java.util.Objects;

/**
 * Implementation of an AvaticaHttpClient which uses SPNEGO.
 */
public class AvaticaCommonsHttpClientSpnegoImpl implements AvaticaHttpClient {
  private static final Logger LOG = LoggerFactory
      .getLogger(AvaticaCommonsHttpClientSpnegoImpl.class);

  public static final String CACHED_CONNECTIONS_MAX_KEY = "avatica.http.spnego.max_cached";
  public static final String CACHED_CONNECTIONS_MAX_DEFAULT = "100";
  public static final String CACHED_CONNECTIONS_MAX_PER_ROUTE_KEY =
      "avatica.http.spnego.max_per_route";
  public static final String CACHED_CONNECTIONS_MAX_PER_ROUTE_DEFAULT = "25";

  private static final boolean USE_CANONICAL_HOSTNAME = true;
  private static final boolean STRIP_PORT_ON_SERVER_LOOKUP = true;

  final URL url;
  final HttpHost host;
  final PoolingHttpClientConnectionManager pool;
  final Lookup<AuthSchemeProvider> authRegistry;
  final BasicCredentialsProvider credentialsProvider;
  final BasicAuthCache authCache;
  final CloseableHttpClient client;

  /**
   * Constructs an http client with the expectation that the user is already logged in with their
   * Kerberos identity via JAAS.
   *
   * @param url The URL for the Avatica server
   */
  public AvaticaCommonsHttpClientSpnegoImpl(URL url) {
    this(url, null);
  }

  /**
   * Constructs an HTTP client with user specified by the given credentials.
   *
   * @param url The URL for the Avatica server
   * @param credential The GSS credentials
   */
  public AvaticaCommonsHttpClientSpnegoImpl(URL url, GSSCredential credential) {
    this.url = Objects.requireNonNull(url);

    pool = new PoolingHttpClientConnectionManager();
    // Increase max total connection to 100
    final String maxCnxns =
        System.getProperty(CACHED_CONNECTIONS_MAX_KEY, CACHED_CONNECTIONS_MAX_DEFAULT);
    pool.setMaxTotal(Integer.parseInt(maxCnxns));
    // Increase default max connection per route to 25
    final String maxCnxnsPerRoute = System.getProperty(CACHED_CONNECTIONS_MAX_PER_ROUTE_KEY,
        CACHED_CONNECTIONS_MAX_PER_ROUTE_DEFAULT);
    pool.setDefaultMaxPerRoute(Integer.parseInt(maxCnxnsPerRoute));

    this.host = new HttpHost(url.getHost(), url.getPort());

    this.authRegistry = RegistryBuilder.<AuthSchemeProvider>create().register(AuthSchemes.SPNEGO,
        new SPNegoSchemeFactory(STRIP_PORT_ON_SERVER_LOOKUP, USE_CANONICAL_HOSTNAME)).build();

    this.credentialsProvider = new BasicCredentialsProvider();
    if (null != credential) {
      // Non-null credential should be used directly with KerberosCredentials.
      this.credentialsProvider.setCredentials(AuthScope.ANY, new KerberosCredentials(credential));
    } else {
      // A null credential implies that the user is logged in via JAAS using the
      // java.security.auth.login.config system property
      this.credentialsProvider.setCredentials(AuthScope.ANY, EmptyCredentials.INSTANCE);
    }

    this.authCache = new BasicAuthCache();

    // A single thread-safe HttpClient, pooling connections via the ConnectionManager
    this.client = HttpClients.custom()
        .setDefaultAuthSchemeRegistry(authRegistry)
        .setConnectionManager(pool).build();
  }

  @Override public byte[] send(byte[] request) {
    HttpClientContext context = HttpClientContext.create();

    context.setTargetHost(host);
    context.setCredentialsProvider(credentialsProvider);
    context.setAuthSchemeRegistry(authRegistry);
    context.setAuthCache(authCache);

    ByteArrayEntity entity = new ByteArrayEntity(request, ContentType.APPLICATION_OCTET_STREAM);

    // Create the client with the AuthSchemeRegistry and manager
    HttpPost post = new HttpPost(toURI(url));
    post.setEntity(entity);

    try (CloseableHttpResponse response = client.execute(post, context)) {
      final int statusCode = response.getStatusLine().getStatusCode();
      if (HttpURLConnection.HTTP_OK == statusCode
          || HttpURLConnection.HTTP_INTERNAL_ERROR == statusCode) {
        return EntityUtils.toByteArray(response.getEntity());
      }

      throw new RuntimeException("Failed to execute HTTP Request, got HTTP/" + statusCode);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOG.debug("Failed to execute HTTP request", e);
      throw new RuntimeException(e);
    }
  }

  private static URI toURI(URL url) throws RuntimeException {
    try {
      return url.toURI();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A credentials implementation which returns null.
   */
  private static class EmptyCredentials implements Credentials {
    public static final EmptyCredentials INSTANCE = new EmptyCredentials();

    @Override public String getPassword() {
      return null;
    }
    @Override public Principal getUserPrincipal() {
      return null;
    }
  }
}

// End AvaticaCommonsHttpClientSpnegoImpl.java
