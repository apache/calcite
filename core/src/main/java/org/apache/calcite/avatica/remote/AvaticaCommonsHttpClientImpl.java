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
import org.apache.http.NoHttpResponseException;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Lookup;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.auth.BasicSchemeFactory;
import org.apache.http.impl.auth.DigestSchemeFactory;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

/**
 * A common class to invoke HTTP requests against the Avatica server agnostic of the data being
 * sent and received across the wire.
 */
public class AvaticaCommonsHttpClientImpl implements AvaticaHttpClient,
    UsernamePasswordAuthenticateable, TrustStoreConfigurable,
        KeyStoreConfigurable, HostnameVerificationConfigurable {
  private static final Logger LOG = LoggerFactory.getLogger(AvaticaCommonsHttpClientImpl.class);

  // Some basic exposed configurations
  private static final String MAX_POOLED_CONNECTION_PER_ROUTE_KEY =
      "avatica.pooled.connections.per.route";
  private static final String MAX_POOLED_CONNECTION_PER_ROUTE_DEFAULT = "25";
  private static final String MAX_POOLED_CONNECTIONS_KEY = "avatica.pooled.connections.max";
  private static final String MAX_POOLED_CONNECTIONS_DEFAULT = "100";

  protected final HttpHost host;
  protected final URI uri;
  protected BasicAuthCache authCache;
  protected CloseableHttpClient client;
  protected Registry<ConnectionSocketFactory> socketFactoryRegistry;
  protected PoolingHttpClientConnectionManager pool;

  protected UsernamePasswordCredentials credentials = null;
  protected CredentialsProvider credentialsProvider = null;
  protected Lookup<AuthSchemeProvider> authRegistry = null;
  protected Object userToken;

  protected File truststore = null;
  protected File keystore = null;
  protected String truststorePassword = null;
  protected String keystorePassword = null;
  protected String keyPassword = null;
  protected HostnameVerification hostnameVerification = null;

  public AvaticaCommonsHttpClientImpl(URL url) {
    this.host = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
    this.uri = toURI(Objects.requireNonNull(url));
    initializeClient();
  }

  private void initializeClient() {
    socketFactoryRegistry = this.configureSocketFactories();
    configureConnectionPool(socketFactoryRegistry);
    this.authCache = new BasicAuthCache();
    // A single thread-safe HttpClient, pooling connections via the ConnectionManager
    this.client = HttpClients.custom().setConnectionManager(pool).build();
  }

  protected void configureConnectionPool(Registry<ConnectionSocketFactory> registry) {
    pool = new PoolingHttpClientConnectionManager(registry);
    // Increase max total connection to 100
    final String maxCnxns =
        System.getProperty(MAX_POOLED_CONNECTIONS_KEY,
            MAX_POOLED_CONNECTIONS_DEFAULT);
    pool.setMaxTotal(Integer.parseInt(maxCnxns));
    // Increase default max connection per route to 25
    final String maxCnxnsPerRoute = System.getProperty(MAX_POOLED_CONNECTION_PER_ROUTE_KEY,
        MAX_POOLED_CONNECTION_PER_ROUTE_DEFAULT);
    pool.setDefaultMaxPerRoute(Integer.parseInt(maxCnxnsPerRoute));
  }

  protected Registry<ConnectionSocketFactory> configureSocketFactories() {
    RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.create();
    if (host.getSchemeName().equalsIgnoreCase("https")) {
      configureHttpsRegistry(registryBuilder);
    } else {
      configureHttpRegistry(registryBuilder);
    }
    return registryBuilder.build();
  }

  protected void configureHttpsRegistry(RegistryBuilder<ConnectionSocketFactory> registryBuilder) {
    try {
      SSLContext sslContext = getSSLContext();
      final HostnameVerifier verifier = getHostnameVerifier(hostnameVerification);
      SSLConnectionSocketFactory sslFactory = new SSLConnectionSocketFactory(sslContext, verifier);
      registryBuilder.register("https", sslFactory);
    } catch (Exception e) {
      LOG.error("HTTPS registry configuration failed");
      throw new RuntimeException(e);
    }
  }

  private SSLContext getSSLContext() throws Exception {
    SSLContextBuilder sslContextBuilder = SSLContexts.custom();
    if (null != truststore && null != truststorePassword) {
      loadTrustStore(sslContextBuilder);
    }
    if (null != keystore && null != keystorePassword && null != keyPassword) {
      loadKeyStore(sslContextBuilder);
    }
    return sslContextBuilder.build();
  }

  protected void loadKeyStore(SSLContextBuilder sslContextBuilder) throws Exception {
    sslContextBuilder.loadKeyMaterial(keystore,
            keystorePassword.toCharArray(), keyPassword.toCharArray());
  }

  protected void loadTrustStore(SSLContextBuilder sslContextBuilder) throws Exception {
    sslContextBuilder.loadTrustMaterial(truststore, truststorePassword.toCharArray());
  }

  protected void configureHttpRegistry(RegistryBuilder<ConnectionSocketFactory> registryBuilder) {
    registryBuilder.register("http", PlainConnectionSocketFactory.getSocketFactory());
  }

  /**
   * Creates the {@code HostnameVerifier} given the provided {@code verification}.
   *
   * @param verification The intended hostname verification action.
   * @return A verifier for the request verification.
   * @throws IllegalArgumentException if the provided verification cannot be handled.
   */
  HostnameVerifier getHostnameVerifier(HostnameVerification verification) {
    // Normally, the configuration logic would give us a default of STRICT if it was not
    // provided by the user. It's easy for us to do a double-check.
    if (verification == null) {
      verification = HostnameVerification.STRICT;
    }
    switch (verification) {
    case STRICT:
      return SSLConnectionSocketFactory.getDefaultHostnameVerifier();
    case NONE:
      return NoopHostnameVerifier.INSTANCE;
    default:
      throw new IllegalArgumentException("Unhandled HostnameVerification: "
          + hostnameVerification);
    }
  }

  public byte[] send(byte[] request) {
    while (true) {
      HttpClientContext context = HttpClientContext.create();

      context.setTargetHost(host);

      // Set the credentials if they were provided.
      if (null != this.credentials) {
        context.setCredentialsProvider(credentialsProvider);
        context.setAuthSchemeRegistry(authRegistry);
        context.setAuthCache(authCache);
      }

      if (null != userToken) {
        context.setUserToken(userToken);
      }

      ByteArrayEntity entity = new ByteArrayEntity(request, ContentType.APPLICATION_OCTET_STREAM);

      // Create the client with the AuthSchemeRegistry and manager
      HttpPost post = new HttpPost(uri);
      post.setEntity(entity);

      try (CloseableHttpResponse response = execute(post, context)) {
        final int statusCode = response.getStatusLine().getStatusCode();
        if (HttpURLConnection.HTTP_OK == statusCode
            || HttpURLConnection.HTTP_INTERNAL_ERROR == statusCode) {
          userToken = context.getUserToken();
          return EntityUtils.toByteArray(response.getEntity());
        } else if (HttpURLConnection.HTTP_UNAVAILABLE == statusCode) {
          LOG.debug("Failed to connect to server (HTTP/503), retrying");
          continue;
        }

        throw new RuntimeException("Failed to execute HTTP Request, got HTTP/" + statusCode);
      } catch (NoHttpResponseException e) {
        // This can happen when sitting behind a load balancer and a backend server dies
        LOG.debug("The server failed to issue an HTTP response, retrying");
        continue;
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        LOG.debug("Failed to execute HTTP request", e);
        throw new RuntimeException(e);
      }
    }
  }

  // Visible for testing
  CloseableHttpResponse execute(HttpPost post, HttpClientContext context)
      throws IOException, ClientProtocolException {
    return client.execute(post, context);
  }

  @Override public void setUsernamePassword(AuthenticationType authType, String username,
      String password) {
    this.credentials = new UsernamePasswordCredentials(
        Objects.requireNonNull(username), Objects.requireNonNull(password));

    this.credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, credentials);

    RegistryBuilder<AuthSchemeProvider> authRegistryBuilder = RegistryBuilder.create();
    switch (authType) {
    case BASIC:
      authRegistryBuilder.register(AuthSchemes.BASIC, new BasicSchemeFactory());
      break;
    case DIGEST:
      authRegistryBuilder.register(AuthSchemes.DIGEST, new DigestSchemeFactory());
      break;
    default:
      throw new IllegalArgumentException("Unsupported authentiation type: " + authType);
    }
    this.authRegistry = authRegistryBuilder.build();
  }

  private static URI toURI(URL url) throws RuntimeException {
    try {
      return url.toURI();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public void setTrustStore(File truststore, String password) {
    this.truststore = Objects.requireNonNull(truststore);
    if (!truststore.exists() || !truststore.isFile()) {
      throw new IllegalArgumentException(
          "Truststore is must be an existing, regular file: " + truststore);
    }
    this.truststorePassword = Objects.requireNonNull(password);
    initializeClient();
  }

  @Override public void setKeyStore(File keystore, String keystorepassword, String keypassword) {
    this.keystore = Objects.requireNonNull(keystore);
    if (!keystore.exists() || !keystore.isFile()) {
      throw new IllegalArgumentException(
              "Keystore is must be an existing, regular file: " + keystore);
    }
    this.keystorePassword = Objects.requireNonNull(keystorepassword);
    this.keyPassword = Objects.requireNonNull(keypassword);
    initializeClient();
  }

  @Override public void setHostnameVerification(HostnameVerification verification) {
    this.hostnameVerification = Objects.requireNonNull(verification);
    initializeClient();
  }
}

// End AvaticaCommonsHttpClientImpl.java
