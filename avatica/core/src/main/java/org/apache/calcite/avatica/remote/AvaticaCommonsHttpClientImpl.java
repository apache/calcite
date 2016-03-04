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

import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.protocol.RequestExpectContinue;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.pool.BasicConnFactory;
import org.apache.http.impl.pool.BasicConnPool;
import org.apache.http.impl.pool.BasicPoolEntry;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.protocol.RequestConnControl;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.util.EntityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Future;

/**
 * A common class to invoke HTTP requests against the Avatica server agnostic of the data being
 * sent and received across the wire.
 */
public class AvaticaCommonsHttpClientImpl implements AvaticaHttpClient {
  private static final Logger LOG = LoggerFactory.getLogger(AvaticaCommonsHttpClientImpl.class);
  private static final ConnectionReuseStrategy REUSE = DefaultConnectionReuseStrategy.INSTANCE;

  // Some basic exposed configurations
  private static final String MAX_POOLED_CONNECTION_PER_ROUTE_KEY =
      "avatica.pooled.connections.per.route";
  private static final String MAX_POOLED_CONNECTION_PER_ROUTE_DEFAULT = "4";
  private static final String MAX_POOLED_CONNECTIONS_KEY = "avatica.pooled.connections.max";
  private static final String MAX_POOLED_CONNECTIONS_DEFAULT = "16";

  protected final HttpHost host;
  protected final HttpProcessor httpProcessor;
  protected final HttpRequestExecutor httpExecutor;
  protected final BasicConnPool httpPool;

  public AvaticaCommonsHttpClientImpl(URL url) {
    this.host = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());

    this.httpProcessor = HttpProcessorBuilder.create()
        .add(new RequestContent())
        .add(new RequestTargetHost())
        .add(new RequestConnControl())
        .add(new RequestExpectContinue()).build();

    this.httpExecutor = new HttpRequestExecutor();

    this.httpPool = new BasicConnPool(new BasicConnFactory());
    int maxPerRoute = Integer.parseInt(
        System.getProperty(MAX_POOLED_CONNECTION_PER_ROUTE_KEY,
            MAX_POOLED_CONNECTION_PER_ROUTE_DEFAULT));
    int maxTotal = Integer.parseInt(
        System.getProperty(MAX_POOLED_CONNECTIONS_KEY,
            MAX_POOLED_CONNECTIONS_DEFAULT));
    httpPool.setDefaultMaxPerRoute(maxPerRoute);
    httpPool.setMaxTotal(maxTotal);
  }

  public byte[] send(byte[] request) {
    while (true) {
      boolean reusable = false;
      // Get a connection from the pool
      Future<BasicPoolEntry> future = this.httpPool.lease(host, null);
      BasicPoolEntry entry = null;
      try {
        entry = future.get();
        HttpCoreContext coreContext = HttpCoreContext.create();
        coreContext.setTargetHost(host);

        HttpClientConnection conn = entry.getConnection();

        ByteArrayEntity entity = new ByteArrayEntity(request, ContentType.APPLICATION_OCTET_STREAM);

        BasicHttpEntityEnclosingRequest postRequest =
            new BasicHttpEntityEnclosingRequest("POST", "/");
        postRequest.setEntity(entity);

        httpExecutor.preProcess(postRequest, httpProcessor, coreContext);
        HttpResponse response = httpExecutor.execute(postRequest, conn, coreContext);
        httpExecutor.postProcess(response, httpProcessor, coreContext);

        // Should the connection be kept alive?
        reusable = REUSE.keepAlive(response, coreContext);

        final int statusCode = response.getStatusLine().getStatusCode();
        if (HttpURLConnection.HTTP_UNAVAILABLE == statusCode) {
          // Could be sitting behind a load-balancer, try again.
          continue;
        }

        return EntityUtils.toByteArray(response.getEntity());
      } catch (Exception e) {
        LOG.debug("Failed to execute HTTP request", e);
        throw new RuntimeException(e);
      } finally {
        // Release the connection back to the pool, marking if it's good to reuse or not.
        httpPool.release(entry, reusable);
      }
    }
  }
}

// End AvaticaCommonsHttpClientImpl.java
