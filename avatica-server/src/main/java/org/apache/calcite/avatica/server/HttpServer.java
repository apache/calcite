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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

/**
 * Avatica HTTP server.
 */
public class HttpServer {
  private static final Log LOG = LogFactory.getLog(HttpServer.class);

  private Server server;
  private int port = -1;
  private final Handler handler;

  public HttpServer(int port, Handler handler) {
    this.port = port;
    this.handler = handler;
  }

  public void start() {
    if (server != null) {
      throw new RuntimeException("Server is already started");
    }

    final QueuedThreadPool threadPool = new QueuedThreadPool();
    threadPool.setDaemon(true);
    server = new Server(threadPool);
    server.manage(threadPool);

    final ServerConnector connector = new ServerConnector(server);
    connector.setIdleTimeout(60 * 1000);
    connector.setSoLingerTime(-1);
    connector.setPort(port);
    server.setConnectors(new Connector[] { connector });

    final HandlerList handlerList = new HandlerList();
    handlerList.setHandlers(new Handler[] { handler, new DefaultHandler() });
    server.setHandler(handlerList);
    try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    port = connector.getLocalPort();

    LOG.info("Service listening on port " + getPort() + ".");
  }

  public void stop() {
    if (server == null) {
      throw new RuntimeException("Server is already stopped");
    }

    LOG.info("Service terminating.");
    try {
      final Server server1 = server;
      port = -1;
      server = null;
      server1.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void join() throws InterruptedException {
    server.join();
  }

  public int getPort() {
    return port;
  }
}

// End HttpServer.java
