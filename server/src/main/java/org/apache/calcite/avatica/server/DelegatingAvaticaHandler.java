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

import org.apache.calcite.avatica.remote.Service.RpcMetadataResponse;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * An AvaticaHandler implementation that delegates to a provided Jetty Handler instance.
 *
 * <p>This implementation provides a no-op implementation for
 * {@link #setServerRpcMetadata(org.apache.calcite.avatica.remote.Service.RpcMetadataResponse)}.
 *
 * <p>Does not implement {@link MetricsAwareAvaticaHandler} as this implementation is only presented
 * for backwards compatibility.
 */
public class DelegatingAvaticaHandler implements AvaticaHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DelegatingAvaticaHandler.class);

  private final Handler handler;

  public DelegatingAvaticaHandler(Handler handler) {
    this.handler = Objects.requireNonNull(handler);
  }

  @Override public void handle(String target, Request baseRequest, HttpServletRequest request,
      HttpServletResponse response) throws IOException, ServletException {
    handler.handle(target, baseRequest, request, response);
  }

  @Override public void setServer(Server server) {
    handler.setServer(server);
  }

  @Override public Server getServer() {
    return handler.getServer();
  }

  @Override public void destroy() {
    handler.destroy();
  }

  @Override public void start() throws Exception {
    handler.start();
  }

  @Override public void stop() throws Exception {
    handler.stop();
  }

  @Override public boolean isRunning() {
    return handler.isRunning();
  }

  @Override public boolean isStarted() {
    return handler.isStarted();
  }

  @Override public boolean isStarting() {
    return handler.isStarting();
  }

  @Override public boolean isStopping() {
    return handler.isStopping();
  }

  @Override public boolean isStopped() {
    return handler.isStopped();
  }

  @Override public boolean isFailed() {
    return handler.isFailed();
  }

  @Override public void addLifeCycleListener(Listener listener) {
    handler.addLifeCycleListener(listener);
  }

  @Override public void removeLifeCycleListener(Listener listener) {
    handler.removeLifeCycleListener(listener);
  }

  @Override public void setServerRpcMetadata(RpcMetadataResponse metadata) {
    LOG.warn("Setting RpcMetadata is not implemented for DelegatingAvaticaHandler");
  }

}

// End DelegatingAvaticaHandler.java
