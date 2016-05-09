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
package org.apache.calcite.avatica.hsqldb;

import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.HttpServer;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import net.hydromatic.scott.data.hsqldb.ScottHsqldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Avatica server for HSQLDB.
 */
public class HsqldbServer {
  private static final Logger LOG = LoggerFactory.getLogger(HsqldbServer.class);

  @Parameter(names = { "-p", "--port" }, required = false,
      description = "Port the server should bind")
  private int port = 0;

  @Parameter(names = { "-s", "--serialization" }, required = false,
      description = "Serialization method to use", converter = SerializationConverter.class)
  private Serialization serialization = Serialization.PROTOBUF;

  private HttpServer server;

  public void start() {
    if (null != server) {
      LOG.error("The server was already started");
      System.exit(ExitCodes.ALREADY_STARTED.ordinal());
      return;
    }

    try {
      // Set up Julian's ScottDB for HSQLDB
      JdbcMeta meta = new JdbcMeta(ScottHsqldb.URI, ScottHsqldb.USER, ScottHsqldb.PASSWORD);
      LocalService service = new LocalService(meta);

      // Construct the server
      this.server = new HttpServer.Builder()
          .withHandler(service, serialization)
          .withPort(port)
          .build();

      // Then start it
      server.start();

      LOG.info("Started Avatica server on port {} with serialization {}", server.getPort(),
          serialization);
    } catch (Exception e) {
      LOG.error("Failed to start Avatica server", e);
      System.exit(ExitCodes.START_FAILED.ordinal());
    }
  }

  public void stop() {
    if (null != server) {
      server.stop();
      server = null;
    }
  }

  public void join() throws InterruptedException {
    server.join();
  }

  public static void main(String[] args) {
    final HsqldbServer server = new HsqldbServer();
    new JCommander(server, args);

    server.start();

    // Try to clean up when the server is stopped.
    Runtime.getRuntime().addShutdownHook(
        new Thread(new Runnable() {
          @Override public void run() {
            LOG.info("Stopping server");
            server.stop();
            LOG.info("Server stopped");
          }
        }));

    try {
      server.join();
    } catch (InterruptedException e) {
      // Reset interruption
      Thread.currentThread().interrupt();
      // And exit now.
      return;
    }
  }

  /**
   * Converter from String to Serialization.
   */
  private static class SerializationConverter implements IStringConverter<Serialization> {
    @Override public Serialization convert(String value) {
      return Serialization.valueOf(value.toUpperCase());
    }
  }

  /**
   * Codes for exit conditions
   */
  private enum ExitCodes {
    NORMAL,
    ALREADY_STARTED, // 1
    START_FAILED;    // 2
  }
}

// End HsqldbServer.java
