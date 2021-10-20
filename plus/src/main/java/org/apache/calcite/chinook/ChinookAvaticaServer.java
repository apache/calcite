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
package org.apache.calcite.chinook;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.AvaticaProtobufHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;

import com.google.common.collect.ImmutableList;

import net.hydromatic.chinook.data.hsqldb.ChinookHsqldb;

import org.eclipse.jetty.server.handler.AbstractHandler;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * Wrapping Calcite engine with Avatica tansport for testing JDBC capabilities
 * between Avatica JDBC transport and Calcite.
 */
public class ChinookAvaticaServer {
  private final Config config;
  private HttpServer server;

  private ChinookAvaticaServer(Config config) {
    this.config = config;
  }

  /** Creates a ChinookAvaticaServer with default options. */
  public ChinookAvaticaServer() {
    this(Config.DEFAULT);
  }

  private HttpServer start(Config config) throws Exception {
    return Main.start(config.argList.toArray(new String[0]), config.port,
        config.handlerFactory);
  }

  public void startWithCalcite() throws Exception {
    final String[] args = {CalciteChinookMetaFactory.class.getName()};
    final Config config2 = config.withArgs(ImmutableList.copyOf(args))
        .withHandlerFactory(AvaticaProtobufHandler::new);
    this.server = start(config2);
  }

  public void startWithRaw() throws Exception {
    final String[] args = {RawChinookMetaFactory.class.getName()};
    final Config config2 = config.withArgs(ImmutableList.copyOf(args))
        .withHandlerFactory(AvaticaProtobufHandler::new);
    this.server = start(config2);
  }

  /** Starts the server on port 57198. */
  public static void main(String[] args) throws Exception {
    final String[] args2 = {CalciteChinookMetaFactory.class.getName()};
    final Config config = Config.DEFAULT.withPort(57198)
        .withArgs(ImmutableList.copyOf(args2));
    final ChinookAvaticaServer server = new ChinookAvaticaServer(config);
    server.startWithCalcite();
    for (;;) {
      Thread.sleep(10_000L);
    }
  }

  public String getURL() {
    return "jdbc:avatica:remote:url=http://localhost:" + server.getPort()
        + ";serialization=" + Driver.Serialization.PROTOBUF.name();
  }

  public void stop() {
    server.stop();
  }

  /**
   * Factory for Chinook Calcite database wrapped in meta for Avatica.
   */
  public static class CalciteChinookMetaFactory implements Meta.Factory {
    private static final CalciteConnectionProvider CONNECTION_PROVIDER =
        new CalciteConnectionProvider();

    private static JdbcMeta instance = null;

    private static JdbcMeta getInstance() {
      if (instance == null) {
        try {
          instance =
              new JdbcMeta(CalciteConnectionProvider.DRIVER_URL,
                  CONNECTION_PROVIDER.provideConnectionInfo());
        } catch (SQLException | IOException e) {
          throw new RuntimeException(e);
        }
      }
      return instance;
    }

    @Override public Meta create(List<String> args) {
      return getInstance();
    }
  }

  /**
   * Factory for Chinook Calcite database wrapped in meta for Avatica.
   */
  public static class RawChinookMetaFactory implements Meta.Factory {
    private static JdbcMeta instance = null;

    private static JdbcMeta getInstance() {
      if (instance == null) {
        try {
          instance =
              new JdbcMeta(ChinookHsqldb.URI,
                  ChinookHsqldb.USER, ChinookHsqldb.PASSWORD);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
      return instance;
    }

    @Override public Meta create(List<String> args) {
      return getInstance();
    }
  }

  /** Configuration. (Immutable.) */
  private static class Config {
    final int port;
    final ImmutableList<String> argList;
    final Main.HandlerFactory handlerFactory;

    static final Config DEFAULT =
        new Config(0, ImmutableList.of(), Config::barf);

    Config(int port, ImmutableList<String> argList,
        Main.HandlerFactory handlerFactory) {
      this.port = port;
      this.argList = argList;
      this.handlerFactory = handlerFactory;
    }

    private static AbstractHandler barf(Service s) {
      throw new UnsupportedOperationException();
    }

    Config withArgs(ImmutableList<String> argList) {
      return this.argList.equals(argList) ? this
          : new Config(port, argList, handlerFactory);
    }

    Config withPort(int port) {
      return this.port == port ? this
          : new Config(port, argList, handlerFactory);
    }

    Config withHandlerFactory(Main.HandlerFactory handlerFactory) {
      return this.handlerFactory == handlerFactory ? this
          : new Config(port, argList, handlerFactory);
    }
  }
}
