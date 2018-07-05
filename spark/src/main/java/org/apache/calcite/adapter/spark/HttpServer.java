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
package org.apache.calcite.adapter.spark;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Iterator;

/**
 * An HTTP server for static content used to allow worker nodes to access JARs.
 *
 * <p>Based on Spark HttpServer, wraps a Jetty server.</p>
 */
class HttpServer {
  private static String localIpAddress;

  private final File resourceBase;

  HttpServer(File resourceBase) {
    this.resourceBase = resourceBase;
  }

  private Server server;
  private int port = -1;

  void start() {
    if (server != null) {
      throw new RuntimeException("Server is already started");
    } else {
      QueuedThreadPool threadPool = new QueuedThreadPool();
      threadPool.setDaemon(true);
      server = new Server(threadPool);
      server.manage(threadPool);

      final ServerConnector connector = new ServerConnector(server);
      connector.setIdleTimeout(60 * 1000);
      connector.setSoLingerTime(-1);
      connector.setPort(0);
      server.setConnectors(new Connector[] { connector });

      final ResourceHandler resHandler = new ResourceHandler();
      resHandler.setResourceBase(resourceBase.getAbsolutePath());

      final HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[] {resHandler, new DefaultHandler()});
      server.setHandler(handlerList);
      try {
        server.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      port = connector.getLocalPort();
    }
  }

  void stop() {
    if (server == null) {
      throw new RuntimeException("Server is already stopped");
    } else {
      try {
        final Server server1 = server;
        port = -1;
        server = null;
        server1.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Returns the URI of this HTTP server ("http://host:port").
   */
  String uri() {
    if (server == null) {
      throw new RuntimeException("Server is not started");
    } else {
      return "http://" + localIpAddress() + ":" + port;
    }
  }

  /**
   * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4).
   * Note, this is typically not used from within core spark.
   */
  static synchronized String localIpAddress() {
    synchronized (HttpServer.class) {
      if (localIpAddress == null) {
        try {
          localIpAddress = findLocalIpAddress();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return localIpAddress;
  }

  private static String findLocalIpAddress() throws IOException {
    String defaultIpOverride = System.getenv("CALCITE_LOCAL_IP");
    if (defaultIpOverride != null) {
      return defaultIpOverride;
    } else {
      final InetAddress address = InetAddress.getLocalHost();
      if (address.isLoopbackAddress()) {
        // Address resolves to something like 127.0.1.1, which happens on
        // Debian; try to find a better address using the local network
        // interfaces.
        for (NetworkInterface ni
            : iterable(NetworkInterface.getNetworkInterfaces())) {
          for (InterfaceAddress interfaceAddress : ni.getInterfaceAddresses()) {
            final InetAddress addr = interfaceAddress.getAddress();
            if (!addr.isLinkLocalAddress()
                && !addr.isLoopbackAddress() && addr instanceof Inet4Address) {
              // We've found an address that looks reasonable!
              logWarning("Your hostname, "
                  + InetAddress.getLocalHost().getHostName()
                  + " resolves to a loopback address: "
                  + address.getHostAddress() + "; using "
                  + addr.getHostAddress() + " instead (on interface "
                  + ni.getName() + ")");
              logWarning(
                  "Set CALCITE_LOCAL_IP if you need to bind to another address");
              return addr.getHostAddress();
            }
          }
        }
        logWarning(
            "Your hostname, " + InetAddress.getLocalHost().getHostName()
            + " resolves to a loopback address: " + address.getHostAddress()
            + ", but we couldn't find any external IP address!");
        logWarning(
            "Set CALCITE_LOCAL_IP if you need to bind to another address");
      }
      return address.getHostAddress();
    }
  }

  private static <E> Iterable<E> iterable(final Enumeration<E> enumeration) {
    return () -> new Iterator<E>() {
      public boolean hasNext() {
        return enumeration.hasMoreElements();
      }

      public E next() {
        return enumeration.nextElement();
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static void logWarning(String s) {
    System.out.println(s);
  }
}

// End HttpServer.java
