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
package org.apache.calcite.runtime;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import javax.net.SocketFactory;

/**
 * Extends the SocketFactory object with the main functionality being that the
 * created sockets inherit a set of options whose values are set in the
 * SocketFactoryImpl.
 *
 * <blockquote><pre>
 * 1.  SO_KEEPALIVE          - is keepalive enabled?
 * 2.  OOBINLINE             - is out of band in-line enabled?
 * 3.  SO_REUSEADDR          - should the address be reused?
 * 4.  TCP_NODELAY           - should data buffering for tcp be used?
 * 5.  SO_RCVBUF             - size of receive buffer
 * 6.  SO_SNDBUF             - size of send buffer
 * 7.  SO_TIMEOUT            - read timeout (millisecs)
 * 8.  SO_CONNECT_TIMEOUT    - connect timeout (millisecs)
 * 9.  SO_LINGER             - is lingering enabled?
 * 10. LINGER                - amount of time to linger (seconds)
 * </pre></blockquote>
 */
public class SocketFactoryImpl extends SocketFactory {
  /**
   * should keep alives be sent
   */
  public static final boolean SO_KEEPALIVE = false;

  /**
   * is out of band in-line enabled
   */
  public static final boolean OOBINLINE = false;

  /**
   * should the address be reused
   */
  public static final boolean SO_REUSEADDR = false;

  /**
   * do not buffer send(s) iff true
   */
  public static final boolean TCP_NODELAY = true;

  /**
   * size of receiving buffer
   */
  public static final int SO_RCVBUF = 8192;

  /**
   * size of sending buffer iff needed
   */
  public static final int SO_SNDBUF = 1024;

  /**
   * read timeout in milliseconds
   */
  public static final int SO_TIMEOUT = 12000;

  /**
   * connect timeout in milliseconds
   */
  public static final int SO_CONNECT_TIMEOUT = 5000;

  /**
   * enabling lingering with 0-timeout will cause the socket to be
   * closed forcefully upon execution of close()
   */
  public static final boolean SO_LINGER = true;

  /**
   * amount of time to linger
   */
  public static final int LINGER = 0;

  @Override public Socket createSocket() throws IOException {
    Socket s = new Socket();
    return applySettings(s);
  }

  /**
   * Applies the current settings to the given socket.
   *
   * @param s Socket to apply the settings to
   * @return Socket the input socket
   */
  protected Socket applySettings(Socket s) {
    try {
      s.setKeepAlive(SO_KEEPALIVE);
      s.setOOBInline(OOBINLINE);
      s.setReuseAddress(SO_REUSEADDR);
      s.setTcpNoDelay(TCP_NODELAY);
      s.setOOBInline(OOBINLINE);

      s.setReceiveBufferSize(SO_RCVBUF);
      s.setSendBufferSize(SO_SNDBUF);
      s.setSoTimeout(SO_TIMEOUT);
      s.setSoLinger(SO_LINGER, LINGER);
    } catch (SocketException e) {
      throw new RuntimeException(e);
    }
    return s;
  }

  @Override public Socket createSocket(String host, int port)
      throws IOException {
    Socket s = createSocket();
    s.connect(new InetSocketAddress(host, port), SO_CONNECT_TIMEOUT);
    return s;
  }

  @Override public Socket createSocket(InetAddress host, int port)
      throws IOException {
    Socket s = createSocket();
    s.connect(new InetSocketAddress(host, port), SO_CONNECT_TIMEOUT);
    return s;
  }

  @Override public Socket createSocket(String host, int port, InetAddress local,
      int localPort) throws IOException {
    Socket s  = createSocket();
    s.bind(new InetSocketAddress(local, localPort));
    s.connect(new InetSocketAddress(host, port), SO_CONNECT_TIMEOUT);
    return s;
  }

  @Override public Socket createSocket(InetAddress host, int port,
      InetAddress local, int localPort) throws IOException {
    Socket s  = createSocket();
    s.bind(new InetSocketAddress(local, localPort));
    s.connect(new InetSocketAddress(host, port), SO_CONNECT_TIMEOUT);
    return s;
  }

  /**
   * @see javax.net.SocketFactory#getDefault()
   */
  public static SocketFactory getDefault() {
    return new SocketFactoryImpl();
  }
}

// End SocketFactoryImpl.java
