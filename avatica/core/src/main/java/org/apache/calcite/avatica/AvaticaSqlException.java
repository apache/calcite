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
package org.apache.calcite.avatica;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

/**
 * A client-facing {@link SQLException} which encapsulates errors from the remote Avatica server.
 */
public class AvaticaSqlException extends SQLException {

  private static final long serialVersionUID = 1L;

  private final String errorMessage;
  private final List<String> stackTraces;
  private final String remoteServer;

  /**
   * Construct the Exception with information from the server.
   *
   * @param errorMessage A human-readable error message.
   * @param errorCode An integer corresponding to a known error.
   * @param stackTraces Server-side stacktrace.
   * @param remoteServer The host:port where the Avatica server is located
   */
  public AvaticaSqlException(String errorMessage, String sqlState, int errorCode,
      List<String> stackTraces, String remoteServer) {
    super("Error " + errorCode + " (" + sqlState + ") : " + errorMessage, sqlState, errorCode);
    this.errorMessage = errorMessage;
    this.stackTraces = Objects.requireNonNull(stackTraces);
    this.remoteServer = remoteServer;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  /**
   * @return The stacktraces for exceptions thrown on the Avatica server.
   */
  public List<String> getStackTraces() {
    return stackTraces;
  }

  /**
   * @return The host:port for the remote Avatica server. May be null.
   */
  public String getRemoteServer() {
    return remoteServer;
  }

  // printStackTrace() will get redirected to printStackTrace(PrintStream), don't need to override.

  @Override public void printStackTrace(PrintStream stream) {
    super.printStackTrace(stream);
    stream.flush();
    printServerStackTrace(new PrintStreamOrWriter(stream));
  }

  @Override public void printStackTrace(PrintWriter writer) {
    super.printStackTrace(writer);
    writer.flush();
    printServerStackTrace(new PrintStreamOrWriter(writer));
  }

  void printServerStackTrace(PrintStreamOrWriter streamOrWriter) {
    for (String serverStackTrace : this.stackTraces) {
      streamOrWriter.println(serverStackTrace);
    }
  }

  /**
   * A class that encapsulates either a PrintStream or a PrintWriter.
   */
  private static class PrintStreamOrWriter {
    /**
     * Enumeration to differentiate between a PrintStream and a PrintWriter.
     */
    private enum Type {
      STREAM,
      WRITER
    }

    private PrintStream stream;
    private PrintWriter writer;
    private final Type type;

    public PrintStreamOrWriter(PrintStream stream) {
      this.stream = stream;
      type = Type.STREAM;
    }

    public PrintStreamOrWriter(PrintWriter writer) {
      this.writer = writer;
      type = Type.WRITER;
    }

    /**
     * Prints the given string to the the provided stream or writer.
     *
     * @param string The string to print
     */
    public void println(String string) {
      switch (type) {
      case STREAM:
        stream.println(string);
        stream.flush();
        return;
      case WRITER:
        writer.println(string);
        writer.flush();
        return;
      default:
        throw new IllegalStateException();
      }
    }
  }
}

// End AvaticaSqlException.java
