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

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;

import java.util.Arrays;

/**
 * Jetty handler that executes Avatica JSON request-responses.
 */
public class Main {
  private Main() {}

  public static void main(String[] args)
      throws InterruptedException, ClassNotFoundException,
      IllegalAccessException, InstantiationException {
    HttpServer server = start(args);
    server.join();
  }

  /**
   * Creates and starts an {@link HttpServer}.
   *
   * <p>Arguments are as follows:
   * <ul>
   *   <li>args[0]: the {@link org.apache.calcite.avatica.Meta.Factory} class
   *       name
   *   <li>args[1+]: arguments passed along to
   *   {@link org.apache.calcite.avatica.Meta.Factory#create(java.util.List)}
   * </ul>
   *
   * @param args Command-line arguments
   */
  public static HttpServer start(String[] args)
      throws ClassNotFoundException, InstantiationException,
      IllegalAccessException {
    String factoryClassName = args[0];
    Class factoryClass = Class.forName(factoryClassName);
    Meta.Factory factory = (Meta.Factory) factoryClass.newInstance();
    Meta meta = factory.create(Arrays.asList(args).subList(1, args.length));
    Service service = new LocalService(meta);
    HttpServer server = new HttpServer(8765, new AvaticaHandler(service));
    server.start();
    return server;
  }
}

// End Main.java
