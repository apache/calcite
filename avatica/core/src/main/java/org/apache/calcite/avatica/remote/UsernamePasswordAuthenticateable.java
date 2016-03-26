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

/**
 * Interface that allows configuration of a username and password with some HTTP authentication.
 */
public interface UsernamePasswordAuthenticateable {

  /**
   * Sets the username, password and method to be used for authentication.
   *
   * @param authType Type of authentication
   * @param username Username
   * @param password Password
   */
  void setUsernamePassword(AuthenticationType authType, String username, String password);

}

// End UsernamePasswordAuthenticateable.java
