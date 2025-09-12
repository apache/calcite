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
package org.apache.calcite.adapter.govdata;

/**
 * Exception thrown by government data adapters.
 *
 * <p>This is a common exception class used across all government data sources
 * to provide consistent error handling and messaging.
 */
public class GovDataException extends RuntimeException {
  
  /**
   * Creates a new GovDataException with the specified message.
   */
  public GovDataException(String message) {
    super(message);
  }

  /**
   * Creates a new GovDataException with the specified message and cause.
   */
  public GovDataException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new GovDataException with the specified cause.
   */
  public GovDataException(Throwable cause) {
    super(cause);
  }
}