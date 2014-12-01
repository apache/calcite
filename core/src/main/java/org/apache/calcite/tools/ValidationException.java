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
package org.apache.calcite.tools;

/**
 * An Exception thrown when attempting to validate a SQL parse tree.
 */
public class ValidationException extends Exception {
  /** Creates a ValidationException with the specified detail message and
   * cause. */
  public ValidationException(String message, Throwable cause) {
    super(message, cause);
  }

  /** Creates a ValidationException with the specified detail message. */
  public ValidationException(String message) {
    super(message);
  }

  /** Creates a ValidationException with the specified cause. */
  public ValidationException(Throwable cause) {
    super(cause);
  }
}

// End ValidationException.java
