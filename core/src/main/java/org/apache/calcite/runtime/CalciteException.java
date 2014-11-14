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
package org.eigenbase.util;

import java.util.logging.*;

// NOTE:  This class gets compiled independently of everything else so that
// resource generation can use reflection.  That means it must have no
// dependencies on other Eigenbase code.


/**
 * Base class for all exceptions originating from Farrago.
 *
 * @see EigenbaseContextException
 */
public class EigenbaseException extends RuntimeException {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * SerialVersionUID created with JDK 1.5 serialver tool. Prevents
   * incompatible class conflict when serialized from JDK 1.5-built server to
   * JDK 1.4-built client.
   */
  private static final long serialVersionUID = -1314522633397794178L;

  private static final Logger LOGGER =
      Logger.getLogger(EigenbaseException.class.getName());

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new EigenbaseException object.
   *
   * @param message error message
   * @param cause   underlying cause
   */
  public EigenbaseException(
      String message,
      Throwable cause) {
    super(message, cause);

    // TODO: Force the caller to pass in a Logger as a trace argument for
    // better context.  Need to extend ResGen for this.
    LOGGER.throwing("EigenbaseException", "constructor", this);
    LOGGER.severe(toString());
  }
}

// End EigenbaseException.java
