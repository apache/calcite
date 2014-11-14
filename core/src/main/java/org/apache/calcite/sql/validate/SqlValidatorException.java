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
package org.eigenbase.sql.validate;

import java.util.logging.*;

// NOTE:  This class gets compiled independently of everything else so that
// resource generation can use reflection.  That means it must have no
// dependencies on other Eigenbase/Farrago code.

import org.eigenbase.util14.*;

/**
 * Exception thrown while validating a SQL statement.
 *
 * <p>Unlike {@link org.eigenbase.util.EigenbaseException}, this is a checked
 * exception, which reminds code authors to wrap it in another exception
 * containing the line/column context.
 */
public class SqlValidatorException extends Exception
    implements EigenbaseValidatorException {
  //~ Static fields/initializers ---------------------------------------------

  private static final Logger LOGGER =
      Logger.getLogger("org.eigenbase.util.EigenbaseException");

  static final long serialVersionUID = -831683113957131387L;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new SqlValidatorException object.
   *
   * @param message error message
   * @param cause   underlying cause
   */
  public SqlValidatorException(
      String message,
      Throwable cause) {
    super(message, cause);

    // TODO: see note in EigenbaseException constructor
    LOGGER.throwing("SqlValidatorException", "constructor", this);
    LOGGER.severe(toString());
  }
}

// End SqlValidatorException.java
