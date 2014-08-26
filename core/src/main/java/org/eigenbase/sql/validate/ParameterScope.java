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

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;

/**
 * A scope which contains nothing besides a few parameters. Like {@link
 * EmptyScope} (which is its base class), it has no parent scope.
 *
 * @see ParameterNamespace
 */
public class ParameterScope extends EmptyScope {
  //~ Instance fields --------------------------------------------------------

  /**
   * Map from the simple names of the parameters to types of the parameters
   * ({@link RelDataType}).
   */
  private final Map<String, RelDataType> nameToTypeMap;

  //~ Constructors -----------------------------------------------------------

  public ParameterScope(
      SqlValidatorImpl validator,
      Map<String, RelDataType> nameToTypeMap) {
    super(validator);
    this.nameToTypeMap = nameToTypeMap;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlIdentifier fullyQualify(SqlIdentifier identifier) {
    return identifier;
  }

  public SqlValidatorScope getOperandScope(SqlCall call) {
    return this;
  }

  public SqlValidatorNamespace resolve(
      String name,
      SqlValidatorScope[] ancestorOut,
      int[] offsetOut) {
    final RelDataType type = nameToTypeMap.get(name);
    return new ParameterNamespace(validator, type);
  }
}

// End ParameterScope.java
