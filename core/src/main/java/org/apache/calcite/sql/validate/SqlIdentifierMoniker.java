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
package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlIdentifier;

import java.util.List;
import java.util.Objects;

/**
 * An implementation of {@link SqlMoniker} that encapsulates the normalized name
 * information of a {@link SqlIdentifier}.
 */
public class SqlIdentifierMoniker implements SqlMoniker {
  //~ Instance fields --------------------------------------------------------

  private final SqlIdentifier id;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an SqlIdentifierMoniker.
   */
  public SqlIdentifierMoniker(SqlIdentifier id) {
    this.id = Objects.requireNonNull(id);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlMonikerType getType() {
    return SqlMonikerType.COLUMN;
  }

  public List<String> getFullyQualifiedNames() {
    return id.names;
  }

  public SqlIdentifier toIdentifier() {
    return id;
  }

  public String toString() {
    return id.toString();
  }

  public String id() {
    return id.toString();
  }
}

// End SqlIdentifierMoniker.java
