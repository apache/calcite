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
package org.apache.calcite.sql.validate.implicit;

import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * Factory class for type coercion instantiation of different sql dialects.
 */
public class TypeCoercions {
  private TypeCoercions() {}

  // All the SqlConformance would have default TypeCoercion instance.
  public static TypeCoercion getTypeCoercion(SqlValidator validator,
      SqlConformance conformance) {
    return new TypeCoercionImpl(validator);
  }
}

// End TypeCoercions.java
