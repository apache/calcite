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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Abstract base for character and binary string literals.
 */
abstract class SqlAbstractStringLiteral extends SqlLiteral {
  //~ Constructors -----------------------------------------------------------

  protected SqlAbstractStringLiteral(
      Object value,
      SqlTypeName typeName,
      SqlParserPos pos) {
    super(value, typeName, pos);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Helper routine for {@link SqlUtil#concatenateLiterals}.
   *
   * @param literals homogeneous StringLiteral args
   * @return StringLiteral with concatenated value. this == lits[0], used only
   * for method dispatch.
   */
  protected abstract SqlAbstractStringLiteral concat1(
      List<SqlLiteral> literals);
}

// End SqlAbstractStringLiteral.java
