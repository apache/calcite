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
import org.apache.calcite.util.Litmus;

import java.util.Objects;

/**
 * Represents a type name for an alien system. For example,
 * UNSIGNED is a built-in type in MySQL which is synonym of INTEGER.
 *
 * <p>You can use this class to define a customized type name with specific alias,
 * for example, in some systems, STRING is synonym of VARCHAR
 * and BYTES is synonym of VARBINARY.
 *
 * <p>Internally we may use the {@link SqlAlienSystemTypeNameSpec} to unparse
 * as the builtin data type name for some alien systems during rel-to-sql conversion.
 */
public class SqlAlienSystemTypeNameSpec extends SqlBasicTypeNameSpec {
  //~ Instance fields --------------------------------------------------------

  // Type alias used for unparsing.
  private final String typeAlias;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a {@code SqlAlienSystemTypeNameSpec} instance.
   *
   * @param typeAlias Type alias of the alien system
   * @param typeName  Type name the {@code typeAlias} implies as the (standard) basic type name
   * @param pos       The parser position
   */
  public SqlAlienSystemTypeNameSpec(
      String typeAlias,
      SqlTypeName typeName,
      SqlParserPos pos) {
    this(typeAlias, typeName, -1, pos);
  }

  /**
   * Creates a {@code SqlAlienSystemTypeNameSpec} instance.
   *
   * @param typeAlias Type alias of the alien system
   * @param typeName  Type name the {@code typeAlias} implies as the (standard) basic type name
   * @param precision Type Precision
   * @param pos       The parser position
   */
  public SqlAlienSystemTypeNameSpec(
      String typeAlias,
      SqlTypeName typeName,
      int precision,
      SqlParserPos pos) {
    super(typeName, precision, pos);
    this.typeAlias = typeAlias;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(typeAlias);
  }

  @Override public boolean equalsDeep(SqlTypeNameSpec node, Litmus litmus) {
    if (!(node instanceof SqlAlienSystemTypeNameSpec)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlAlienSystemTypeNameSpec that = (SqlAlienSystemTypeNameSpec) node;
    if (!Objects.equals(this.typeAlias, that.typeAlias)) {
      return litmus.fail("{} != {}", this, node);
    }
    return super.equalsDeep(node, litmus);
  }
}

// End SqlAlienSystemTypeNameSpec.java
