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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;

/**
 * A <code>SqlTypeNameSpec</code> is a type name specification that allows user to
 * customize sql node unparsing and data type deriving.
 *
 * <p>To customize sql node unparsing, override the method
 * {@link #unparse(SqlWriter, int, int)}.
 * <p>To customize data type deriving, override the method
 * {@link #deriveType(SqlValidator)}.
 */
public abstract class SqlTypeNameSpec {
  private final SqlIdentifier typeName;
  private final SqlParserPos pos;

  /**
   * Creates a {@code SqlTypeNameSpec}.
   *
   * @param name Name of the type.
   * @param pos  Parser position, must not be null.
   */
  SqlTypeNameSpec(SqlIdentifier name, SqlParserPos pos) {
    this.typeName = name;
    this.pos = pos;
  }

  /**
   * Derive type from this SqlTypeNameSpec.
   *
   * @param validator The sql validator.
   * @return the {@code RelDataType} instance, throws exception if we could not
   *         deduce the type.
   */
  public abstract RelDataType deriveType(SqlValidator validator);

  /** Writes a SQL representation of this spec to a writer. */
  public abstract void unparse(SqlWriter writer, int leftPrec, int rightPrec);

  /** Returns whether this spec is structurally equivalent to another spec. */
  public abstract boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus);

  public SqlParserPos getParserPos() {
    return pos;
  }

  public SqlIdentifier getTypeName() {
    return typeName;
  }
}

// End SqlTypeNameSpec.java
