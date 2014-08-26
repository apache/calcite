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
package org.eigenbase.sql;

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;

/**
 * A <code>SqlDynamicParam</code> represents a dynamic parameter marker in an
 * SQL statement. The textual order in which dynamic parameters appear within an
 * SQL statement is the only property which distinguishes them, so this 0-based
 * index is recorded as soon as the parameter is encountered.
 */
public class SqlDynamicParam extends SqlNode {
  //~ Instance fields --------------------------------------------------------

  private final int index;

  //~ Constructors -----------------------------------------------------------

  public SqlDynamicParam(
      int index,
      SqlParserPos pos) {
    super(pos);
    this.index = index;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode clone(SqlParserPos pos) {
    return new SqlDynamicParam(index, pos);
  }

  public SqlKind getKind() {
    return SqlKind.DYNAMIC_PARAM;
  }

  public int getIndex() {
    return index;
  }

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.print("?");
    writer.setNeedWhitespace(false);
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateDynamicParam(this);
  }

  public SqlMonotonicity getMonotonicity(SqlValidatorScope scope) {
    return SqlMonotonicity.CONSTANT;
  }

  public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  public boolean equalsDeep(SqlNode node, boolean fail) {
    if (!(node instanceof SqlDynamicParam)) {
      assert !fail : this + "!=" + node;
      return false;
    }
    SqlDynamicParam that = (SqlDynamicParam) node;
    if (this.index != that.index) {
      assert !fail : this + "!=" + node;
      return false;
    }
    return true;
  }
}

// End SqlDynamicParam.java
