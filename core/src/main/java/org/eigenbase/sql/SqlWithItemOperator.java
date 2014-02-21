/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql;

/**
 * SqlWithItemOperator is used to represent an item in a WITH clause of a query.
 * It has a name, an optional column list, and a query.
 */
public class SqlWithItemOperator extends SqlSpecialOperator {
  public SqlWithItemOperator() {
    super("WITH_ITEM", SqlKind.WITH_ITEM, 0);
  }

  //~ Methods ----------------------------------------------------------------

  public void unparse(
      SqlWriter writer,
      SqlNode[] operands,
      int leftPrec,
      int rightPrec) {
    final Call call = Call.of(operands);
    call.name.unparse(writer, getLeftPrec(), getRightPrec());
    if (call.columnList != null) {
      call.columnList.unparse(writer, getLeftPrec(), getRightPrec());
    }
    writer.sep("AS");
    call.query.unparse(writer, getLeftPrec(), getRightPrec());
  }

  /** Copy of the operands to a call to {@link SqlWithItemOperator}, with names
   * and types applied. Use this rather than accessing the operands by
   * position. */
  public static class Call {
    public final SqlIdentifier name;
    public final SqlNodeList columnList; // may be null
    public final SqlNode query;

    private Call(SqlIdentifier name, SqlNodeList columnList, SqlNode query) {
      this.name = name;
      this.columnList = columnList;
      this.query = query;
      assert query.isA(SqlKind.QUERY);
    }

    public static Call of(SqlCall call) {
      assert call.getKind() == SqlKind.WITH_ITEM;
      return of(call.getOperands());
    }

    public static Call of(SqlNode[] operands) {
      assert operands.length == 3;
      return new Call((SqlIdentifier) operands[0], (SqlNodeList) operands[1],
          operands[2]);
    }
  }
}

// End SqlWithItemOperator.java
