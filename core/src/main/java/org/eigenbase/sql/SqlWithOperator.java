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

import java.util.List;

import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;

import com.google.common.collect.ImmutableList;

/**
 * SqlWithOperator is used to represent a WITH clause of a query. It wraps
 * a SELECT, UNION, or INTERSECT.
 */
public class SqlWithOperator extends SqlSpecialOperator {
  //~ Static fields/initializers ---------------------------------------------

  //~ Constructors -----------------------------------------------------------

  public SqlWithOperator() {
    // NOTE:  make precedence lower then SELECT to avoid extra parens
    super("WITH", SqlKind.WITH, 2);
  }

  //~ Methods ----------------------------------------------------------------

  public void unparse(
      SqlWriter writer,
      SqlNode[] operands,
      int leftPrec,
      int rightPrec) {
    final Call call = Call.of(ImmutableList.copyOf(operands));
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.WITH, "WITH", "");
    final SqlWriter.Frame frame1 = writer.startList("", "");
    for (SqlNode node : call.withList) {
      writer.sep(",");
      node.unparse(writer, 0, 0);
    }
    writer.endList(frame1);
    call.body.unparse(writer, 0, 0);
    writer.endList(frame);
  }

  @Override
  public void validateCall(SqlCall call_,
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
    validator.validateWith(call_, scope);
  }

  /** Copy of the operands to a call to {@link SqlWithOperator}, with names and
   * types applied. Use this rather than accessing the operands by position. */
  public static class Call {
    public final SqlNodeList withList;
    public final SqlNode body;

    private Call(SqlNodeList withList, SqlNode body) {
      this.withList = withList;
      this.body = body;
    }

    public static Call of(SqlCall call) {
      assert call.getKind() == SqlKind.WITH;
      return of(call.getOperandList());
    }

    public static Call of(List<SqlNode> operands) {
      assert operands.size() == 2;
      return new Call((SqlNodeList) operands.get(0),
          operands.get(1));
    }
  }
}

// End SqlWithOperator.java
