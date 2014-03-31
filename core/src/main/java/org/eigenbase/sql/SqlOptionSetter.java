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

import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.util.SqlVisitor;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;

/**
 * Node to represent set option statement.
 */
public class SqlOptionSetter extends SqlNode {

  String name;
  SqlNode value;
  String rawValue;
  String scope;

  /**
   * Creates a node.
   *
   * @param pos Parser position, must not be null.
   */
  public SqlOptionSetter(SqlParserPos pos) {
    super(pos);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("alter");
    writer.keyword(getScope());
    writer.keyword("set");
    final SqlWriter.Frame frame =
      writer.startList(
        SqlWriter.FrameTypeEnum.SIMPLE);
    writer.identifier(getName());
    writer.sep("=");
    if (value != null) {
      getVal().unparse(writer, leftPrec, rightPrec);
    } else {
      writer.literal(getRawVal());
    }
    writer.endList(frame);
  }

  /**
   * Options can be set to several valid sql types, such as literals, or in
   * some cases, such as 'on', set to reserved words that need not be given
   * in quotes as string literals. In this case these are saved by the parser
   * as strings in the rawValue field. Only one of these should hold a value
   * in an instance of {@link SqlOptionSetter }.
   *
   * @param validator
   * @param scope     Validator
   */
  @Override
  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    if (getVal() != null) {
      validator.validate(getVal());
    }
  }

  @Override
  public <R> R accept(SqlVisitor<R> visitor) {
    // TODO - figure out if this type should be added to the visitor
    // definition or if this class should be declared a subclass
    // of SqlCall
    return visitor.visit((SqlLiteral) this.getVal());
  }

  @Override
  public boolean equalsDeep(SqlNode node, boolean fail) {
    if (!(node instanceof SqlCall)) {
      assert !fail : this + "!=" + node;
      return false;
    }
    SqlOptionSetter that = (SqlOptionSetter) node;
    if (!this.getScope().equals(that.getScope())) {
      assert !fail : this + "!=" + node;
      return false;
    }
    if (!this.getName().equals(that.getName())) {
      assert !fail : this + "!=" + node;
      return false;
    }
    if (this.getVal() != null) {
      if (!this.getVal().equalsDeep(that.getVal(), fail)) {
        assert !fail : this + "!=" + node;
        return false;
      }
    }
    if (this.getRawVal() != null) {
      if (!this.getVal().equals(that.getVal())) {
        assert !fail : this + "!=" + node;
        return false;
      }
    }
    return true;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getScope() {
    return scope;
  }

  public void setScope(String scope) {
    this.scope = scope;
  }

  public String getRawVal() {
    return rawValue;
  }

  public void setRawVal(String rawValue) {
    assert this.value == null
      : "Either a rawValue or a typed value can be set in an "
      + "SqlOptionSetter instance, setting both is invalid.";
    this.rawValue = rawValue;
  }

  public SqlNode getVal() {
    return value;
  }

  public void setVal(SqlNode val) {
    assert this.rawValue == null
      : "Either a rawValue or a typed value can be set in an "
      + "SqlOptionSetter instance, setting both is invalid.";
    this.value = val;
  }
}
