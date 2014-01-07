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
package org.eigenbase.sql.validate;

/**
 * Enumeration of valid SQL compatiblity modes.
 */
public enum SqlConformance {
  Default, Strict92, Strict99, Pragmatic99, Oracle10g, Sql2003, Pragmatic2003;

  /**
   * Whether 'order by 2' is interpreted to mean 'sort by the 2nd column in
   * the select list'.
   */
  public boolean isSortByOrdinal() {
    switch (this) {
    case Default:
    case Oracle10g:
    case Strict92:
    case Pragmatic99:
    case Pragmatic2003:
      return true;
    default:
      return false;
    }
  }

  /**
   * Whether 'order by x' is interpreted to mean 'sort by the select list item
   * whose alias is x' even if there is a column called x.
   */
  public boolean isSortByAlias() {
    switch (this) {
    case Default:
    case Oracle10g:
    case Strict92:
      return true;
    default:
      return false;
    }
  }

  /**
   * Whether "empno" is invalid in "select empno as x from emp order by empno"
   * because the alias "x" obscures it.
   */
  public boolean isSortByAliasObscures() {
    return this == SqlConformance.Strict92;
  }
}

// End SqlConformance.java
