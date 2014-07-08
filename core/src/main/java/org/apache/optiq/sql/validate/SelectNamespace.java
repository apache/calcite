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
package org.apache.optiq.sql.validate;

import org.apache.optiq.reltype.*;
import org.apache.optiq.sql.*;
import org.apache.optiq.sql.type.*;

/**
 * Namespace offered by a subquery.
 *
 * @see SelectScope
 * @see SetopNamespace
 */
public class SelectNamespace extends AbstractNamespace {
  //~ Instance fields --------------------------------------------------------

  private final SqlSelect select;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SelectNamespace.
   *
   * @param validator     Validate
   * @param select        Select node
   * @param enclosingNode Enclosing node
   */
  public SelectNamespace(
      SqlValidatorImpl validator,
      SqlSelect select,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.select = select;
  }

  //~ Methods ----------------------------------------------------------------

  // implement SqlValidatorNamespace, overriding return type
  public SqlSelect getNode() {
    return select;
  }

  public RelDataType validateImpl() {
    validator.validateSelect(select, validator.unknownType);
    return rowType;
  }

  public SqlMonotonicity getMonotonicity(String columnName) {
    final RelDataType rowType = this.getRowTypeSansSystemColumns();
    final int field = SqlTypeUtil.findField(rowType, columnName);
    final SqlNode selectItem =
        validator.getRawSelectScope(select)
            .getExpandedSelectList().get(field);
    return validator.getSelectScope(select).getMonotonicity(selectItem);
  }
}

// End SelectNamespace.java
