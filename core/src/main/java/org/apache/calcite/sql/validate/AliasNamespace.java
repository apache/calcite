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
package org.eigenbase.sql.validate;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.util.Util;

import static org.eigenbase.util.Static.RESOURCE;

/**
 * Namespace for an <code>AS t(c1, c2, ...)</code> clause.
 *
 * <p>A namespace is necessary only if there is a column list, in order to
 * re-map column names; a <code>relation AS t</code> clause just uses the same
 * namespace as <code>relation</code>.
 */
public class AliasNamespace extends AbstractNamespace {
  //~ Instance fields --------------------------------------------------------

  protected final SqlCall call;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AliasNamespace.
   *
   * @param validator     Validator
   * @param call          Call to AS operator
   * @param enclosingNode Enclosing node
   */
  protected AliasNamespace(
      SqlValidatorImpl validator,
      SqlCall call,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.call = call;
    assert call.getOperator() == SqlStdOperatorTable.AS;
  }

  //~ Methods ----------------------------------------------------------------

  protected RelDataType validateImpl() {
    final List<String> nameList = new ArrayList<String>();
    final List<SqlNode> operands = call.getOperandList();
    final SqlValidatorNamespace childNs =
        validator.getNamespace(operands.get(0));
    final RelDataType rowType = childNs.getRowTypeSansSystemColumns();
    for (final SqlNode operand : Util.skip(operands, 2)) {
      String name = ((SqlIdentifier) operand).getSimple();
      if (nameList.contains(name)) {
        throw validator.newValidationError(operand,
            RESOURCE.aliasListDuplicate(name));
      }
      nameList.add(name);
    }
    if (nameList.size() != rowType.getFieldCount()) {
      // Position error at first name in list.
      throw validator.newValidationError(operands.get(2),
          RESOURCE.aliasListDegree(rowType.getFieldCount(), getString(rowType),
              nameList.size()));
    }
    final List<RelDataType> typeList = new ArrayList<RelDataType>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      typeList.add(field.getType());
    }
    return validator.getTypeFactory().createStructType(
        typeList,
        nameList);
  }

  private String getString(RelDataType rowType) {
    StringBuilder buf = new StringBuilder();
    buf.append("(");
    for (RelDataTypeField field : rowType.getFieldList()) {
      if (field.getIndex() > 0) {
        buf.append(", ");
      }
      buf.append("'");
      buf.append(field.getName());
      buf.append("'");
    }
    buf.append(")");
    return buf.toString();
  }

  public SqlNode getNode() {
    return call;
  }

  public String translate(String name) {
    final RelDataType underlyingRowType =
        validator.getValidatedNodeType(call.operand(0));
    int i = 0;
    for (RelDataTypeField field : rowType.getFieldList()) {
      if (field.getName().equals(name)) {
        return underlyingRowType.getFieldList().get(i).getName();
      }
      ++i;
    }
    throw new AssertionError(
        "unknown field '" + name
        + "' in rowtype " + underlyingRowType);
  }
}

// End AliasNamespace.java
