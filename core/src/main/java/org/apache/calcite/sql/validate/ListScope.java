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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Abstract base for a scope which is defined by a list of child namespaces and
 * which inherits from a parent scope.
 */
public abstract class ListScope extends DelegatingScope {
  //~ Instance fields --------------------------------------------------------

  /**
   * List of child {@link SqlValidatorNamespace} objects and their names.
   */
  protected final List<Pair<String, SqlValidatorNamespace>> children =
      new ArrayList<Pair<String, SqlValidatorNamespace>>();

  //~ Constructors -----------------------------------------------------------

  public ListScope(SqlValidatorScope parent) {
    super(parent);
  }

  //~ Methods ----------------------------------------------------------------

  public void addChild(SqlValidatorNamespace ns, String alias) {
    assert alias != null;
    children.add(Pair.of(alias, ns));
  }

  /**
   * Returns an immutable list of child namespaces.
   *
   * @return list of child namespaces
   */
  public List<SqlValidatorNamespace> getChildren() {
    return Pair.right(children);
  }

  protected SqlValidatorNamespace getChild(String alias) {
    if (alias == null) {
      if (children.size() != 1) {
        throw Util.newInternal(
            "no alias specified, but more than one table in from list");
      }
      return children.get(0).right;
    } else {
      final int i = validator.catalogReader.match(Pair.left(children), alias);
      if (i >= 0) {
        return children.get(i).right;
      }
      return null;
    }
  }

  /** Returns a child namespace that matches a fully-qualified list of names.
   * This will be a schema-qualified table, for example
   *
   * <blockquote><pre>SELECT sales.emp.empno FROM sales.emp</pre></blockquote>
   */
  protected SqlValidatorNamespace getChild(List<String> names) {
    int i = findChild(names);
    return i < 0 ? null : children.get(i).right;
  }

  protected int findChild(List<String> names) {
    for (int i = 0; i < children.size(); i++) {
      Pair<String, SqlValidatorNamespace> child = children.get(i);
      if (child.left != null) {
        if (validator.catalogReader.matches(child.left, Util.last(names))) {
          if (names.size() == 1) {
            return i;
          }
        } else {
          // Alias does not match last segment. Don't consider the
          // fully-qualified name. E.g.
          //    SELECT sales.emp.name FROM sales.emp AS otherAlias
          continue;
        }
      }

      // Look up the 2 tables independently, in case one is qualified with
      // catalog & schema and the other is not.
      final SqlValidatorTable table = child.right.getTable();
      if (table != null) {
        final SqlValidatorTable table2 =
            validator.catalogReader.getTable(names);
        if (table2 != null
            && table.getQualifiedName().equals(table2.getQualifiedName())) {
          return i;
        }
      }
    }
    return -1;
  }

  public void findAllColumnNames(List<SqlMoniker> result) {
    for (Pair<String, SqlValidatorNamespace> pair : children) {
      addColumnNames(pair.right, result);
    }
    parent.findAllColumnNames(result);
  }

  public void findAliases(Collection<SqlMoniker> result) {
    for (Pair<String, SqlValidatorNamespace> pair : children) {
      result.add(new SqlMonikerImpl(pair.left, SqlMonikerType.TABLE));
    }
    parent.findAliases(result);
  }

  public Pair<String, SqlValidatorNamespace>
  findQualifyingTableName(final String columnName, SqlNode ctx) {
    int count = 0;
    Pair<String, SqlValidatorNamespace> tableName = null;
    for (Pair<String, SqlValidatorNamespace> child : children) {
      final RelDataType rowType = child.right.getRowType();
      if (validator.catalogReader.field(rowType, columnName) != null) {
        tableName = child;
        count++;
      }
    }
    switch (count) {
    case 0:
      return parent.findQualifyingTableName(columnName, ctx);
    case 1:
      return tableName;
    default:
      throw validator.newValidationError(ctx,
          RESOURCE.columnAmbiguous(columnName));
    }
  }

  public SqlValidatorNamespace resolve(
      List<String> names,
      SqlValidatorScope[] ancestorOut,
      int[] offsetOut) {
    // First resolve by looking through the child namespaces.
    final int i = findChild(names);
    if (i >= 0) {
      if (ancestorOut != null) {
        ancestorOut[0] = this;
      }
      if (offsetOut != null) {
        offsetOut[0] = i;
      }
      return children.get(i).right;
    }

    // Then call the base class method, which will delegate to the
    // parent scope.
    return parent.resolve(names, ancestorOut, offsetOut);
  }

  public RelDataType resolveColumn(String columnName, SqlNode ctx) {
    int found = 0;
    RelDataType type = null;
    for (Pair<String, SqlValidatorNamespace> pair : children) {
      SqlValidatorNamespace childNs = pair.right;
      final RelDataType childRowType = childNs.getRowType();
      final RelDataTypeField field =
          validator.catalogReader.field(childRowType, columnName);
      if (field != null) {
        found++;
        type = field.getType();
      }
    }
    switch (found) {
    case 0:
      return null;
    case 1:
      return type;
    default:
      throw validator.newValidationError(ctx,
          RESOURCE.columnAmbiguous(columnName));
    }
  }
}

// End ListScope.java
