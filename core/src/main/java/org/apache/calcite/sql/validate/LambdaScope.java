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
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The name-resolution context for expression inside a LAMBDA clause. The objects visible are the
 * lambda paramters, and those inherited from the parent scope.
 *
 * <p>Consider "SELECT lambdaExp((x)-&gt;A.a+x) FROM A
 * resolved in the lambda scope for "Lambda Parameters and Table columns of A"</p>
 */
public class LambdaScope extends ListScope {

  //~ Instance fields --------------------------------------------------------

  private final SqlLambda lambda;
  private Map<String, RelDataType> parameterTypes = new LinkedHashMap<>();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LambdaScope.
   */
  public LambdaScope(
      SqlLambda lambda,
      SqlValidatorScope parent) {
    super(parent);
    this.lambda = lambda;

    for (int i = 0; i < lambda.getParameters().size(); i++) {
      SqlIdentifier identifier = (SqlIdentifier) lambda.getParameters().get(i);
      parameterTypes.put(identifier.getSimple(),
          validator.typeFactory.createSqlType(SqlTypeName.ANY));
    }
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlNode getNode() {
    return lambda;
  }

  public SqlQualified fullyQualify(SqlIdentifier identifier) {
    if (parameterTypes.containsKey(identifier.getSimple())) {
      return SqlQualified.create(this, 1, null, identifier);
    } else {
      return parent.fullyQualify(identifier);
    }
  }

  @Override public RelDataType resolveColumn(String name, SqlNode ctx) {
    RelDataType parameterType = parameterTypes.get(name);
    if (parameterType != null) {
      return parameterType;
    } else {
      return parent.resolveColumn(name, ctx);
    }
  }

  public void setParameterType(int i, SqlTypeFamily typeFamily) {
    SqlIdentifier identifier = (SqlIdentifier) lambda.getParameters().get(i);
    if (typeFamily == SqlTypeFamily.ANY) {
      parameterTypes.put(identifier.getSimple(),
          validator.typeFactory.createSqlType(SqlTypeName.ANY));
    } else {
      parameterTypes.put(identifier.getSimple(),
          typeFamily.getDefaultConcreteType(validator.typeFactory));
    }
  }

  public Map<String, RelDataType> getParameters() {
    return parameterTypes;
  }

  @Override public SqlValidatorNamespace getTableNamespace(List<String> names) {
    if (names.size() == 1 && parameterTypes.containsKey(names.get(0))) {
      return validator.getNamespace(lambda);
    }
    return parent.getTableNamespace(names);
  }

  @Override public void resolveTable(List<String> names,
      SqlNameMatcher nameMatcher, Path path, Resolved resolved) {
    if (names.size() == 1 && parameterTypes.containsKey(names.get(0))) {
      final SqlValidatorNamespace ns = validator.getNamespace(lambda);
      final Step path2 = path
          .plus(ns.getRowType(), 0, names.get(0), StructKind.FULLY_QUALIFIED);
      resolved.found(ns, false, null, path2, null);
      return;
    }
    super.resolveTable(names, nameMatcher, path, resolved);
  }

  @Override public void resolve(List<String> names, SqlNameMatcher nameMatcher,
      boolean deep, Resolved resolved) {
    if (names.size() == 1 && parameterTypes.containsKey(names.get(0))) {
      final SqlValidatorNamespace ns = validator.getNamespace(lambda);
      final Step path = Path.EMPTY.plus(ns.getRowType(), 0, names.get(0),
          StructKind.FULLY_QUALIFIED);
      resolved.found(ns, false, null, path, null);
      return;
    }
    super.resolve(names, nameMatcher, deep, resolved);
  }
}
