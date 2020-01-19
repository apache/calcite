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
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlLambda;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * The name-resolution context for expression inside a LAMBDA clause. The objects visible are the
 * lambda paramters, and those inherited from the parent scope.
 *
 * <p>Consider "SELECT lambdaExp((x)->A.a+x) FROM A
 * resolved in the lambda scope for "Lambda Parameters and Table columns of A"</p>
 */
public class LambdaScope extends ListScope {

  //~ Instance fields --------------------------------------------------------

  private final SqlLambda lambda;
  private List<RelDataTypeField> parameters;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LambdaScope.
   */
  public LambdaScope(
      SqlLambda lambda,
      SqlValidatorScope parent) {
    super(parent);
    this.lambda = lambda;
    parameters = createParameters(lambda);
  }

  private List<RelDataTypeField> createParameters(SqlLambda lambda) {
    List<RelDataTypeField> parameters = new ArrayList<>();
    for (int i = 0; i < lambda.getParameters().size(); i++) {
      SqlIdentifier identifier = (SqlIdentifier) lambda.getParameters().get(i);
      parameters.add(
          new RelDataTypeFieldImpl(identifier.getSimple(), i,
          validator.typeFactory.createSqlType(SqlTypeName.ANY)));
    }
    return parameters;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlNode getNode() {
    return lambda;
  }

  public SqlQualified fullyQualify(SqlIdentifier identifier) {
    if (findParameter(identifier.getSimple()) != null) {
      return SqlQualified.create(this, 1, null, identifier);
    } else {
      return parent.fullyQualify(identifier);
    }
  }

  @Override public RelDataType resolveColumn(String name, SqlNode ctx) {
    RelDataTypeField relDataTypeField = findParameter(name);
    if (relDataTypeField != null) {
      return relDataTypeField.getType();
    } else {
      return parent.resolveColumn(name, ctx);
    }
  }

  private RelDataTypeField findParameter(String name) {
    return parameters.stream().filter(s -> s.getName()
        .equals(name)).findFirst().orElse(null);
  }

  public List<RelDataTypeField> getParameters() {
    return parameters;
  }

  @Override public SqlValidatorNamespace getTableNamespace(List<String> names) {
    if (names.size() == 1 && findParameter(names.get(0)) != null) {
      return validator.getNamespace(lambda);
    }
    return parent.getTableNamespace(names);
  }

  @Override public void resolveTable(List<String> names,
      SqlNameMatcher nameMatcher, Path path, Resolved resolved) {
    if (names.size() == 1 && findParameter(names.get(0)) != null) {
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
    if (names.size() == 1 && findParameter(names.get(0)) != null) {
      final SqlValidatorNamespace ns = validator.getNamespace(lambda);
      final Step path = Path.EMPTY.plus(ns.getRowType(), 0, names.get(0),
          StructKind.FULLY_QUALIFIED);
      resolved.found(ns, false, null, path, null);
      return;
    }
    super.resolve(names, nameMatcher, deep, resolved);
  }
}
