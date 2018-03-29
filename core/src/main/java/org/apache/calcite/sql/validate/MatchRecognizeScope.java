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
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Scope for expressions in a {@code MATCH_RECOGNIZE} clause.
 *
 * <p>Defines variables and uses them as prefix of columns reference.
 */
public class MatchRecognizeScope extends ListScope {
  private static final String STAR = "*";

  //~ Instance fields ---------------------------------------------
  private final SqlMatchRecognize matchRecognize;
  private final Set<String> patternVars;

  /** Creates a MatchRecognizeScope. */
  public MatchRecognizeScope(SqlValidatorScope parent,
      SqlMatchRecognize matchRecognize) {
    super(parent);
    this.matchRecognize = matchRecognize;
    patternVars = validator.getCatalogReader().nameMatcher().createSet();
    patternVars.add(STAR);
  }

  @Override public SqlNode getNode() {
    return matchRecognize;
  }

  public SqlMatchRecognize getMatchRecognize() {
    return matchRecognize;
  }

  public Set<String> getPatternVars() {
    return patternVars;
  }

  public void addPatternVar(String str) {
    patternVars.add(str);
  }

  @Override public Map<String, ScopeChild>
  findQualifyingTableNames(String columnName, SqlNode ctx,
      SqlNameMatcher nameMatcher) {
    final Map<String, ScopeChild> map = new HashMap<>();
    for (ScopeChild child : children) {
      final RelDataType rowType = child.namespace.getRowType();
      if (nameMatcher.field(rowType, columnName) != null) {
        map.put(STAR, child);
      }
    }
    switch (map.size()) {
    case 0:
      return parent.findQualifyingTableNames(columnName, ctx, nameMatcher);
    default:
      return map;
    }
  }

  @Override public void resolve(List<String> names, SqlNameMatcher nameMatcher,
     boolean deep, Resolved resolved) {
    if (patternVars.contains(names.get(0))) {
      final Step path = new EmptyPath().plus(null, 0, null, StructKind.FULLY_QUALIFIED);
      final ScopeChild child = children.get(0);
      resolved.found(child.namespace, child.nullable, this, path, names);
      if (resolved.count() > 0) {
        return;
      }
    }
    super.resolve(names, nameMatcher, deep, resolved);
  }

}

// End MatchRecognizeScope.java
