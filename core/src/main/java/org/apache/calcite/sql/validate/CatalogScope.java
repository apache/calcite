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

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.sql.SqlNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * Implementation of {@link SqlValidatorScope} that can see all schemas in the
 * current catalog.
 *
 * <p>Occurs near the root of the scope stack; its parent is typically
 * {@link EmptyScope}.
 *
 * <p>Helps resolve {@code schema.table.column} column references, such as
 *
 * <blockquote><pre>select sales.emp.empno from sales.emp</pre></blockquote>
 */
class CatalogScope extends DelegatingScope {
  /** Fully-qualified name of the catalog. Typically empty or ["CATALOG"]. */
  final ImmutableList<String> names;
  private final Set<List<String>> schemaNames;

  //~ Constructors -----------------------------------------------------------

  CatalogScope(SqlValidatorScope parent, List<String> names) {
    super(parent);
    this.names = ImmutableList.copyOf(names);
    this.schemaNames =
        Linq4j.asEnumerable(
            validator.getCatalogReader()
                .getAllSchemaObjectNames(ImmutableList.<String>of()))
            .where(
                new Predicate1<SqlMoniker>() {
                  public boolean apply(SqlMoniker input) {
                    return input.getType() == SqlMonikerType.SCHEMA;
                  }
                })
            .select(
                new Function1<SqlMoniker, List<String>>() {
                  public List<String> apply(SqlMoniker input) {
                    return input.getFullyQualifiedNames();
                  }
                })
            .into(Sets.<List<String>>newHashSet());
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode getNode() {
    throw new UnsupportedOperationException();
  }

}

// End CatalogScope.java
