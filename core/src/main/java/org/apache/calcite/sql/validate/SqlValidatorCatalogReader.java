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

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.List;

/**
 * Supplies catalog information for {@link SqlValidator}.
 *
 * <p>This interface only provides a thin API to the underlying repository, and
 * this is intentional. By only presenting the repository information of
 * interest to the validator, we reduce the dependency on exact mechanism to
 * implement the repository. It is also possible to construct mock
 * implementations of this interface for testing purposes.
 */
public interface SqlValidatorCatalogReader extends Wrapper {
  //~ Methods ----------------------------------------------------------------

  /**
   * Finds a table or schema with the given name, possibly qualified.
   *
   * <p>Uses the case-sensitivity policy of the catalog reader.
   *
   * <p>If not found, returns null. If you want a more descriptive error
   * message or to override the case-sensitivity of the match, use
   * {@link SqlValidatorScope#resolveTable}.
   *
   * @param names Name of table, may be qualified or fully-qualified
   *
   * @return Table with the given name, or null
   */
  SqlValidatorTable getTable(List<String> names);

  /**
   * Finds a user-defined type with the given name, possibly qualified.
   *
   * <p>NOTE jvs 12-Feb-2005: the reason this method is defined here instead
   * of on RelDataTypeFactory is that it has to take into account
   * context-dependent information such as SQL schema path, whereas a type
   * factory is context-independent.
   *
   * @param typeName Name of type
   * @return named type, or null if not found
   */
  RelDataType getNamedType(SqlIdentifier typeName);

  /**
   * Given fully qualified schema name, returns schema object names as
   * specified. They can be schema, table, function, view.
   * When names array is empty, the contents of root schema should be returned.
   *
   * @param names the array contains fully qualified schema name or empty
   *              list for root schema
   * @return the list of all object (schema, table, function,
   *         view) names under the above criteria
   */
  List<SqlMoniker> getAllSchemaObjectNames(List<String> names);

  /**
   * Returns the paths of all schemas to look in for tables.
   *
   * @return paths of current schema and root schema
   */
  List<List<String>> getSchemaPaths();

  /** @deprecated Use
   * {@link #nameMatcher()}.{@link SqlNameMatcher#field(RelDataType, String)} */
  @Deprecated // to be removed before 2.0
  RelDataTypeField field(RelDataType rowType, String alias);

  /** Returns an implementation of
   * {@link org.apache.calcite.sql.validate.SqlNameMatcher}
   * that matches the case-sensitivity policy. */
  SqlNameMatcher nameMatcher();

  /** @deprecated Use
   * {@link #nameMatcher()}.{@link SqlNameMatcher#matches(String, String)} */
  @Deprecated // to be removed before 2.0
  boolean matches(String string, String name);

  RelDataType createTypeFromProjection(RelDataType type,
      List<String> columnNameList);

  /** @deprecated Use
   * {@link #nameMatcher()}.{@link SqlNameMatcher#isCaseSensitive()} */
  @Deprecated // to be removed before 2.0
  boolean isCaseSensitive();

  /** Returns the root namespace for name resolution. */
  CalciteSchema getRootSchema();

  /** Returns Config settings */
  CalciteConnectionConfig getConfig();
}

// End SqlValidatorCatalogReader.java
