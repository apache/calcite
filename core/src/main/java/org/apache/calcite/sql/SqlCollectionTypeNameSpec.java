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
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import java.util.Objects;

/**
 * A sql type name specification of collection type.
 *
 * <p>The grammar definition in SQL-2011 IWD 9075-2:201?(E)
 * 6.1 &lt;collection type&gt; is as following:
 * <blockquote><pre>
 * &lt;collection type&gt; ::=
 *     &lt;array type&gt;
 *   | &lt;multiset type&gt;
 *
 * &lt;array type&gt; ::=
 *   &lt;data type&gt; ARRAY
 *   [ &lt;left bracket or trigraph&gt;
 *     &lt;maximum cardinality&gt;
 *     &lt;right bracket or trigraph&gt; ]
 *
 * &lt;maximum cardinality&gt; ::=
 *   &lt;unsigned integer&gt;
 *
 * &lt;multiset type&gt; ::=
 *   &lt;data type&gt; MULTISET
 * </pre></blockquote>
 *
 * <p>This class is intended to be used in nested collection type, it can be used as the
 * element type name of {@link SqlDataTypeSpec}. i.e. "int array array" or "int array multiset".
 * For simple collection type like "int array", {@link SqlBasicTypeNameSpec} is descriptive enough.
 */
public class SqlCollectionTypeNameSpec extends SqlTypeNameSpec {
  private final SqlTypeNameSpec elementTypeName;
  private final SqlTypeName collectionTypeName;

  /**
   * Creates a {@code SqlCollectionTypeNameSpec}.
   *
   * @param elementTypeName    Type of the collection element.
   * @param collectionTypeName Collection type name.
   * @param pos                Parser position, must not be null.
   */
  public SqlCollectionTypeNameSpec(SqlTypeNameSpec elementTypeName,
      SqlTypeName collectionTypeName,
      SqlParserPos pos) {
    super(new SqlIdentifier(collectionTypeName.name(), pos), pos);
    this.elementTypeName = Objects.requireNonNull(elementTypeName);
    this.collectionTypeName = Objects.requireNonNull(collectionTypeName);
  }

  public SqlTypeNameSpec getElementTypeName() {
    return elementTypeName;
  }

  @Override public RelDataType deriveType(SqlValidator validator) {
    final RelDataType type = elementTypeName.deriveType(validator);
    return createCollectionType(type, validator.getTypeFactory());
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    elementTypeName.unparse(writer, leftPrec, rightPrec);
    writer.keyword(collectionTypeName.name());
  }

  @Override public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
    if (!(spec instanceof SqlCollectionTypeNameSpec)) {
      return litmus.fail("{} != {}", this, spec);
    }
    SqlCollectionTypeNameSpec that = (SqlCollectionTypeNameSpec) spec;
    if (!this.elementTypeName.equalsDeep(that.elementTypeName, litmus)) {
      return litmus.fail("{} != {}", this, spec);
    }
    if (!Objects.equals(this.collectionTypeName, that.collectionTypeName)) {
      return litmus.fail("{} != {}", this, spec);
    }
    return litmus.succeed();
  }

  //~ Tools ------------------------------------------------------------------

  /**
   * Create collection data type.
   * @param elementType Type of the collection element.
   * @param typeFactory Type factory.
   * @return The collection data type, or throw exception if the collection
   *         type name does not belong to {@code SqlTypeName} enumerations.
   */
  private RelDataType createCollectionType(RelDataType elementType,
      RelDataTypeFactory typeFactory) {
    switch (collectionTypeName) {
    case MULTISET:
      return typeFactory.createMultisetType(elementType, -1);
    case ARRAY:
      return typeFactory.createArrayType(elementType, -1);

    default:
      throw Util.unexpected(collectionTypeName);
    }
  }
}

// End SqlCollectionTypeNameSpec.java
