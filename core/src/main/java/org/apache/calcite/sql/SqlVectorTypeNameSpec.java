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
import org.apache.calcite.sql.type.VectorSqlType;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A SQL type name specification of the {@code VECTOR} type, for example
 * {@code VECTOR(3)} or {@code VECTOR(768, TINYINT)}.
 *
 * <p>The syntax is
 *
 * <blockquote><pre>VECTOR(dimension [, elementType ])</pre></blockquote>
 *
 * <p>where {@code dimension} is a positive integer and {@code elementType}
 * is {@code REAL} (the default) or {@code TINYINT}.
 *
 * <p>The {@code VECTOR} type does not belong to standard SQL.
 *
 * @see VectorSqlType
 */
public class SqlVectorTypeNameSpec extends SqlTypeNameSpec {
  private final int dimension;
  private final @Nullable SqlTypeNameSpec elementTypeName;

  /**
   * Creates a {@code SqlVectorTypeNameSpec}.
   *
   * @param dimension       Number of elements
   * @param elementTypeName Element type, or null if not specified (in which
   *                        case the element type is {@code REAL})
   * @param pos             Parser position, must not be null
   */
  public SqlVectorTypeNameSpec(int dimension,
      @Nullable SqlTypeNameSpec elementTypeName, SqlParserPos pos) {
    super(new SqlIdentifier(SqlTypeName.VECTOR.name(), pos), pos);
    this.dimension = dimension;
    this.elementTypeName = elementTypeName;
  }

  /** Returns the number of elements. */
  public int getDimension() {
    return dimension;
  }

  /** Returns the element type, or null if not specified. */
  public @Nullable SqlTypeNameSpec getElementTypeName() {
    return elementTypeName;
  }

  @Override public RelDataType deriveType(SqlValidator validator) {
    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
    if (dimension < 1) {
      throw SqlUtil.newContextException(getParserPos(),
          RESOURCE.invalidVectorDimension(dimension));
    }
    final RelDataType elementType;
    if (elementTypeName == null) {
      elementType =
          typeFactory.createSqlType(VectorSqlType.DEFAULT_ELEMENT_TYPE_NAME);
    } else {
      elementType = elementTypeName.deriveType(validator);
      if (!VectorSqlType.isValidElementType(elementType.getSqlTypeName())) {
        throw SqlUtil.newContextException(elementTypeName.getParserPos(),
            RESOURCE.invalidVectorElementType(elementType.toString()));
      }
    }
    return typeFactory.createVectorType(elementType, dimension);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec,
      int rightPrec) {
    writer.keyword(SqlTypeName.VECTOR.name());
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
    writer.print(dimension);
    if (elementTypeName != null) {
      writer.sep(",", true);
      elementTypeName.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
  }

  @Override public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
    if (!(spec instanceof SqlVectorTypeNameSpec)) {
      return litmus.fail("{} != {}", this, spec);
    }
    final SqlVectorTypeNameSpec that = (SqlVectorTypeNameSpec) spec;
    if (this.dimension != that.dimension) {
      return litmus.fail("{} != {}", this, spec);
    }
    if (this.elementTypeName == null || that.elementTypeName == null) {
      if (this.elementTypeName != that.elementTypeName) {
        return litmus.fail("{} != {}", this, spec);
      }
    } else if (!this.elementTypeName.equalsDeep(that.elementTypeName,
        litmus)) {
      return litmus.fail("{} != {}", this, spec);
    }
    return litmus.succeed();
  }
}
