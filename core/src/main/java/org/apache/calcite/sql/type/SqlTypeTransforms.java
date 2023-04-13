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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.util.Util;

import java.util.List;

import static org.apache.calcite.sql.type.NonNullableAccessors.getCharset;
import static org.apache.calcite.sql.type.NonNullableAccessors.getCollation;

import static java.util.Objects.requireNonNull;

/**
 * SqlTypeTransforms defines a number of reusable instances of
 * {@link SqlTypeTransform}.
 *
 * <p>NOTE: avoid anonymous inner classes here except for unique,
 * non-generalizable strategies; anything else belongs in a reusable top-level
 * class. If you find yourself copying and pasting an existing strategy's
 * anonymous inner class, you're making a mistake.
 */
public abstract class SqlTypeTransforms {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * Parameter type-inference transform strategy where a derived type is
   * transformed into the same type but nullable if any of a calls operands is
   * nullable.
   */
  public static final SqlTypeTransform TO_NULLABLE =
      (opBinding, typeToTransform) ->
          SqlTypeUtil.makeNullableIfOperandsAre(opBinding.getTypeFactory(),
              opBinding.collectOperandTypes(),
              requireNonNull(typeToTransform, "typeToTransform"));

  /**
   * Parameter type-inference transform strategy where a derived type is
   * transformed into the same type, but nullable if and only if all of a call's
   * operands are nullable.
   */
  public static final SqlTypeTransform TO_NULLABLE_ALL = (opBinding, type) -> {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    return typeFactory.createTypeWithNullability(type,
        SqlTypeUtil.allNullable(opBinding.collectOperandTypes()));
  };

  /**
   * Parameter type-inference transform strategy where a derived type is
   * transformed into the same type but not nullable.
   */
  public static final SqlTypeTransform TO_NOT_NULLABLE =
      (opBinding, typeToTransform) ->
          opBinding.getTypeFactory().createTypeWithNullability(
              requireNonNull(typeToTransform, "typeToTransform"), false);

  /**
   * Parameter type-inference transform strategy where a derived type is
   * transformed into the same type with nulls allowed.
   */
  public static final SqlTypeTransform FORCE_NULLABLE =
      (opBinding, typeToTransform) ->
          opBinding.getTypeFactory().createTypeWithNullability(
              requireNonNull(typeToTransform, "typeToTransform"), true);

  /**
   * Type-inference strategy whereby the result is NOT NULL if any of
   * the arguments is NOT NULL; otherwise the type is unchanged.
   */
  public static final SqlTypeTransform LEAST_NULLABLE =
      (opBinding, typeToTransform) -> {
        for (RelDataType type : opBinding.collectOperandTypes()) {
          if (!type.isNullable()) {
            return opBinding.getTypeFactory()
                .createTypeWithNullability(typeToTransform, false);
          }
        }
        return typeToTransform;
      };

  /**
   * Type-inference strategy whereby the result type of a call is VARYING the
   * type given. The length returned is the same as length of the first
   * argument. Return type will have same nullability as input type
   * nullability. First Arg must be of string type.
   */
  public static final SqlTypeTransform TO_VARYING =
      new SqlTypeTransform() {
        @Override public RelDataType transformType(
            SqlOperatorBinding opBinding,
            RelDataType typeToTransform) {
          switch (typeToTransform.getSqlTypeName()) {
          case VARCHAR:
          case VARBINARY:
            return typeToTransform;
          default:
            break;
          }

          SqlTypeName retTypeName = toVar(typeToTransform);

          RelDataType ret =
              opBinding.getTypeFactory().createSqlType(
                  retTypeName,
                  typeToTransform.getPrecision());
          if (SqlTypeUtil.inCharFamily(typeToTransform)) {
            ret =
                opBinding.getTypeFactory()
                    .createTypeWithCharsetAndCollation(
                        ret,
                        getCharset(typeToTransform),
                        getCollation(typeToTransform));
          }
          return opBinding.getTypeFactory().createTypeWithNullability(
              ret,
              typeToTransform.isNullable());
        }

        private SqlTypeName toVar(RelDataType type) {
          final SqlTypeName sqlTypeName = type.getSqlTypeName();
          switch (sqlTypeName) {
          case CHAR:
            return SqlTypeName.VARCHAR;
          case BINARY:
            return SqlTypeName.VARBINARY;
          case ANY:
            return SqlTypeName.ANY;
          case NULL:
            return SqlTypeName.NULL;
          case UNKNOWN:
            return SqlTypeName.UNKNOWN;
          default:
            throw Util.unexpected(sqlTypeName);
          }
        }
      };

  /**
   * Parameter type-inference transform strategy where a derived type must be
   * a multiset type and the returned type is the multiset's element type.
   *
   * @see MultisetSqlType#getComponentType
   */
  public static final SqlTypeTransform TO_MULTISET_ELEMENT_TYPE =
      (opBinding, typeToTransform) -> requireNonNull(
          typeToTransform.getComponentType(),
          () -> "componentType for " + typeToTransform + " in opBinding " + opBinding);

  /**
   * Parameter type-inference transform strategy that wraps a given type
   * in a multiset.
   *
   * @see org.apache.calcite.rel.type.RelDataTypeFactory#createMultisetType(RelDataType, long)
   */
  public static final SqlTypeTransform TO_MULTISET =
      (opBinding, typeToTransform) ->
          opBinding.getTypeFactory().createMultisetType(typeToTransform, -1);

  /**
   * Parameter type-inference transform strategy that wraps a given type in a multiset or
   * wraps a field of the given type in a multiset if the given type is struct with one field.
   * It is used when a multiset input is a sub-query.
   */
  public static final SqlTypeTransform TO_MULTISET_QUERY =
      (opBinding, typeToTransform) ->
          TO_MULTISET.transformType(opBinding,
              SqlTypeUtil.deriveCollectionQueryComponentType(SqlTypeName.MULTISET,
                  typeToTransform));

  /**
   * Parameter type-inference transform strategy that wraps a given type
   * in a array.
   *
   * @see org.apache.calcite.rel.type.RelDataTypeFactory#createArrayType(RelDataType, long)
   */
  public static final SqlTypeTransform TO_ARRAY =
      (opBinding, typeToTransform) ->
          opBinding.getTypeFactory().createArrayType(typeToTransform, -1);

  /** Parameter type-inference transform that transforms {@code T} to
   * {@code MEASURE<T>} for some type T. */
  public static final SqlTypeTransform TO_MEASURE =
      (opBinding, typeToTransform) ->
          opBinding.getTypeFactory().createMeasureType(typeToTransform);

  /** Parameter type-inference transform that transforms {@code MEASURE<T>} to
   * {@code T} for some type T. Inverse of {@link #TO_MEASURE}. */
  public static final SqlTypeTransform FROM_MEASURE =
      (opBinding, typeToTransform) ->
          ((MeasureSqlType) typeToTransform).types.get(0);

  /**
   * Parameter type-inference transform strategy that wraps a given type in an array or
   * wraps a field of the given type in an array if the given type is struct with one field.
   * It is used when an array input is a sub-query.
   */
  public static final SqlTypeTransform TO_ARRAY_QUERY =
      (opBinding, typeToTransform) ->
        TO_ARRAY.transformType(opBinding,
            SqlTypeUtil.deriveCollectionQueryComponentType(SqlTypeName.ARRAY, typeToTransform));

  /**
   * Parameter type-inference transform strategy that converts a two-field
   * record type to a MAP type.
   *
   * @see org.apache.calcite.rel.type.RelDataTypeFactory#createMapType
   */
  public static final SqlTypeTransform TO_MAP =
      (opBinding, typeToTransform) ->
          SqlTypeUtil.createMapTypeFromRecord(opBinding.getTypeFactory(),
              typeToTransform);

  /**
   * Parameter type-inference transform strategy where a derived type must be
   * a struct type with precisely one field and the returned type is the type
   * of that field.
   */
  public static final SqlTypeTransform ONLY_COLUMN =
      (opBinding, typeToTransform) -> {
        final List<RelDataTypeField> fields = typeToTransform.getFieldList();
        assert fields.size() == 1;
        return fields.get(0).getType();
      };

}
