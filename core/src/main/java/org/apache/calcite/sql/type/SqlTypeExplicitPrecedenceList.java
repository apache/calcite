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
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.util.Glossary;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * SqlTypeExplicitPrecedenceList implements the
 * {@link RelDataTypePrecedenceList} interface via an explicit list of
 * {@link SqlTypeName} entries.
 */
public class SqlTypeExplicitPrecedenceList
    implements RelDataTypePrecedenceList {
  //~ Static fields/initializers ---------------------------------------------

  // NOTE jvs 25-Jan-2005:  the null entries delimit equivalence
  // classes
  private static final List<SqlTypeName> NUMERIC_TYPES =
      ImmutableNullableList.of(
          SqlTypeName.TINYINT,
          null,
          SqlTypeName.SMALLINT,
          null,
          SqlTypeName.INTEGER,
          null,
          SqlTypeName.BIGINT,
          null,
          SqlTypeName.DECIMAL,
          null,
          SqlTypeName.REAL,
          null,
          SqlTypeName.FLOAT,
          SqlTypeName.DOUBLE);

  private static final List<SqlTypeName> COMPACT_NUMERIC_TYPES =
      ImmutableList.copyOf(Util.filter(NUMERIC_TYPES, Objects::nonNull));

  /**
   * Map from SqlTypeName to corresponding precedence list.
   *
   * @see Glossary#SQL2003 SQL:2003 Part 2 Section 9.5
   */
  private static final Map<SqlTypeName, SqlTypeExplicitPrecedenceList>
      TYPE_NAME_TO_PRECEDENCE_LIST =
      ImmutableMap.<SqlTypeName, SqlTypeExplicitPrecedenceList>builder()
          .put(SqlTypeName.BOOLEAN, list(SqlTypeName.BOOLEAN))
          .put(SqlTypeName.TINYINT, numeric(SqlTypeName.TINYINT))
          .put(SqlTypeName.SMALLINT, numeric(SqlTypeName.SMALLINT))
          .put(SqlTypeName.INTEGER, numeric(SqlTypeName.INTEGER))
          .put(SqlTypeName.BIGINT, numeric(SqlTypeName.BIGINT))
          .put(SqlTypeName.DECIMAL, numeric(SqlTypeName.DECIMAL))
          .put(SqlTypeName.REAL, numeric(SqlTypeName.REAL))
          .put(SqlTypeName.FLOAT, list(SqlTypeName.FLOAT, SqlTypeName.REAL, SqlTypeName.DOUBLE))
          .put(SqlTypeName.DOUBLE, list(SqlTypeName.DOUBLE, SqlTypeName.DECIMAL))
          .put(SqlTypeName.CHAR, list(SqlTypeName.CHAR, SqlTypeName.VARCHAR))
          .put(SqlTypeName.VARCHAR, list(SqlTypeName.VARCHAR))
          .put(SqlTypeName.BINARY,
              list(SqlTypeName.BINARY, SqlTypeName.VARBINARY))
          .put(SqlTypeName.VARBINARY, list(SqlTypeName.VARBINARY))
          .put(SqlTypeName.DATE, list(SqlTypeName.DATE))
          .put(SqlTypeName.TIME, list(SqlTypeName.TIME))
          .put(SqlTypeName.TIMESTAMP,
              list(SqlTypeName.TIMESTAMP, SqlTypeName.DATE, SqlTypeName.TIME))
          .put(SqlTypeName.INTERVAL_YEAR,
              list(SqlTypeName.YEAR_INTERVAL_TYPES))
          .put(SqlTypeName.INTERVAL_YEAR_MONTH,
              list(SqlTypeName.YEAR_INTERVAL_TYPES))
          .put(SqlTypeName.INTERVAL_MONTH,
              list(SqlTypeName.YEAR_INTERVAL_TYPES))
          .put(SqlTypeName.INTERVAL_DAY,
              list(SqlTypeName.DAY_INTERVAL_TYPES))
          .put(SqlTypeName.INTERVAL_DAY_HOUR,
              list(SqlTypeName.DAY_INTERVAL_TYPES))
          .put(SqlTypeName.INTERVAL_DAY_MINUTE,
              list(SqlTypeName.DAY_INTERVAL_TYPES))
          .put(SqlTypeName.INTERVAL_DAY_SECOND,
              list(SqlTypeName.DAY_INTERVAL_TYPES))
          .put(SqlTypeName.INTERVAL_HOUR,
              list(SqlTypeName.DAY_INTERVAL_TYPES))
          .put(SqlTypeName.INTERVAL_HOUR_MINUTE,
              list(SqlTypeName.DAY_INTERVAL_TYPES))
          .put(SqlTypeName.INTERVAL_HOUR_SECOND,
              list(SqlTypeName.DAY_INTERVAL_TYPES))
          .put(SqlTypeName.INTERVAL_MINUTE,
              list(SqlTypeName.DAY_INTERVAL_TYPES))
          .put(SqlTypeName.INTERVAL_MINUTE_SECOND,
              list(SqlTypeName.DAY_INTERVAL_TYPES))
          .put(SqlTypeName.INTERVAL_SECOND,
              list(SqlTypeName.DAY_INTERVAL_TYPES))
          .build();

  //~ Instance fields --------------------------------------------------------

  private final List<SqlTypeName> typeNames;

  //~ Constructors -----------------------------------------------------------

  public SqlTypeExplicitPrecedenceList(Iterable<SqlTypeName> typeNames) {
    this.typeNames = ImmutableNullableList.copyOf(typeNames);
  }

  //~ Methods ----------------------------------------------------------------

  private static SqlTypeExplicitPrecedenceList list(SqlTypeName... typeNames) {
    return list(Arrays.asList(typeNames));
  }

  private static SqlTypeExplicitPrecedenceList list(Iterable<SqlTypeName> typeNames) {
    return new SqlTypeExplicitPrecedenceList(typeNames);
  }

  private static SqlTypeExplicitPrecedenceList numeric(SqlTypeName typeName) {
    int i = getListPosition(typeName, COMPACT_NUMERIC_TYPES);
    return new SqlTypeExplicitPrecedenceList(
        Util.skip(COMPACT_NUMERIC_TYPES, i));
  }

  // implement RelDataTypePrecedenceList
  public boolean containsType(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    return typeName != null && typeNames.contains(typeName);
  }

  // implement RelDataTypePrecedenceList
  public int compareTypePrecedence(RelDataType type1, RelDataType type2) {
    assert containsType(type1) : type1;
    assert containsType(type2) : type2;

    int p1 =
        getListPosition(
            type1.getSqlTypeName(),
            typeNames);
    int p2 =
        getListPosition(
            type2.getSqlTypeName(),
            typeNames);
    return p2 - p1;
  }

  private static int getListPosition(SqlTypeName type, List<SqlTypeName> list) {
    int i = list.indexOf(type);
    assert i != -1;

    // adjust for precedence equivalence classes
    for (int j = i - 1; j >= 0; --j) {
      if (list.get(j) == null) {
        return j;
      }
    }
    return i;
  }

  static RelDataTypePrecedenceList getListForType(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return null;
    }
    return TYPE_NAME_TO_PRECEDENCE_LIST.get(typeName);
  }
}

// End SqlTypeExplicitPrecedenceList.java
