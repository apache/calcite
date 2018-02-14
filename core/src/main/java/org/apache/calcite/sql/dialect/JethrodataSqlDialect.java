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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactoryImpl.JethroSupportedFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * A <code>SqlDialect</code> implementation for the Jethrodata database.
 */
public class JethrodataSqlDialect extends SqlDialect {
  private static final Logger LOG = LoggerFactory.getLogger(JethrodataSqlDialect.class);

  private final ImmutableMap<String, HashSet<JethroSupportedFunction>> supportedFunctions;

  /** Creates an JethrodataSqlDialect. */
  public JethrodataSqlDialect(
                        Context context,
                        ImmutableMap<String, HashSet<JethroSupportedFunction>> supportedFunctions)
                                throws SQLException {
      super(context);
    this.supportedFunctions = supportedFunctions;
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst, boolean desc) {
    return node;
  }

  @Override public boolean supportsAggregateFunction(SqlKind kind) {
    switch (kind) {
    case COUNT:
    case SUM:
    case AVG:
    case MIN:
    case MAX:
    case STDDEV_POP:
    case STDDEV_SAMP:
    case VAR_POP:
    case VAR_SAMP:
      return true;
    default:
      break;
    }
    return false;
  }

  @Override public boolean supportsFunction(SqlOperator operator, RelDataType type,
      List<RelDataType> paramsList) {
    if (operator.getKind() == SqlKind.IS_NOT_NULL
        || operator.getKind() == SqlKind.IS_NULL
        || operator.getKind() == SqlKind.AND
        || operator.getKind() == SqlKind.OR
        || operator.getKind() == SqlKind.NOT
        || operator.getKind() == SqlKind.BETWEEN
        || operator.getKind() == SqlKind.CASE
        || operator.getKind() == SqlKind.CAST) {
      return true;
    }
    if (supportedFunctions != null) {
      Set<JethroSupportedFunction> currMethodSignatures =
          supportedFunctions.get(operator.getName());

      if (currMethodSignatures != null) {
        for (JethroSupportedFunction curr : currMethodSignatures) {
          final SqlTypeName[] operandType = curr.getOperandsType();
          if (paramsList.size() == operandType.length) {
            for (int i = 0; i < paramsList.size(); i++) {
              if (paramsList.get(i).getSqlTypeName() != operandType[i]) {
                continue;
              }
            }
            return true;
          }
        }
      }
    }
    LOG.debug("Unsupported function in jethro: " + operator + " with params " + paramsList);
    return false;
  }

  @Override public boolean supportsOffsetFetch() {
    return false;
  }

  @Override public boolean supportsNestedAggregations() {
    return false;
  }
}

// End JethrodataSqlDialect.java
