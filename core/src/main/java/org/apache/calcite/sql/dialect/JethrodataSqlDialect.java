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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * A <code>SqlDialect</code> implementation for the Jethrodata database.
 */
public class JethrodataSqlDialect extends SqlDialect {
  private static final Logger LOG = LoggerFactory.getLogger(JethrodataSqlDialect.class);

  private final ImmutableMap<String, HashSet<SupportedFunction>> supportedFunctions;

  /** Creates an JethrodataSqlDialect. */
  public JethrodataSqlDialect(Context context,
                              Connection connection) throws SQLException {
      super(context);
    this.supportedFunctions = getSupportedFunctions(connection);
  }

  @VisibleForTesting
  public JethrodataSqlDialect(Context context) throws SQLException {
      super(context);
    this.supportedFunctions = null;
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

  @Override public boolean supportsFunction(RexCall call) {

    if (call instanceof RexOver) {
      return false;
    }

    final SqlOperator operator = call.getOperator();

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

    ArrayList<RelDataType> paramsListType = new ArrayList<RelDataType>();
    for (RexNode currNode : call.getOperands()) {
      paramsListType.add(currNode.getType());
    }

    if (supportedFunctions != null) {
      Set<SupportedFunction> currMethodSignatures = supportedFunctions.get(operator.getName());

      if (currMethodSignatures != null) {
        for (SupportedFunction curr : currMethodSignatures) {
          if (paramsListType.size() == curr.operandsType.length) {
            for (int i = 0; i < paramsListType.size(); i++) {
              if (paramsListType.get(i).getSqlTypeName() != curr.operandsType[i]) {
                continue;
              }
            }
            return true;
          }
        }
      }
    }
    LOG.debug("Unsupported function in jethro: " + operator + " with params " + paramsListType);
    return false;
  }

  @Override public boolean supportsOffsetFetch() {
    return false;
  }

  @Override public boolean supportsNestedAggregations() {
    return false;
  }

  /**
   * A class to hold one jethro supported function info
   */
  static class SupportedFunction {
    final String funcName;
    final SqlTypeName [] operandsType;


    /**
     * @param funcName The sql function name
     * @param operands The sql function parameters type
     */
    SupportedFunction(String funcName, String operands) {
      super();

      this.funcName = funcName;
      String[] operandsStrType = operands.split(":");
      this.operandsType = new SqlTypeName [operandsStrType.length];
      for (int i = 0; i < this.operandsType.length; ++i) {
        SqlTypeName curr_t = SqlTypeName.ANY;
        switch (operandsStrType [i]) {
        case "kInteger64":
          curr_t = SqlTypeName.BIGINT;
          break;
        case "kInteger32":
          curr_t = SqlTypeName.INTEGER;
          break;
        case "kDouble":
          curr_t = SqlTypeName.DOUBLE;
          break;
        case "kFloat":
          curr_t = SqlTypeName.FLOAT;
          break;
        case "kString":
          curr_t = SqlTypeName.CHAR;
          break;
        case "kTimeStamp":
          curr_t = SqlTypeName.TIMESTAMP;
          break;

        default:
          break;
        }
        operandsType [i] = curr_t;
      }
    }
  };

  private static final Map<String, ImmutableMap<String, HashSet<SupportedFunction>>>
            SUPPORTED_JETHRO_FUNCTIONS = new HashMap<>();

  /**
   * @param jethroConnection The JethroData jdbc data source
   * @return
   * @throws SQLException
   */
  private static synchronized
                    ImmutableMap<String, HashSet<SupportedFunction>> getSupportedFunctions(
                                         Connection jethroConnection) throws SQLException {
    if (jethroConnection == null) {
      throw new SQLException("JethrodataSqlDialect reuqies a connection");
    }

    DatabaseMetaData metaData = jethroConnection.getMetaData();
    assert "JethroData".equals(metaData.getDatabaseProductName());
    String productVersion = metaData.getDatabaseProductVersion();
    ImmutableMap<String, HashSet<SupportedFunction>> res =
            SUPPORTED_JETHRO_FUNCTIONS.get(productVersion);

    if (res == null) {
      Statement jethroStatement = null;
      ResultSet functionsTupleSet = null;
      try {
        jethroStatement = jethroConnection.createStatement();
        functionsTupleSet = jethroStatement.executeQuery("show functions extended");

        Map<String, HashSet<SupportedFunction>> supportedFunctions = new HashMap<>();
        while (functionsTupleSet.next()) {
          String functionName = functionsTupleSet.getString(1);
          String operandsType = functionsTupleSet.getString(3);
          HashSet<SupportedFunction> funcSignatures = supportedFunctions.get(functionName);
          if (funcSignatures == null) {
            funcSignatures = new HashSet<>();
            supportedFunctions.put(functionName, funcSignatures);
          }
          funcSignatures.add(new SupportedFunction(functionName, operandsType));
        }
        res = ImmutableMap.copyOf(supportedFunctions);
        SUPPORTED_JETHRO_FUNCTIONS.put(productVersion, res);
      } catch (Exception e) {
        LOG.error("Jethro server failed to execute 'show functions extended'", e);
        throw new SQLException("Jethro server failed to execute 'show functions extended',"
                               + " make sure your Jethro server is up to date");
      } finally {
        try {
          if (jethroStatement != null) {
            jethroStatement.close();
          }

          if (functionsTupleSet != null) {
            functionsTupleSet.close();
          }
        } catch (Exception e) {
          LOGGER.error("Failed to close jethro connection resources", e);
        }
      }
    }
    return res;
  }

}

// End JethrodataSqlDialect.java
