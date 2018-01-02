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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import javax.sql.DataSource;


/**
 * A <code>SqlDialect</code> implementation for the Jethrodata database.
 */
public class JethrodataSqlDialect extends SqlDialect {
  private static final Logger LOG = LoggerFactory.getLogger(JethrodataSqlDialect.class);

//  public static final SqlDialect DEFAULT = new JethrodataSqlDialect(
//            EMPTY_CONTEXT.withDatabaseProduct(DatabaseProduct.JETHRO).
//                          withIdentifierQuoteString("\"").withDatabaseVersion("Default"));

  private final String version;

  /** Creates an InterbaseSqlDialect. */
  public JethrodataSqlDialect(Context context) {
      super(context);
    if (context.databaseVersion() != null) {
      version = context.databaseVersion();
    } else {
      version = "Default";
    }
  }

  @Override public void initFromDataSource(DataSource ds) throws SQLException {
    initializeSupportedFunctions(ds);
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst, boolean desc) {
    //return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
    return node;
  }

  @Override public boolean supportsAggregateFunction(SqlKind kind) {
      //TODOY check if STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP are supported
    switch (kind) {
    case COUNT:
    case SUM:
    case AVG:
    case MIN:
    case MAX:
      return true;
    }
    return false;
  }

  @Override public boolean supportsFunction(SqlOperator operator,
                                            RelDataType type, ArrayList<RelDataType> paramsList) {
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
    final HashMap<String, HashSet<SupportedFunction>> supportedFunctions =
        SUPPORTED_JETHRO_FUNCTIONS.get(version);
    if (supportedFunctions != null) {
      HashSet<SupportedFunction> currMethodSignatures = supportedFunctions.get(operator.getName());
      if (currMethodSignatures != null) {
        for (SupportedFunction curr : currMethodSignatures) {
          if (paramsList.size() == curr.operandsType.length) {
            for (int i = 0; i < paramsList.size(); i++) {
              if (paramsList.get(i).getSqlTypeName() != curr.operandsType[i]) {
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

  /**
   * A class to hold one jethro supported function info
   */
  static class SupportedFunction {
    /**
     * Jethro types
     */
    /*enum Type {
      Int, Long, Double, Float, String, TimeStamp, Unknown
    };*/
    final String funcName;
    final SqlTypeName [] operandsType;


    /**
     * @param funcName Function name
     * @param operands function opernads
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

  private static final HashMap<String, HashMap<String, HashSet<SupportedFunction>>>
            SUPPORTED_JETHRO_FUNCTIONS = new HashMap<>();

  /**
   * @param ds data source
   * @throws SQLException
   */
  public static synchronized void initializeSupportedFunctions(DataSource ds) throws SQLException {
    java.sql.Connection jethroConnection = null;
    try {
      jethroConnection = ds.getConnection();
      DatabaseMetaData metaData = jethroConnection.getMetaData();
      assert "JethroData".equals(metaData.getDatabaseProductName());
      String productVersion = "Default"; //metaData.getDatabaseProductVersion();
      if (SUPPORTED_JETHRO_FUNCTIONS.containsKey(productVersion)) {
        return;
      }
      final HashMap<String, HashSet<SupportedFunction>> supportedFunctions =
          new HashMap<String, HashSet<SupportedFunction>>();
      SUPPORTED_JETHRO_FUNCTIONS.put(productVersion, supportedFunctions);
      Statement jethroStatement = jethroConnection.createStatement();
      ResultSet functionsTupleSet = jethroStatement.executeQuery("show functions");
      while (functionsTupleSet.next()) {
        String functionName = functionsTupleSet.getString(1);
        String operandsType = functionsTupleSet.getString(3);
        HashSet<SupportedFunction> funcSignatures = supportedFunctions.get(functionName);
        if (funcSignatures == null) {
          funcSignatures = new HashSet<SupportedFunction>();
          supportedFunctions.put(functionName, funcSignatures);
        }
        funcSignatures.add(new SupportedFunction(functionName, operandsType));
      }
    } finally {
      jethroConnection.close();
    }
  }

}

// End JethrodataSqlDialect.java
