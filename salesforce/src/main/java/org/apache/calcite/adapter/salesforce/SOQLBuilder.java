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
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Builds SOQL queries from Rex expressions.
 */
public class SOQLBuilder {
  
  private SOQLBuilder() {}
  
  /**
   * Convert a filter condition to a SOQL WHERE clause.
   */
  public static String buildWhereClause(RexNode condition) {
    return condition.accept(new SOQLFilterTranslator());
  }
  
  /**
   * Visitor that translates Rex expressions to SOQL.
   */
  private static class SOQLFilterTranslator extends RexVisitorImpl<String> {
    
    protected SOQLFilterTranslator() {
      super(true);
    }
    
    @Override
    public String visitCall(RexCall call) {
      SqlKind kind = call.getKind();
      List<String> operands = new ArrayList<>();
      
      for (RexNode operand : call.getOperands()) {
        operands.add(operand.accept(this));
      }
      
      switch (kind) {
        case EQUALS:
          return operands.get(0) + " = " + operands.get(1);
          
        case NOT_EQUALS:
          return operands.get(0) + " != " + operands.get(1);
          
        case GREATER_THAN:
          return operands.get(0) + " > " + operands.get(1);
          
        case GREATER_THAN_OR_EQUAL:
          return operands.get(0) + " >= " + operands.get(1);
          
        case LESS_THAN:
          return operands.get(0) + " < " + operands.get(1);
          
        case LESS_THAN_OR_EQUAL:
          return operands.get(0) + " <= " + operands.get(1);
          
        case AND:
          return "(" + String.join(" AND ", operands) + ")";
          
        case OR:
          return "(" + String.join(" OR ", operands) + ")";
          
        case NOT:
          return "NOT (" + operands.get(0) + ")";
          
        case IS_NULL:
          return operands.get(0) + " = null";
          
        case IS_NOT_NULL:
          return operands.get(0) + " != null";
          
        case LIKE:
          // SOQL uses LIKE with % wildcards
          return operands.get(0) + " LIKE " + operands.get(1);
          
        case IN:
          // First operand is the field, rest are values
          String field = operands.get(0);
          operands.remove(0);
          return field + " IN (" + String.join(", ", operands) + ")";
          
        default:
          throw new UnsupportedOperationException(
              "Operator not supported in SOQL: " + kind);
      }
    }
    
    @Override
    public String visitInputRef(RexInputRef inputRef) {
      // This should be resolved to actual field name by the caller
      throw new UnsupportedOperationException(
          "Field references must be resolved before SOQL translation");
    }
    
    @Override
    public String visitLiteral(RexLiteral literal) {
      if (literal.isNull()) {
        return "null";
      }
      
      SqlTypeName typeName = literal.getType().getSqlTypeName();
      Object value = literal.getValue();
      
      switch (typeName) {
        case VARCHAR:
        case CHAR:
          // Escape single quotes in strings
          String str = value.toString().replace("'", "\\'");
          return "'" + str + "'";
          
        case BOOLEAN:
          return value.toString();
          
        case INTEGER:
        case BIGINT:
        case SMALLINT:
        case TINYINT:
        case DECIMAL:
        case DOUBLE:
        case FLOAT:
          return value.toString();
          
        case DATE:
          // Format: YYYY-MM-DD
          Calendar cal = (Calendar) value;
          SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
          dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
          return dateFormat.format(cal.getTime());
          
        case TIMESTAMP:
          // Format: YYYY-MM-DDTHH:MM:SS.sssZ
          Calendar tsCal = (Calendar) value;
          SimpleDateFormat tsFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
          tsFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
          return tsFormat.format(tsCal.getTime());
          
        default:
          throw new UnsupportedOperationException(
              "Literal type not supported in SOQL: " + typeName);
      }
    }
  }
}