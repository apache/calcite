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

package org.apache.calcite.plan;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl.JavaType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;

/**
 * DataContext for evaluating an RexExpression
 */
public class VisitorDataContext implements DataContext {
  private final Object[] values;

  public VisitorDataContext(Object[] values) {
    this.values = values;
  }

  public SchemaPlus getRootSchema() {
    throw new RuntimeException("Unsupported");
  }

  public JavaTypeFactory getTypeFactory() {
    throw new RuntimeException("Unsupported");
  }

  public QueryProvider getQueryProvider() {
    throw new RuntimeException("Unsupported");
  }

  public Object get(String name) {
    if (name.equals("inputRecord")) {
      return values;
    } else {
      return null;
    }
  }
  public static DataContext getDataContext(RelNode targetRel, LogicalFilter queryRel) {
    return getDataContext(targetRel.getRowType(), queryRel.getCondition());
  }

  public static DataContext getDataContext(RelDataType rowType, RexNode rex) {
    int size = rowType.getFieldList().size();
    Object[] values = new Object[size];
    List<RexNode> operands = ((RexCall) rex).getOperands();
    final RexNode firstOperand = operands.get(0);
    final RexNode secondOperand = operands.get(1);
    final Pair<Integer, ? extends Object> value = getValue(firstOperand, secondOperand);
    if (value != null) {
      int index = value.getKey();
      values[index] = value.getValue();
      return new VisitorDataContext(values);
    } else {
      return null;
    }
  }

  public static DataContext getDataContext(RelDataType rowType, List<Pair<RexInputRef,
          RexNode>> usgList) {
    int size = rowType.getFieldList().size();
    Object[] values = new Object[size];
    for (Pair<RexInputRef, RexNode> elem: usgList) {
      Pair<Integer, ? extends Object> value = getValue(elem.getKey(), elem.getValue());
      if (value == null) {
        return null;
      }
      int index = value.getKey();
      values[index] = value.getValue();
    }
    return new VisitorDataContext(values);
  }

  public static Pair<Integer, ? extends Object> getValue(RexNode inputRef, RexNode literal) {
    inputRef = removeCast(inputRef);
    literal = removeCast(literal);

    if (inputRef instanceof RexInputRef
            && literal instanceof RexLiteral)  {
      Integer index = ((RexInputRef) inputRef).getIndex();
      Object value = ((RexLiteral) literal).getValue();
      final Class javaClass = ((JavaType) (inputRef.getType())).getJavaClass();
      if (javaClass == Integer.class
              && value instanceof BigDecimal) {
        final Integer intValue = new Integer(((BigDecimal) value).intValue());
        return  Pair.of(index, intValue);
      }
      if (javaClass == Date.class
              && value instanceof NlsString) {
        value = ((RexLiteral) literal).getValue2();
        final Date dateValue = Date.valueOf((String) value);
        return Pair.of(index, dateValue);
      }

      if (javaClass == Double.class
              && value instanceof BigDecimal) {
        return Pair.of(index,
                new Double(((BigDecimal) value).doubleValue()));
      }
      return Pair.of(index, value);
    }

    return null;
  }

  private static RexNode removeCast(RexNode inputRef) {
    if (inputRef instanceof RexCall) {
      final RexCall castedRef = (RexCall) inputRef;
      final SqlOperator operator = castedRef.getOperator();
      if (operator instanceof SqlCastFunction) {
        inputRef = castedRef.getOperands().get(0);
      }
    }
    return inputRef;
  }
}
