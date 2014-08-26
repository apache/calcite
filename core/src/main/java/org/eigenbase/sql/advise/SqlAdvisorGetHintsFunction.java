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
package org.eigenbase.sql.advise;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexCall;
import org.eigenbase.sql.validate.SqlMoniker;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.Types;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.ReflectiveFunctionBase;
import net.hydromatic.optiq.rules.java.*;

import com.google.common.collect.Iterables;

/**
 * Table function that returns completion hints for a given SQL statement.
 */
public class SqlAdvisorGetHintsFunction
    implements TableFunction, ImplementableFunction {
  private static final Expression ADVISOR =
      Expressions.convert_(
          Expressions.call(DataContext.ROOT,
              BuiltinMethod.DATA_CONTEXT_GET.method,
              Expressions.constant(DataContext.Variable.SQL_ADVISOR.camelName)),
          SqlAdvisor.class);

  private static final Method GET_COMPLETION_HINTS =
      Types.lookupMethod(SqlAdvisorGetHintsFunction.class, "getCompletionHints",
        SqlAdvisor.class, String.class, int.class);

  private static final CallImplementor IMPLEMENTOR =
    RexImpTable.createImplementor(
        new NotNullImplementor() {
          public Expression implement(RexToLixTranslator translator,
              RexCall call, List<Expression> operands) {
            return Expressions.call(GET_COMPLETION_HINTS,
                Iterables.concat(Collections.singleton(ADVISOR), operands));
          }
        }, NullPolicy.ANY, false);

  private static final List<FunctionParameter> PARAMETERS =
    ReflectiveFunctionBase.toFunctionParameters(String.class, int.class);

  public CallImplementor getImplementor() {
    return IMPLEMENTOR;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory,
      List<Object> arguments) {
    return typeFactory.createJavaType(SqlAdvisorHint.class);
  }

  public Type getElementType(List<Object> arguments) {
    return SqlAdvisorHint.class;
  }

  public List<FunctionParameter> getParameters() {
    return PARAMETERS;
  }

  /**
   * Returns completion hints for a given SQL statement.
   *
   * <p>Typically this is called from generated code
   * (via {@link SqlAdvisorGetHintsFunction#IMPLEMENTOR}).
   *
   * @param advisor Advisor to produce completion hints
   * @param sql     SQL to complete
   * @param pos     Cursor position in SQL
   * @return the table that contains completion hints for a given SQL statement
   */
  public static Enumerable<SqlAdvisorHint> getCompletionHints(
      final SqlAdvisor advisor, final String sql, final int pos) {
    final String[] replaced = {null};
    final List<SqlMoniker> hints = advisor.getCompletionHints(sql,
      pos, replaced);
    final List<SqlAdvisorHint> res =
        new ArrayList<SqlAdvisorHint>(hints.size() + 1);
    res.add(new SqlAdvisorHint(replaced[0], null, "MATCH"));
    for (SqlMoniker hint : hints) {
      res.add(new SqlAdvisorHint(hint));
    }
    return Linq4j.asEnumerable(res).asQueryable();
  }
}

// End SqlAdvisorGetHintsFunction.java
