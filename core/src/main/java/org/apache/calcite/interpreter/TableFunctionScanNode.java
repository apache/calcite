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
package org.apache.calcite.interpreter;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Enumerables;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;

import com.google.common.collect.ImmutableList;

/**
 * Interpreter node that implements a
 * {@link TableFunctionScan}.
 */
public class TableFunctionScanNode implements Node {
  private final Scalar scalar;
  private final Context context;
  private final Sink sink;

  private TableFunctionScanNode(Compiler compiler, TableFunctionScan rel) {
    this.scalar =
        compiler.compile(ImmutableList.of(rel.getCall()), rel.getRowType());
    this.context = compiler.createContext();
    this.sink = compiler.sink(rel);
  }

  @Override public void run() throws InterruptedException {
    final Object o = scalar.execute(context);
    if (o instanceof Enumerable) {
      @SuppressWarnings("unchecked") final Enumerable<Row> rowEnumerable =
          Enumerables.toRow((Enumerable) o);
      final Enumerator<Row> enumerator = rowEnumerable.enumerator();
      while (enumerator.moveNext()) {
        sink.send(enumerator.current());
      }
    }
  }

  /** Creates a TableFunctionScanNode. */
  static TableFunctionScanNode create(Compiler compiler, TableFunctionScan rel) {
    RexNode call = rel.getCall();
    if (call instanceof RexCall) {
      SqlOperator operator = ((RexCall) call).getOperator();
      if (operator instanceof SqlUserDefinedTableFunction) {
        Function function = ((SqlUserDefinedTableFunction) operator).function;
        if (function instanceof TableFunctionImpl) {
          return new TableFunctionScanNode(compiler, rel);
        }
      }
    }
    throw new AssertionError("cannot convert table function scan "
        + rel.getCall() + " to enumerable");
  }
}
