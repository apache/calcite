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

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Join}.
 */
public class JoinNode implements Node {
  private final Source leftSource;
  private final Source rightSource;
  private final Sink sink;
  private final Join rel;
  private final Scalar condition;
  private final Context context;

  public JoinNode(Compiler compiler, Join rel) {
    this.leftSource = compiler.source(rel, 0);
    this.rightSource = compiler.source(rel, 1);
    this.sink = compiler.sink(rel);
    this.condition = compiler.compile(ImmutableList.of(rel.getCondition()),
        compiler.combinedRowType(rel.getInputs()));
    this.rel = rel;
    this.context = compiler.createContext();

  }

  public void run() throws InterruptedException {

    final int fieldCount = rel.getLeft().getRowType().getFieldCount()
        + rel.getRight().getRowType().getFieldCount();
    context.values = new Object[fieldCount];

    // source for the outer relation of nested loop
    Source outerSource = leftSource;
    // source for the inner relation of nested loop
    Source innerSource = rightSource;
    if (rel.getJoinType() == JoinRelType.RIGHT) {
      outerSource = rightSource;
      innerSource = leftSource;
    }

    // row from outer source
    Row outerRow = null;
    // rows from inner source
    List<Row> innerRows = null;
    Set<Row> matchRowSet = new HashSet<>();
    while ((outerRow = outerSource.receive()) != null) {
      if (innerRows == null) {
        innerRows = new ArrayList<Row>();
        Row innerRow = null;
        while ((innerRow = innerSource.receive()) != null) {
          innerRows.add(innerRow);
        }
      }
      matchRowSet.addAll(doJoin(outerRow, innerRows, rel.getJoinType()));
    }
    if (rel.getJoinType() == JoinRelType.FULL) {
      // send un-match rows for full join on right source
      List<Row> empty = new ArrayList<>();
      for (Row row: innerRows) {
        if (matchRowSet.contains(row)) {
          continue;
        }
        doSend(row, empty, JoinRelType.RIGHT);
      }
    }
  }

  /**
   * Execution of the join action, returns the matched rows for the outer source row.
   */
  private List<Row> doJoin(Row outerRow, List<Row> innerRows,
      JoinRelType joinRelType) throws InterruptedException {
    boolean outerRowOnLeft = joinRelType != JoinRelType.RIGHT;
    copyToContext(outerRow, outerRowOnLeft);
    List<Row> matchInnerRows = new ArrayList<>();
    for (Row innerRow: innerRows) {
      copyToContext(innerRow, !outerRowOnLeft);
      final Boolean execute = (Boolean) condition.execute(context);
      if (execute != null && execute) {
        matchInnerRows.add(innerRow);
      }
    }
    doSend(outerRow, matchInnerRows, joinRelType);
    return matchInnerRows;
  }

  /**
   * If there exists matched rows with the outer row, sends the corresponding joined result,
   * otherwise, checks if need to use null value for column.
   */
  private void doSend(Row outerRow, List<Row> matchInnerRows,
      JoinRelType joinRelType) throws InterruptedException {
    if (!matchInnerRows.isEmpty()) {
      switch (joinRelType) {
      case INNER:
      case LEFT:
      case RIGHT:
      case FULL:
        boolean outerRowOnLeft = joinRelType != JoinRelType.RIGHT;
        copyToContext(outerRow, outerRowOnLeft);
        for (Row row: matchInnerRows) {
          copyToContext(row, !outerRowOnLeft);
          sink.send(Row.asCopy(context.values));
        }
        break;
      case SEMI:
        sink.send(Row.asCopy(outerRow.getValues()));
        break;
      }
    } else {
      switch (joinRelType) {
      case LEFT:
      case RIGHT:
      case FULL:
        int nullColumnNum = context.values.length - outerRow.size();
        // for full join, use left source as outer source,
        // and send un-match rows in left source fist,
        // the un-match rows in right source will be process later.
        copyToContext(outerRow, joinRelType.generatesNullsOnRight());
        int nullColumnStart = joinRelType.generatesNullsOnRight() ? outerRow.size() : 0;
        System.arraycopy(new Object[nullColumnNum], 0,
            context.values, nullColumnStart, nullColumnNum);
        sink.send(Row.asCopy(context.values));
        break;
      case ANTI:
        sink.send(Row.asCopy(outerRow.getValues()));
        break;
      }
    }
  }

  /**
   * Copies the value of row into context values.
   */
  private void copyToContext(Row row, boolean toLeftSide) {
    Object[] values = row.getValues();
    if (toLeftSide) {
      System.arraycopy(values, 0, context.values, 0, values.length);
    } else {
      System.arraycopy(values, 0, context.values,
          context.values.length - values.length, values.length);
    }
  }
}
