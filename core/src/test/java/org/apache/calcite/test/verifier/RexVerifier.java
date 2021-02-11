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
package org.apache.calcite.test.verifier;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;

import com.microsoft.z3.BoolExpr;
import com.microsoft.z3.Context;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Verify if two given Rex Node are logically equivalent.
 **/
public class RexVerifier {

  private RexVerifier() {
    // not called
  }


  /**
   * Verify if two given Rex Node are logically equivalent on same inputs.
   * False can mean unknown.
   */
  public static boolean logicallyEquivalent(RexNode n1, RexNode n2) {
    Map<Integer, RelDataType> indexWithType = new HashMap<>();
    RexInputRefFinder finder = new RexInputRefFinder(indexWithType);
    n1.accept(finder);
    finder = new RexInputRefFinder(finder.getIndexWithType());
    n2.accept(finder);
    indexWithType = finder.getIndexWithType();
    Context z3Context = new Context();
    List<SymbolicColumn> symbolicInputs = constructSymbolicInputs(indexWithType, z3Context);
    return logicallyEquivalent(n1, symbolicInputs, n2, symbolicInputs, z3Context);
  }

  /**
   * Verify if two given Rex Node are logically equivalent on different inputs.
   */
  public static boolean logicallyEquivalent(RexNode n1, List<SymbolicColumn> input1,
      RexNode n2, List<SymbolicColumn> input2, Context z3Context) {
    List<BoolExpr> env = new ArrayList<>();
    SymbolicColumn symbolicCond1 =
        RexToSymbolicColumn.rexToColumn(n1, input1, z3Context, env);
    SymbolicColumn symbolicCond2 =
        RexToSymbolicColumn.rexToColumn(n2, input2, z3Context, env);
    return SymbolicColumn.checkCondEq(symbolicCond1, symbolicCond2, env, z3Context);
  }

  private static List<SymbolicColumn> constructSymbolicInputs(
      Map<Integer, RelDataType> indexWithType, Context z3Context) {
    int maximumIndex = 0;
    for (Map.Entry<Integer, RelDataType> inputRef : indexWithType.entrySet()) {
      if (inputRef.getKey() > maximumIndex) {
        maximumIndex = inputRef.getKey();
      }
    }
    List<SymbolicColumn> symbolicColumns = new ArrayList<>();
    for (int i = 0; i <= maximumIndex; i++) {
      if (indexWithType.containsKey(i)) {
        SymbolicColumn symbolicColumn =
            SymbolicColumn.mkNewSymbolicColumn(z3Context, indexWithType.get(i));
        symbolicColumns.add(symbolicColumn);
      } else {
        SymbolicColumn unusedColumn = SymbolicColumn.mkUnusedColumn(z3Context);
        symbolicColumns.add(unusedColumn);
      }
    }
    return symbolicColumns;
  }

  /**
   * Walks over an expression and collects all input ref index.
   */
  private static class RexInputRefFinder extends RexVisitorImpl<Void> {
    private Map<Integer, RelDataType> indexWithType;

    RexInputRefFinder(Map<Integer, RelDataType> knownIndexWithType) {
      super(true);
      this.indexWithType = knownIndexWithType;
    }

    public Map<Integer, RelDataType> getIndexWithType() {
      return this.indexWithType;
    }

    @Override public Void visitInputRef(RexInputRef inputRef) {
      int index = inputRef.getIndex();
      if (!indexWithType.containsKey(index)) {
        indexWithType.put(index, inputRef.getType());
      }
      return null;
    }
  }

}
