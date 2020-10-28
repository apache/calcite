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

import com.microsoft.z3.BoolExpr;
import com.microsoft.z3.Context;

import java.util.List;

/**
 * Help functions for using z3.
 **/

public class Z3Utility {

  private Z3Utility() {
    // not called
  }

  public static BoolExpr mkAnd(List<BoolExpr> constraints, Context z3Context) {
    BoolExpr[] andC = new BoolExpr[constraints.size()];
    constraints.toArray(andC);
    return (BoolExpr) z3Context.mkAnd(andC).simplify();
  }

  public static BoolExpr mkOr(List<BoolExpr> constraints, Context z3Context) {
    BoolExpr[] orC = new BoolExpr[constraints.size()];
    constraints.toArray(orC);
    return (BoolExpr) z3Context.mkOr(orC).simplify();
  }
}
