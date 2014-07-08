/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.optiq.sarg;

import org.apache.optiq.rex.*;

/**
 * SargBinding represents the binding of a {@link SargExpr} to a particular
 * {@link RexInputRef}.
 */
public class SargBinding {
  //~ Instance fields --------------------------------------------------------

  private final SargExpr expr;

  private final RexInputRef inputRef;

  //~ Constructors -----------------------------------------------------------

  public SargBinding(SargExpr expr, RexInputRef inputRef) {
    this.expr = expr;
    this.inputRef = inputRef;
  }

  //~ Methods ----------------------------------------------------------------

  public SargExpr getExpr() {
    return expr;
  }

  public RexInputRef getInputRef() {
    return inputRef;
  }
}

// End SargBinding.java
