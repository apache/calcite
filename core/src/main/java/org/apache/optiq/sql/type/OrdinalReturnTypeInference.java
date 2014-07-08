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
package org.apache.optiq.sql.type;

import org.apache.optiq.reltype.*;
import org.apache.optiq.sql.*;
import org.apache.optiq.sql.validate.*;

/**
 * Returns the type of the operand at a particular 0-based ordinal position.
 */
public class OrdinalReturnTypeInference implements SqlReturnTypeInference {
  //~ Instance fields --------------------------------------------------------

  private final int ordinal;

  //~ Constructors -----------------------------------------------------------

  public OrdinalReturnTypeInference(int ordinal) {
    this.ordinal = ordinal;
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    return opBinding.getOperandType(ordinal);
  }
}

// End OrdinalReturnTypeInference.java
