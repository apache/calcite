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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;

/**
 * Returns the rowtype of a cursor of the operand at a particular 0-based
 * ordinal position.
 *
 * @see OrdinalReturnTypeInference
 */
public class CursorReturnTypeInference implements SqlReturnTypeInference {
  //~ Instance fields --------------------------------------------------------

  private final int ordinal;

  //~ Constructors -----------------------------------------------------------

  public CursorReturnTypeInference(int ordinal) {
    this.ordinal = ordinal;
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    return opBinding.getCursorOperand(ordinal);
  }
}

// End CursorReturnTypeInference.java
