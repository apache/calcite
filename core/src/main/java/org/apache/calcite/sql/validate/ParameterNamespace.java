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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Namespace representing the type of a dynamic parameter.
 *
 * @see ParameterScope
 */
class ParameterNamespace extends AbstractNamespace {
  //~ Instance fields --------------------------------------------------------

  @SuppressWarnings("HidingField")
  private final RelDataType type;

  //~ Constructors -----------------------------------------------------------

  ParameterNamespace(SqlValidatorImpl validator, RelDataType type) {
    super(validator, null);
    this.type = type;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public @Nullable SqlNode getNode() {
    return null;
  }

  @Override public RelDataType validateImpl(RelDataType targetRowType) {
    return type;
  }

  @Override public RelDataType getRowType() {
    return type;
  }
}
