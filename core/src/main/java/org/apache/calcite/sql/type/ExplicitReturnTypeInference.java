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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.SqlOperatorBinding;

/**
 * A {@link SqlReturnTypeInference} which always returns the same SQL type.
 */
public class ExplicitReturnTypeInference implements SqlReturnTypeInference {
  //~ Instance fields --------------------------------------------------------

  protected final RelProtoDataType protoType;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an inference rule which always returns the same type object.
   *
   * <p>If the requesting type factory is different, returns a copy of the
   * type object made using {@link RelDataTypeFactory#copyType(RelDataType)}
   * within the requesting type factory.
   *
   * <p>A copy of the type is required because each statement is prepared using
   * a different type factory; each type factory maintains its own cache of
   * canonical instances of each type.
   *
   * @param protoType Type object
   */
  protected ExplicitReturnTypeInference(RelProtoDataType protoType) {
    assert protoType != null;
    this.protoType = protoType;
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    return protoType.apply(opBinding.getTypeFactory());
  }
}

// End ExplicitReturnTypeInference.java
