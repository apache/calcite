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
package org.apache.calcite.sql2rel;

import org.apache.calcite.sql.SqlFunction;

/**
 *  A factory class used to generate AuxiliaryConverter based on context.
 */
public interface AuxiliaryConverterFactory {

  /**
   * Get AuxiliaryConverter for a given SqlFunction
   * @param f sqlFunction itself.
   * @return the auxiliary converter used in the context.
   */
  AuxiliaryConverter getConverter(SqlFunction f);

  /**
   * Simple Implementation of {@link AuxiliaryConverterFactory}
   */
  class Impl implements AuxiliaryConverterFactory {

    public AuxiliaryConverter getConverter(SqlFunction f) {
      return new AuxiliaryConverter.Impl(f);
    }
  }
}
// End AuxiliaryConverterFactory.java
