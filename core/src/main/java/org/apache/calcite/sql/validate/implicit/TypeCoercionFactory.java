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
package org.apache.calcite.sql.validate.implicit;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlValidator;

import org.apiguardian.api.API;

/** Factory for {@link TypeCoercion} objects.
 *
 * <p>A type coercion factory allows you to include custom rules of
 * implicit type coercion. Usually you should inherit the {@link TypeCoercionImpl}
 * and override the methods that you want to customize.
 *
 * <p>This interface is experimental and would change without notice.
 *
 * @see SqlValidator.Config#withTypeCoercionFactory
 */
@API(status = API.Status.EXPERIMENTAL, since = "1.23")
public interface TypeCoercionFactory {

  /**
   * Creates a TypeCoercion.
   *
   * @param typeFactory Type factory
   * @param validator   SQL validator
   */
  TypeCoercion create(RelDataTypeFactory typeFactory, SqlValidator validator);
}
