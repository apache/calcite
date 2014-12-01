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
package org.apache.calcite.schema;

import java.util.List;

/**
 * Named expression that accepts parameters and returns a result.
 *
 * <p>The application may occur at compile time (for a macro) or at run time
 * (for a regular function). The result may be a relation, and so might any of
 * the parameters.</p>
 *
 * <p>Functions are registered in a {@link Schema}, and may be queried by name
 * ({@link Schema#getFunctions(String)}) then overloads resolved based on
 * parameter types.</p>
 *
 * @see TableMacro
 * @see ScalarFunction
 */
public interface Function {
  /**
   * Returns the parameters of this function.
   *
   * @return Parameters; never null
   */
  List<FunctionParameter> getParameters();
}

// End Function.java
