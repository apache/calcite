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
package net.hydromatic.optiq.runtime;

import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.optiq.DataContext;

/**
 * Executable statement.
 */
public interface Executable {
  /**
   * Executes this statement and returns an enumerable which will yield rows.
   * The {@code environment} parameter provides the values in the root of the
   * environment (usually schemas).
   *
   * @param dataContext Environment that provides tables
   * @return Enumerable over rows
   */
  Enumerable execute(DataContext dataContext);
}

// End Executable.java
