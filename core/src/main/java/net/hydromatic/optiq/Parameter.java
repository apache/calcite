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
package net.hydromatic.optiq;

import org.eigenbase.reltype.RelDataType;

/**
 * Parameter to a {@link TableFunction}.
 */
public interface Parameter {
  /**
   * Zero-based ordinal of this parameter within the member's parameter
   * list.
   *
   * @return Parameter ordinal
   */
  int getOrdinal();

  /**
   * Name of the parameter.
   *
   * @return Parameter name
   */
  String getName();

  /**
   * Returns the type of this parameter.
   *
   * @return Parameter type.
   */
  RelDataType getType();
}

// End Parameter.java
