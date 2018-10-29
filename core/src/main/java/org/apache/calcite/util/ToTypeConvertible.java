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
package org.apache.calcite.util;

import org.apache.calcite.rel.type.RelDataType;

/**
 * Helper API to deal with conversion between internal sql type representation and java sql types
 */
public interface ToTypeConvertible {

  /**
   * Converts to desired type, null otherwise
   * @param type desired type to be converted to
   * @return value converted to class specified by type or null when conversion is not possible
   */
  Object convertTo(RelDataType type);

}

// End ToTypeConvertible.java
