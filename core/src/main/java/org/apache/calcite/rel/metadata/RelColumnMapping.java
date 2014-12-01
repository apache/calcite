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
package org.apache.calcite.rel.metadata;

/**
 * Mapping from an input column of a {@link org.apache.calcite.rel.RelNode} to
 * one of its output columns.
 */
public class RelColumnMapping {
  public RelColumnMapping(
      int iOutputColumn, int iInputRel, int iInputColumn, boolean derived) {
    this.iOutputColumn = iOutputColumn;
    this.iInputRel = iInputRel;
    this.iInputColumn = iInputColumn;
    this.derived = derived;
  }

  //~ Instance fields --------------------------------------------------------

  /**
   * 0-based ordinal of mapped output column.
   */
  public final int iOutputColumn;

  /**
   * 0-based ordinal of mapped input rel.
   */
  public final int iInputRel;

  /**
   * 0-based ordinal of mapped column within input rel.
   */
  public final int iInputColumn;

  /**
   * Whether the column mapping transforms the input.
   */
  public final boolean derived;
}

// End RelColumnMapping.java
