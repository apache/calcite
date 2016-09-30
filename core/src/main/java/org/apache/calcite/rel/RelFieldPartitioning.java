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
package org.apache.calcite.rel;

/**
 * Definition of the partitioning of one field of a {@link RelNode} whose
 * output is to be partitioned.
 *
 * @see RelPartitioning
 */
public class RelFieldPartitioning {

/**
 * Kind of partitioning on field
 */
  public enum PartitioningKind {

    /**
     * Hash value of field is used to determine on which partition field
     *  belongs to.
     */
    HASH("HASH"),

    /**
     * Value of field is used to determine on which partition field
     * belongs to.
     */
    RANGE("RANGE");

    public final String shortString;

    PartitioningKind(String shortString) {
      this.shortString = shortString;
    }
  }

  //~ Instance fields --------------------------------------------------------

  /**
   * 0-based index of field on which input is being partitioned on.
   */
  private final int fieldIndex;

  /**
   * Kind of partitioning.
   */
  public final PartitioningKind kind;

  public RelFieldPartitioning(int fieldIndex, PartitioningKind kind) {
    this.fieldIndex = fieldIndex;
    this.kind = kind;
  }

  public int getFieldIndex() {
    return fieldIndex;
  }
}
