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
package org.eigenbase.stat;

/**
 * This interface provides results based on column statistics. It may be used to
 * summarize the results of applying a predicate to a column of a relational
 * expression. Alternatively, it may be used to summarize aspects of the entire
 * column.
 */
public interface RelStatColumnStatistics {
  //~ Methods ----------------------------------------------------------------

  /**
   * Estimates the percentage of a relational expression's rows which satisfy
   * a given condition. This corresponds to the metadata query {@link
   * org.eigenbase.rel.metadata.RelMetadataQuery#getSelectivity}.
   *
   * @return an estimated percentage from 0.0 to 1.0 or null if no reliable
   * estimate can be determined
   */
  Double getSelectivity();

  /**
   * Estimates the number of distinct values returned from a relational
   * expression that satisfy a given condition.
   *
   * @return an estimate of the distinct values of a predicate or null if no
   * reliable estimate can be determined
   */
  Double getCardinality();

  /**
   * Determine how many blocks on disk will be read from physical storage
   * to retrieve the column values selected. This corresponds to an
   * attribute set by the Broadbase server. This feature is deferred until
   * we find a use for it
   */
  // public Long getNumBlocks();
}

// End RelStatColumnStatistics.java
