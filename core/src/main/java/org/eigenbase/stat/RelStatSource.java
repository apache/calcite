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

import org.eigenbase.sarg.*;

/**
 * This class encapsulates statistics for a RelNode
 */
public interface RelStatSource {
  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the number of rows in a relation, as determined by statistics
   *
   * @return a row count, or null if one could not be determined
   */
  Double getRowCount();

  /**
   * Returns statistics pertaining to a column specified by the 0-based
   * ordinal and the sargable predicates associated with that column. The
   * second argument can be null if there are no sargable predicates on the
   * column.
   *
   * @param ordinal   zero based column ordinal
   * @param predicate associated predicates(s), evaluated as intervals
   * @return filtered column statistics, or null if they could not be obtained
   */
  RelStatColumnStatistics getColumnStatistics(
      int ordinal,
      SargIntervalSequence predicate);
}

// End RelStatSource.java
