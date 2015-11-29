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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.BuiltInMethod;

/**
 * RelMdMaxRowCount supplies a default implementation of
 * {@link RelMetadataQuery#getMaxRowCount} for the standard logical algebra.
 */
public class RelMdMaxRowCount {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.MAX_ROW_COUNT.method, new RelMdMaxRowCount());

  //~ Methods ----------------------------------------------------------------

  public Double getMaxRowCount(Union rel) {
    double nRows = 0.0;

    for (RelNode input : rel.getInputs()) {
      Double partialRowCount = RelMetadataQuery.getMaxRowCount(input);
      if (partialRowCount == null || partialRowCount.equals(Double.POSITIVE_INFINITY)) {
        return Double.POSITIVE_INFINITY;
      }
      nRows += partialRowCount;
    }
    return nRows;
  }

  public Double getMaxRowCount(Filter rel) {
    return RelMetadataQuery.getMaxRowCount(rel.getInput());
  }

  public Double getMaxRowCount(Project rel) {
    return RelMetadataQuery.getMaxRowCount(rel.getInput());
  }

  public Double getMaxRowCount(Sort rel) {
    final Double rowCount = RelMetadataQuery.getMaxRowCount(rel.getInput());
    if (rowCount != null && rel.fetch != null) {
      final int offset = rel.offset == null ? 0 : RexLiteral.intValue(rel.offset);
      final int limit = RexLiteral.intValue(rel.fetch);
      final Double offsetLimit = new Double(offset + limit);
      // offsetLimit is smaller than rowCount of the input operator
      // thus, we return the offsetLimit
      if (offsetLimit < rowCount) {
        return offsetLimit;
      }
    }
    return rowCount;
  }

  public Double getMaxRowCount(Aggregate rel) {
    return RelMetadataQuery.getMaxRowCount(rel.getInput());
  }

  public Double getMaxRowCount(Join rel) {
    Double left = RelMetadataQuery.getMaxRowCount(rel.getLeft());
    Double right = RelMetadataQuery.getMaxRowCount(rel.getLeft());
    if (left.equals(Double.POSITIVE_INFINITY) || right.equals(Double.POSITIVE_INFINITY)) {
      return Double.POSITIVE_INFINITY;
    } else {
      return RelMetadataQuery.getMaxRowCount(rel.getLeft())
              * RelMetadataQuery.getMaxRowCount(rel.getRight());
    }
  }

  // Catch-all rule when none of the others apply.
  public Double getMaxRowCount(RelNode rel) {
    return Double.POSITIVE_INFINITY;
  }
}

// End RelMdMaxRowCount.java
