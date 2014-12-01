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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NumberUtil;

/**
 * RelMdRowCount supplies a default implementation of
 * {@link RelMetadataQuery#getRowCount} for the standard logical algebra.
 */
public class RelMdRowCount {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.ROW_COUNT.method, new RelMdRowCount());

  //~ Methods ----------------------------------------------------------------

  public Double getRowCount(Union rel) {
    double nRows = 0.0;

    for (RelNode input : rel.getInputs()) {
      Double partialRowCount = RelMetadataQuery.getRowCount(input);
      if (partialRowCount == null) {
        return null;
      }
      nRows += partialRowCount;
    }
    return nRows;
  }

  public Double getRowCount(Filter rel) {
    return NumberUtil.multiply(
        RelMetadataQuery.getSelectivity(
            rel.getInput(),
            rel.getCondition()),
        RelMetadataQuery.getRowCount(rel.getInput()));
  }

  public Double getRowCount(Project rel) {
    return RelMetadataQuery.getRowCount(rel.getInput());
  }

  public Double getRowCount(Sort rel) {
    return RelMetadataQuery.getRowCount(rel.getInput());
  }

  public Double getRowCount(SemiJoin rel) {
    // create a RexNode representing the selectivity of the
    // semijoin filter and pass it to getSelectivity
    RexNode semiJoinSelectivity =
        RelMdUtil.makeSemiJoinSelectivityRexNode(rel);

    return NumberUtil.multiply(
        RelMetadataQuery.getSelectivity(
            rel.getLeft(),
            semiJoinSelectivity),
        RelMetadataQuery.getRowCount(rel.getLeft()));
  }

  public Double getRowCount(Aggregate rel) {
    ImmutableBitSet groupKey = ImmutableBitSet.range(rel.getGroupCount());

    // rowcount is the cardinality of the group by columns
    Double distinctRowCount =
        RelMetadataQuery.getDistinctRowCount(
            rel.getInput(),
            groupKey,
            null);
    if (distinctRowCount == null) {
      return RelMetadataQuery.getRowCount(rel.getInput()) / 10;
    } else {
      return distinctRowCount;
    }
  }

  // Catch-all rule when none of the others apply.
  public Double getRowCount(RelNode rel) {
    return rel.getRows();
  }
}

// End RelMdRowCount.java
