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
package org.apache.calcite.plan.cascades.rel;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;

/**
 *
 */
public class TestTable extends AbstractTable {
  private final RelDataType dataType;
  private final double rowCount;
  private final List<RelCollation> collations;
  private final RelDistribution distribution;

  public TestTable(RelDataType type, double rowCount,
      final List<RelCollation> collations,
      RelDistribution distribution) {
    this.dataType = type;
    this.rowCount = rowCount;
    this.collations = collations;
    this.distribution = distribution;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return dataType;
  }

  @Override public Statistic getStatistic() {
    return Statistics.of(rowCount,
        collations,
        distribution);
  }
}
