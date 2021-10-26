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
package org.apache.calcite.test;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.test.catalog.MockCatalogReader;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * SqlToRelTestBase is an abstract base for tests which involve conversion from
 * SQL to relational algebra.
 *
 * <p>SQL statements to be translated can use the schema defined in
 * {@link MockCatalogReader}; note that this is slightly different from
 * Farrago's SALES schema. If you get a parser or validator error from your test
 * SQL, look down in the stack until you see "Caused by", which will usually
 * tell you the real error.
 */
public abstract class SqlToRelTestBase {
  //~ Static fields/initializers ---------------------------------------------

  protected static final String NL = System.getProperty("line.separator");

  //~ Instance fields --------------------------------------------------------

  //~ Methods ----------------------------------------------------------------

  /** Creates the test fixture that determines the behavior of tests.
   * Sub-classes that, say, test different parser implementations should
   * override. */
  public SqlToRelFixture fixture() {
    return SqlToRelFixture.DEFAULT;
  }

  /**
   * Creates a test context with a SQL query.
   * Default catalog: {@link org.apache.calcite.test.catalog.MockCatalogReaderSimple#init()}.
   */
  public final SqlToRelFixture sql(String sql) {
    return fixture().withSql(sql);
  }

  public final SqlToRelFixture expr(String sql) {
    return fixture().expression(true).withSql(sql);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Custom implementation of Correlate for testing.
   */
  public static class CustomCorrelate extends Correlate {
    public CustomCorrelate(
        RelOptCluster cluster,
        RelTraitSet traits,
        List<RelHint> hints,
        RelNode left,
        RelNode right,
        CorrelationId correlationId,
        ImmutableBitSet requiredColumns,
        JoinRelType joinType) {
      super(cluster, traits, hints, left, right, correlationId, requiredColumns,
          joinType);
    }

    @Override public Correlate copy(RelTraitSet traitSet,
        RelNode left, RelNode right, CorrelationId correlationId,
        ImmutableBitSet requiredColumns, JoinRelType joinType) {
      return new CustomCorrelate(getCluster(), traitSet, hints, left, right,
          correlationId, requiredColumns, joinType);
    }

    @Override public RelNode withHints(List<RelHint> hintList) {
      return new CustomCorrelate(getCluster(), traitSet, hintList, left, right,
          correlationId, requiredColumns, joinType);
    }

    @Override public RelNode accept(RelShuttle shuttle) {
      return shuttle.visit(this);
    }
  }

}
