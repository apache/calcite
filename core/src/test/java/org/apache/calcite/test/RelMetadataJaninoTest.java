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

import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit test for {@link DefaultRelMetadataProvider}.
 */
public class RelMetadataJaninoTest extends RelMetadataTestBase {
  @Override Supplier<RelMetadataQuery> getSupplier() {
    return RelMetadataQuery::instance;
  }

  @Test void testBrokenCustomProviderWithMetadataFactory() {
    final List<String> buf = new ArrayList<>();
    ColTypeImpl.THREAD_LIST.set(buf);

    final String sql = "select deptno, count(*) from emp where deptno > 10 "
        + "group by deptno having count(*) = 0";
    final RelRoot root = tester
        .withClusterFactory(cluster -> {
          cluster.setMetadataProvider(
              ChainedRelMetadataProvider.of(
                  ImmutableList.of(BrokenColTypeImpl.SOURCE,
                      cluster.getMetadataProvider())));
          return cluster;
        })
        .convertSqlToRel(sql);

    final RelNode rel = root.rel;
    assertThat(rel, instanceOf(LogicalFilter.class));
    final MyRelMetadataQuery mq = new MyRelMetadataQuery();

    try {
      assertThat(colType(mq, rel, 0), equalTo("DEPTNO-rel"));
      fail("expected error");
    } catch (IllegalArgumentException e) {
      final String value = "No handler for method [public abstract "
          + "java.lang.String org.apache.calcite.test.RelMetadataJaninoTest$ColType$Handler.getColType("
          + "org.apache.calcite.rel.RelNode,org.apache.calcite.rel.metadata.RelMetadataQuery,int)] "
          + "applied to argument of type [class org.apache.calcite.rel.logical.LogicalFilter]; "
          + "we recommend you create a catch-all (RelNode) handler";
      assertThat(e.getMessage(), is(value));
    }
  }

  @Test void testBrokenCustomProviderWithMetadataQuery() {
    final List<String> buf = new ArrayList<>();
    ColTypeImpl.THREAD_LIST.set(buf);

    final String sql = "select deptno, count(*) from emp where deptno > 10 "
        + "group by deptno having count(*) = 0";
    final RelRoot root = tester
        .withClusterFactory(cluster -> {
          cluster.setMetadataProvider(
              ChainedRelMetadataProvider.of(
                  ImmutableList.of(BrokenColTypeImpl.SOURCE,
                      cluster.getMetadataProvider())));
          cluster.setMetadataQuerySupplier(MyRelMetadataQuery::new);
          return cluster;
        })
        .convertSqlToRel(sql);

    final RelNode rel = root.rel;
    assertThat(rel, instanceOf(LogicalFilter.class));
    assertThat(rel.getCluster().getMetadataQuery(), instanceOf(MyRelMetadataQuery.class));
    final MyRelMetadataQuery mq = (MyRelMetadataQuery) rel.getCluster().getMetadataQuery();

    try {
      assertThat(colType(mq, rel, 0), equalTo("DEPTNO-rel"));
      fail("expected error");
    } catch (IllegalArgumentException e) {
      final String value = "No handler for method [public abstract java.lang.String "
          + "org.apache.calcite.test.RelMetadataJaninoTest$ColType$Handler.getColType("
          + "org.apache.calcite.rel.RelNode,org.apache.calcite.rel.metadata.RelMetadataQuery,int)]"
          + " applied to argument of type [class org.apache.calcite.rel.logical.LogicalFilter];"
          + " we recommend you create a catch-all (RelNode) handler";
      assertThat(e.getMessage(), is(value));
    }
  }

  @Deprecated // to be removed before 2.0
  public String colType(RelMetadataQuery mq, RelNode rel, int column) {
    return rel.metadata(ColType.class, mq).getColType(column);
  }

  public String colType(MyRelMetadataQuery myRelMetadataQuery, RelNode rel, int column) {
    return myRelMetadataQuery.colType(rel, column);
  }

  @Deprecated // to be removed before 2.0
  @Test void testCustomProviderWithRelMetadataFactory() {
    final List<String> buf = new ArrayList<>();
    ColTypeImpl.THREAD_LIST.set(buf);

    final String sql = "select deptno, count(*) from emp where deptno > 10 "
        + "group by deptno having count(*) = 0";
    final RelRoot root = tester
        .withClusterFactory(cluster -> {
          // Create a custom provider that includes ColType.
          // Include the same provider twice just to be devious.
          final ImmutableList<RelMetadataProvider> list =
              ImmutableList.of(ColTypeImpl.SOURCE, ColTypeImpl.SOURCE,
                  cluster.getMetadataProvider());
          cluster.setMetadataProvider(
              ChainedRelMetadataProvider.of(list));
          return cluster;
        })
        .convertSqlToRel(sql);
    final RelNode rel = root.rel;

    // Top node is a filter. Its metadata uses getColType(RelNode, int).
    assertThat(rel, instanceOf(LogicalFilter.class));
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    assertThat(colType(mq, rel, 0), equalTo("DEPTNO-rel"));
    assertThat(colType(mq, rel, 1), equalTo("EXPR$1-rel"));

    // Next node is an aggregate. Its metadata uses
    // getColType(LogicalAggregate, int).
    final RelNode input = rel.getInput(0);
    assertThat(input, instanceOf(LogicalAggregate.class));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));

    // There is no caching. Another request causes another call to the provider.
    assertThat(buf.toString(), equalTo("[DEPTNO-rel, EXPR$1-rel, DEPTNO-agg]"));
    assertThat(buf.size(), equalTo(3));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(4));

    // Now add a cache. Only the first request for each piece of metadata
    // generates a new call to the provider.
    final RelOptPlanner planner = rel.getCluster().getPlanner();
    rel.getCluster().setMetadataProvider(
        new org.apache.calcite.rel.metadata.CachingRelMetadataProvider(
            rel.getCluster().getMetadataProvider(), planner));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(5));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(5));
    assertThat(colType(mq, input, 1), equalTo("EXPR$1-agg"));
    assertThat(buf.size(), equalTo(6));
    assertThat(colType(mq, input, 1), equalTo("EXPR$1-agg"));
    assertThat(buf.size(), equalTo(6));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(6));

    // With a different timestamp, a metadata item is re-computed on first call.
    long timestamp = planner.getRelMetadataTimestamp(rel);
    assertThat(timestamp, equalTo(0L));
    ((MockRelOptPlanner) planner).setRelMetadataTimestamp(timestamp + 1);
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(7));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(7));
  }

  @Test void testCustomProviderWithRelMetadataQuery() {
    final List<String> buf = new ArrayList<>();
    ColTypeImpl.THREAD_LIST.set(buf);

    final String sql = "select deptno, count(*) from emp where deptno > 10 "
        + "group by deptno having count(*) = 0";
    final RelRoot root = tester
        .withClusterFactory(cluster -> {
          // Create a custom provider that includes ColType.
          // Include the same provider twice just to be devious.
          final ImmutableList<RelMetadataProvider> list =
              ImmutableList.of(ColTypeImpl.SOURCE, ColTypeImpl.SOURCE,
                  cluster.getMetadataProvider());
          cluster.setMetadataProvider(
              ChainedRelMetadataProvider.of(list));
          cluster.setMetadataQuerySupplier(MyRelMetadataQuery::new);
          return cluster;
        })
        .convertSqlToRel(sql);
    final RelNode rel = root.rel;

    // Top node is a filter. Its metadata uses getColType(RelNode, int).
    assertThat(rel, instanceOf(LogicalFilter.class));
    assertThat(rel.getCluster().getMetadataQuery(), instanceOf(MyRelMetadataQuery.class));
    final MyRelMetadataQuery mq = (MyRelMetadataQuery) rel.getCluster().getMetadataQuery();
    assertThat(colType(mq, rel, 0), equalTo("DEPTNO-rel"));
    assertThat(colType(mq, rel, 1), equalTo("EXPR$1-rel"));

    // Next node is an aggregate. Its metadata uses
    // getColType(LogicalAggregate, int).
    final RelNode input = rel.getInput(0);
    assertThat(input, instanceOf(LogicalAggregate.class));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));

    // The metadata query is caching, only the first request for each piece of metadata
    // generates a new call to the provider.
    assertThat(buf.toString(), equalTo("[DEPTNO-rel, EXPR$1-rel, DEPTNO-agg]"));
    assertThat(buf.size(), equalTo(3));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(3));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(3));
    assertThat(colType(mq, input, 1), equalTo("EXPR$1-agg"));
    assertThat(buf.size(), equalTo(4));
    assertThat(colType(mq, input, 1), equalTo("EXPR$1-agg"));
    assertThat(buf.size(), equalTo(4));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(4));

    // Invalidate the metadata query triggers clearing of all the metadata.
    rel.getCluster().invalidateMetadataQuery();
    assertThat(rel.getCluster().getMetadataQuery(), instanceOf(MyRelMetadataQuery.class));
    final MyRelMetadataQuery mq1 = (MyRelMetadataQuery) rel.getCluster().getMetadataQuery();
    assertThat(colType(mq1, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(5));
    assertThat(colType(mq1, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(5));
    // Resets the RelMetadataQuery to default.
    rel.getCluster().setMetadataQuerySupplier(RelMetadataQuery::instance);
  }

  /** Custom metadata interface. */
  public interface ColType extends Metadata {
    Method METHOD = Types.lookupMethod(ColType.class, "getColType", int.class);

    MetadataDef<ColType> DEF =
        MetadataDef.of(ColType.class, ColType.Handler.class, METHOD);

    String getColType(int column);

    /** Handler API. */
    interface Handler extends MetadataHandler<ColType> {
      String getColType(RelNode r, RelMetadataQuery mq, int column);
    }
  }

  /** A provider for {@link RelMetadataTestBase.ColType} via
   * reflection. */
  public abstract static class PartialColTypeImpl
      implements MetadataHandler<ColType> {
    static final ThreadLocal<List<String>> THREAD_LIST = new ThreadLocal<>();

    @Deprecated
    public MetadataDef<ColType> getDef() {
      return ColType.DEF;
    }

    /** Implementation of {@link ColType#getColType(int)} for
     * {@link org.apache.calcite.rel.logical.LogicalAggregate}, called via
     * reflection. */
    @SuppressWarnings("UnusedDeclaration")
    public String getColType(Aggregate rel, RelMetadataQuery mq, int column) {
      final String name =
          rel.getRowType().getFieldList().get(column).getName() + "-agg";
      THREAD_LIST.get().add(name);
      return name;
    }
  }

  /** A provider for {@link RelMetadataTestBase.ColType} via
   * reflection. */
  public static class ColTypeImpl extends PartialColTypeImpl {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(new ColTypeImpl(), ColType.Handler.class);

    /** Implementation of {@link ColType#getColType(int)} for
     * {@link RelNode}, called via reflection. */
    @SuppressWarnings("UnusedDeclaration")
    public String getColType(RelNode rel, RelMetadataQuery mq, int column) {
      final String name =
          rel.getRowType().getFieldList().get(column).getName() + "-rel";
      THREAD_LIST.get().add(name);
      return name;
    }
  }

  /** Implementation of {@link ColType} that has no fall-back for {@link RelNode}. */
  public static class BrokenColTypeImpl extends PartialColTypeImpl {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            new BrokenColTypeImpl(), ColType.Handler.class);
  }

  /** Extension to {@link RelMetadataQuery} to support {@link ColType}.
   *
   * <p>Illustrates how you would package up a user-defined metadata type. */
  private static class MyRelMetadataQuery extends RelMetadataQuery {
    private ColType.Handler colTypeHandler;

    MyRelMetadataQuery() {
      colTypeHandler = initialHandler(ColType.Handler.class);
    }

    public String colType(RelNode rel, int column) {
      for (;;) {
        try {
          return colTypeHandler.getColType(rel, this, column);
        } catch (JaninoRelMetadataProvider.NoHandler e) {
          colTypeHandler = revise(ColType.Handler.class);
        }
      }
    }
  }
}
