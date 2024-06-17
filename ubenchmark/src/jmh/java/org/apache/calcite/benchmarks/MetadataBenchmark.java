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
package org.apache.calcite.benchmarks;

import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.ProxyingMetadataHandlerProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.test.CalciteAssert;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A benchmark to compare metadata retrieval time for a complex query.
 *
 * <p>Compares metadata retrieval performance on a large query.
 */
@Fork(value = 1, jvmArgsPrepend = "-Xmx2048m")
@State(Scope.Benchmark)
@Measurement(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Threads(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class MetadataBenchmark {

  @Setup
  public void setup() throws SQLException {
    DriverManager.registerDriver(new Driver());
  }

  private void test(final Supplier<RelMetadataQuery> supplier) {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select \"store\".\"store_country\" as \"c0\",\n"
            + " \"time_by_day\".\"the_year\" as \"c1\",\n"
            + " \"product_class\".\"product_family\" as \"c2\",\n"
            + " count(\"sales_fact_1997\".\"product_id\") as \"m0\"\n"
            + "from \"store\" as \"store\",\n"
            + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
            + " \"time_by_day\" as \"time_by_day\",\n"
            + " \"product_class\" as \"product_class\",\n"
            + " \"product\" as \"product\"\n"
            + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
            + "and \"store\".\"store_country\" = 'USA'\n"
            + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
            + "and \"time_by_day\".\"the_year\" = 1997\n"
            + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
            + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
            + "group by \"store\".\"store_country\",\n"
            + " \"time_by_day\".\"the_year\",\n"
            + " \"product_class\".\"product_family\"")
        .withHook(Hook.CONVERTED, (Consumer<RelNode>) rel -> {
          rel.getCluster().setMetadataQuerySupplier(supplier);
          rel.getCluster().invalidateMetadataQuery();
        })
        .explainContains(""
            + "EnumerableAggregate(group=[{1, 6, 10}], m0=[COUNT()])\n"
            + "  EnumerableMergeJoin(condition=[=($2, $8)], joinType=[inner])\n"
            + "    EnumerableSort(sort0=[$2], dir0=[ASC])\n"
            + "      EnumerableMergeJoin(condition=[=($3, $5)], joinType=[inner])\n"
            + "        EnumerableSort(sort0=[$3], dir0=[ASC])\n"
            + "          EnumerableHashJoin(condition=[=($0, $4)], joinType=[inner])\n"
            + "            EnumerableCalc(expr#0..23=[{inputs}], expr#24=['USA':VARCHAR(30)], "
            + "expr#25=[=($t9, $t24)], store_id=[$t0], store_country=[$t9], $condition=[$t25])\n"
            + "              EnumerableTableScan(table=[[foodmart2, store]])\n"
            + "            EnumerableCalc(expr#0..7=[{inputs}], proj#0..1=[{exprs}], "
            + "store_id=[$t4])\n"
            + "              EnumerableTableScan(table=[[foodmart2, sales_fact_1997]])\n"
            + "        EnumerableCalc(expr#0..9=[{inputs}], expr#10=[CAST($t4):INTEGER], "
            + "expr#11=[1997], expr#12=[=($t10, $t11)], time_id=[$t0], the_year=[$t4], "
            + "$condition=[$t12])\n"
            + "          EnumerableTableScan(table=[[foodmart2, time_by_day]])\n"
            + "    EnumerableHashJoin(condition=[=($0, $2)], joinType=[inner])\n"
            + "      EnumerableCalc(expr#0..14=[{inputs}], proj#0..1=[{exprs}])\n"
            + "        EnumerableTableScan(table=[[foodmart2, product]])\n"
            + "      EnumerableCalc(expr#0..4=[{inputs}], product_class_id=[$t0], "
            + "product_family=[$t4])\n"
            + "        EnumerableTableScan(table=[[foodmart2, product_class]])")
        .returns("c0=USA; c1=1997; c2=Non-Consumable; m0=16414\n"
            + "c0=USA; c1=1997; c2=Drink; m0=7978\n"
            + "c0=USA; c1=1997; c2=Food; m0=62445\n");
  }

  @Benchmark
  public void janino() {
    test(RelMetadataQuery::instance);
  }

  @Benchmark
  public void janinoWithCompile() {
    JaninoRelMetadataProvider.clearStaticCache();
    test(() ->
        new RelMetadataQuery(JaninoRelMetadataProvider.of(DefaultRelMetadataProvider.INSTANCE)));
  }

  @Benchmark
  public void proxying() {
    test(
        () -> new RelMetadataQuery(
            new ProxyingMetadataHandlerProvider(
        DefaultRelMetadataProvider.INSTANCE)));
  }

}
