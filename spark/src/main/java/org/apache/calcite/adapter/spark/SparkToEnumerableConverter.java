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
package org.apache.calcite.adapter.spark;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.validate.SqlConformance;

import java.util.List;

/**
 * Relational expression that converts input of
 * {@link org.apache.calcite.adapter.spark.SparkRel#CONVENTION Spark convention}
 * into {@link org.apache.calcite.adapter.enumerable.EnumerableConvention}.
 *
 * <p>Concretely, this means calling the
 * {@link org.apache.spark.api.java.JavaRDD#collect()} method of an RDD
 * and converting it to enumerable.</p>
 */
public class SparkToEnumerableConverter
    extends ConverterImpl
    implements EnumerableRel {
  protected SparkToEnumerableConverter(RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SparkToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.01);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // Generate:
    //   RDD rdd = ...;
    //   return SparkRuntime.asEnumerable(rdd);
    final BlockBuilder list = new BlockBuilder();
    final SparkRel child = (SparkRel) getInput();
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(),
            getRowType(),
            JavaRowFormat.CUSTOM);
    SparkRel.Implementor sparkImplementor =
        new SparkImplementorImpl(implementor);
    final SparkRel.Result result = child.implementSpark(sparkImplementor);
    final Expression rdd = list.append("rdd", result.block);
    final Expression enumerable =
        list.append(
            "enumerable",
            Expressions.call(
                SparkMethod.AS_ENUMERABLE.method,
                rdd));
    list.add(
        Expressions.return_(null, enumerable));
    return implementor.result(physType, list.toBlock());
  }

  /** Implementation of
   * {@link org.apache.calcite.adapter.spark.SparkRel.Implementor}. */
  private static class SparkImplementorImpl extends SparkRel.Implementor {
    private final EnumerableRelImplementor implementor;

    SparkImplementorImpl(EnumerableRelImplementor implementor) {
      super(implementor.getRexBuilder());
      this.implementor = implementor;
    }

    public SparkRel.Result result(PhysType physType,
        BlockStatement blockStatement) {
      return new SparkRel.Result(physType, blockStatement);
    }

    SparkRel.Result visitInput(SparkRel parent, int ordinal, SparkRel input) {
      if (parent != null) {
        assert input == parent.getInputs().get(ordinal);
      }
      return input.implementSpark(this);
    }

    public JavaTypeFactory getTypeFactory() {
      return implementor.getTypeFactory();
    }

    public SqlConformance getConformance() {
      return implementor.getConformance();
    }
  }
}

// End SparkToEnumerableConverter.java
