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
package net.hydromatic.optiq.impl.spark;

import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.optiq.rules.java.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRelImpl;
import org.eigenbase.relopt.*;

import java.util.List;

/**
 * Relational expression that converts input of {@link EnumerableConvention}
 * into {@link SparkRel#CONVENTION Spark convention}.
 *
 * <p>Concretely, this means iterating over the contents of an
 * {@link net.hydromatic.linq4j.Enumerable}, storing them in a list, and
 * building an {@link org.apache.spark.rdd.RDD} on top of it.</p>
 */
public class EnumerableToSparkConverter
    extends ConverterRelImpl
    implements SparkRel {
  protected EnumerableToSparkConverter(RelOptCluster cluster,
      RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableToSparkConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(.01);
  }

  public Result implementSpark(Implementor implementor) {
    // Generate:
    //   Enumerable source = ...;
    //   return SparkRuntime.createRdd(sparkContext, source);
    final BlockBuilder list = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) getChild();
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(), getRowType(),
            JavaRowFormat.CUSTOM);
    final Expression source = null; // TODO:
    final Expression sparkContext =
        Expressions.call(
            SparkMethod.GET_SPARK_CONTEXT.method,
            implementor.getRootExpression());
    final Expression rdd =
        list.append(
            "rdd",
            Expressions.call(
                SparkMethod.CREATE_RDD.method,
                sparkContext,
                source));
    list.add(
        Expressions.return_(null, rdd));
    return implementor.result(physType, list.toBlock());
  }
}

// End EnumerableToSparkConverter.java
