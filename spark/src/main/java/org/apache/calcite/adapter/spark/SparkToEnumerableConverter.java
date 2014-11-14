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

import net.hydromatic.linq4j.expressions.BlockBuilder;
import net.hydromatic.linq4j.expressions.BlockStatement;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRelImpl;
import org.eigenbase.relopt.*;

import java.util.List;

/**
 * Relational expression that converts input of
 * {@link net.hydromatic.optiq.impl.spark.SparkRel#CONVENTION Spark convention}
 * into {@link net.hydromatic.optiq.rules.java.EnumerableConvention}.
 *
 * <p>Concretely, this means calling the
 * {@link org.apache.spark.api.java.JavaRDD#collect()} method of an RDD
 * and converting it to enumerable.</p>
 */
public class SparkToEnumerableConverter
    extends ConverterRelImpl
    implements EnumerableRel {
  protected SparkToEnumerableConverter(RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SparkToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(.01);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // Generate:
    //   RDD rdd = ...;
    //   return SparkRuntime.asEnumerable(rdd);
    final BlockBuilder list = new BlockBuilder();
    final SparkRel child = (SparkRel) getChild();
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
   * {@link net.hydromatic.optiq.impl.spark.SparkRel.Implementor}. */
  private static class SparkImplementorImpl extends SparkRel.Implementor {
    private final EnumerableRelImplementor implementor;

    public SparkImplementorImpl(EnumerableRelImplementor implementor) {
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
      createFrame(parent, ordinal, input);
      return input.implementSpark(this);
    }

    public JavaTypeFactory getTypeFactory() {
      return implementor.getTypeFactory();
    }
  }
}

// End SparkToEnumerableConverter.java
