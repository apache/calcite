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
package org.apache.calcite.adapter.pig;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;

/**
 * Relational expression representing a scan of a table in a Pig data source.
 */
public class PigToEnumerableConverter
    extends ConverterImpl
    implements EnumerableRel {
  /** Creates a PigToEnumerableConverter. */
  protected PigToEnumerableConverter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new PigToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation does not actually execute the associated Pig Latin
   * script and return results. Instead it returns an empty
   * {@link org.apache.calcite.adapter.enumerable.EnumerableRel.Result}
   * in order to allow for testing and verification of every step of query
   * processing up to actual physical execution and result verification.
   *
   * <p>Next step is to invoke Pig from here, likely in local mode, have it
   * store results in a predefined file so they can be read here and returned as
   * a {@code Result} object.
   */
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder list = new BlockBuilder();
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), rowType,
            pref.prefer(JavaRowFormat.ARRAY));
    PigRel.Implementor impl = new PigRel.Implementor();
    impl.visitChild(0, getInput());
    Hook.QUERY_PLAN.run(impl.getScript()); // for script validation in tests
    list.add(
        Expressions.return_(null,
            Expressions.call(
                BuiltInMethod.EMPTY_ENUMERABLE.method)));
    return implementor.result(physType, list.toBlock());
  }
}

// End PigToEnumerableConverter.java
