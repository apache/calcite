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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.interpreter.Sink;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;

import java.util.List;

/**
 * Relational expression that converts an enumerable input to interpretable
 * calling convention.
 *
 * @see org.apache.calcite.adapter.enumerable.EnumerableConvention
 * @see org.apache.calcite.interpreter.BindableConvention
 */
public class EnumerableBindable extends ConverterImpl implements BindableRel {
  protected EnumerableBindable(RelOptCluster cluster, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE,
        cluster.traitSetOf(BindableConvention.INSTANCE), input);
  }

  @Override public EnumerableBindable copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return new EnumerableBindable(getCluster(), sole(inputs));
  }

  public Class<Object[]> getElementType() {
    return Object[].class;
  }

  public Enumerable<Object[]> bind(DataContext dataContext) {
    final ImmutableMap<String, Object> map = ImmutableMap.of();
    final Bindable bindable = EnumerableInterpretable.toBindable(map, null,
        (EnumerableRel) getInput(), EnumerableRel.Prefer.ARRAY);
    final ArrayBindable arrayBindable = EnumerableInterpretable.box(bindable);
    return arrayBindable.bind(dataContext);
  }

  public Node implement(final InterpreterImplementor implementor) {
    return new Node() {
      public void run() throws InterruptedException {
        final Sink sink =
            implementor.relSinks.get(EnumerableBindable.this).get(0);
        final Enumerable<Object[]> enumerable = bind(implementor.dataContext);
        final Enumerator<Object[]> enumerator = enumerable.enumerator();
        while (enumerator.moveNext()) {
          sink.send(Row.asCopy(enumerator.current()));
        }
      }
    };
  }

  /**
   * Rule that converts any enumerable relational expression to bindable.
   */
  public static class EnumerableToBindableConverterRule extends ConverterRule {
    public static final EnumerableToBindableConverterRule INSTANCE =
        new EnumerableToBindableConverterRule(RelFactories.LOGICAL_BUILDER);

    /**
     * Creates an EnumerableToBindableConverterRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public EnumerableToBindableConverterRule(RelBuilderFactory relBuilderFactory) {
      super(EnumerableRel.class, Predicates.<RelNode>alwaysTrue(), EnumerableConvention.INSTANCE,
          BindableConvention.INSTANCE, relBuilderFactory, "EnumerableToBindableConverterRule");
    }

    @Override public RelNode convert(RelNode rel) {
      return new EnumerableBindable(rel.getCluster(), rel);
    }
  }
}

// End EnumerableBindable.java
