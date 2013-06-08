/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl.mongodb;

import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.Functions;
import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRelImpl;
import org.eigenbase.relopt.*;
import org.eigenbase.util.Pair;

import java.util.List;

/**
 * Relational expression representing a scan of a table in a Mongo data source.
 */
public class MongoToEnumerableConverter
    extends ConverterRelImpl
    implements EnumerableRel
{
  private final PhysType physType;

  protected MongoToEnumerableConverter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.instance, traits, input);
    this.physType =
        PhysTypeImpl.of(
            (JavaTypeFactory) cluster.getTypeFactory(),
            getRowType(),
            (EnumerableConvention) getConvention());
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new MongoToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  public PhysType getPhysType() {
    return physType;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(.1);
  }

  public BlockExpression implement(EnumerableRelImplementor implementor) {
    // Generates a call to "find" or "aggregate", depending upon whether
    // an aggregate is present.
    //
    //   ((MongoTable) schema.getTable("zips")).find(
    //     "{state: 'CA'}",
    //     "{city: 1, zipcode: 1}")
    //
    //   ((MongoTable) schema.getTable("zips")).aggregate(
    //     "{$filter: {state: 'CA'}}",
    //     "{$group: {_id: '$city', c: {$sum: 1}, p: {$sum: "$pop"}}")
    final BlockBuilder list = new BlockBuilder();
    final MongoRel.Implementor mongoImplementor = new MongoRel.Implementor();
    mongoImplementor.visitChild(0, getChild());
    int aggCount = 0;
    int findCount = 0;
    String project = null;
    String filter = null;
    for (Pair<String, String> op : mongoImplementor.list) {
      if (op.left == null) {
        ++aggCount;
      }
      if (op.right.startsWith("{$match:")) {
        filter = op.left;
        ++findCount;
      }
      if (op.right.startsWith("{$project:")) {
        project = op.left;
        ++findCount;
      }
    }
    final Expression fields =
        list.append("fields", constantArrayList(getRowType().getFieldNames()));
    final Expression table =
        list.append("table", mongoImplementor.table.getExpression());
    final Expression enumerable;
    if (aggCount == 0 && findCount <= 2) {
      enumerable =
          list.append("enumerable",
              Expressions.call(
                  table, "find",
                  Expressions.constant(filter, String.class),
                  Expressions.constant(project, String.class),
                  fields));
    } else {
      final Expression ops =
          list.append("ops",
              constantArrayList(Pair.right(mongoImplementor.list)));
      enumerable =
          list.append("enumerable",
              Expressions.call(
                  table, "aggregate", fields, ops));
    }
    list.add(
        Expressions.return_(null, enumerable));
    return list.toBlock();
  }

  /** E.g. {@code constantArrayList("x", "y")} returns
   * "Arrays.asList('x', 'y')". */
  private static <T> MethodCallExpression constantArrayList(List<T> values) {
    return Expressions.call(
        BuiltinMethod.ARRAYS_AS_LIST.method,
        Expressions.newArrayInit(String.class, constantList(values)));
  }

  /** E.g. {@code constantList("x", "y")} returns
   * {@code {ConstantExpression("x"), ConstantExpression("y")}}. */
  private static <T> List<Expression> constantList(List<T> values) {
    return Functions.apply(values,
        new Function1<T, Expression>() {
          public Expression apply(T a0) {
            return Expressions.constant(a0);
          }
        });
  }
}

// End MongoToEnumerableConverter.java
