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

import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.util.BuiltInMethod;

/**
 * Implementation of {@link Spool} in
 * {@link EnumerableConvention enumerable calling convention}
 * that writes into a {@link ModifiableTable}
 *
 * <p>NOTE: The current API is experimental and subject to change without notice.</p>
 */
@Experimental
public class EnumerableTableSpool extends Spool implements EnumerableRel {

  // table name, it must refer to an existing ModifiableTable
  protected final String tableName;

  protected EnumerableTableSpool(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                                 Type readType, Type writeType, String tableName) {
    super(cluster, traitSet, input, readType, writeType);
    this.tableName = tableName;
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // TODO for the moment only LAZY read & write is supported
    if (!(readType == Type.LAZY && writeType == Type.LAZY)) {
      throw new UnsupportedOperationException(
          "EnumerableTableSpool supports for the moment only LAZY read and LAZY write");
    }

    //  ModifiableTable t = (ModifiableTable) root.getRootSchema().getTable(tableName);
    //  return lazyCollectionSpool(t.getModifiableCollection(), <inputExp>);

    BlockBuilder builder = new BlockBuilder();

    RelNode input = getInput();
    Result inputResult = implementor.visitChild(this, 0, (EnumerableRel) input, pref);

    Expression tableExp = Expressions.convert_(
        Expressions.call(
            Expressions.call(
                implementor.getRootExpression(),
                BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method),
            BuiltInMethod.SCHEMA_GET_TABLE.method,
            Expressions.constant(tableName, String.class)),
        ModifiableTable.class);
    Expression collectionExp = Expressions.call(
        tableExp,
        BuiltInMethod.MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION.method);

    Expression inputExp = builder.append("input", inputResult.block);

    Expression spoolExp = Expressions.call(
        BuiltInMethod.LAZY_COLLECTION_SPOOL.method,
        collectionExp,
        inputExp);
    builder.add(spoolExp);

    PhysType physType = PhysTypeImpl.of(
        implementor.getTypeFactory(),
        getRowType(),
        pref.prefer(inputResult.format));
    return implementor.result(physType, builder.toBlock());
  }

  @Override protected Spool copy(RelTraitSet traitSet, RelNode input,
                                 Type readType, Type writeType) {
    return new EnumerableTableSpool(input.getCluster(), traitSet, input,
        readType, writeType, tableName);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    return pw.item("tableName", tableName);
  }
}

// End EnumerableTableSpool.java
