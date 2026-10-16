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
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CursorInputs;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.util.CursorInput;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.util.BuiltInMethod;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Implementation of {@link org.apache.calcite.rel.core.TableFunctionScan} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableTableFunctionScan extends TableFunctionScan
    implements EnumerableRel {

  public EnumerableTableFunctionScan(RelOptCluster cluster,
      RelTraitSet traits, List<RelNode> inputs, @Nullable Type elementType,
      RelDataType rowType, RexNode call,
      @Nullable Set<RelColumnMapping> columnMappings) {
    super(cluster, traits, inputs, call, elementType, rowType,
        columnMappings);
  }

  @Override public EnumerableTableFunctionScan copy(
      RelTraitSet traitSet,
      List<RelNode> inputs,
      RexNode rexCall,
      @Nullable Type elementType,
      RelDataType rowType,
      @Nullable Set<RelColumnMapping> columnMappings) {
    return new EnumerableTableFunctionScan(getCluster(), traitSet, inputs,
        elementType, rowType, rexCall, columnMappings);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    if (isImplementorDefined((RexCall) getCall())) {
      return tvfImplementorBasedImplement(implementor, pref);
    } else {
      return defaultTableFunctionImplement(implementor, pref);
    }
  }

  private static boolean isImplementorDefined(RexCall call) {
    if (call.getOperator() instanceof SqlWindowTableFunction
        && RexImpTable.INSTANCE.get((SqlWindowTableFunction) call.getOperator()) != null) {
      return true;
    }
    return false;
  }

  private boolean isQueryable() {
    if (!(getCall() instanceof RexCall)) {
      return false;
    }
    final RexCall call = (RexCall) getCall();
    if (!(call.getOperator() instanceof SqlUserDefinedTableFunction)) {
      return false;
    }
    final SqlUserDefinedTableFunction udtf =
        (SqlUserDefinedTableFunction) call.getOperator();
    if (!(udtf.getFunction() instanceof TableFunctionImpl)) {
      return false;
    }
    final TableFunctionImpl tableFunction =
        (TableFunctionImpl) udtf.getFunction();
    final Method method = tableFunction.method;
    return QueryableTable.class.isAssignableFrom(method.getReturnType());
  }

  private Result defaultTableFunctionImplement(
      EnumerableRelImplementor implementor,
      @SuppressWarnings("unused") Prefer pref) { // TODO: remove or use
    BlockBuilder bb = new BlockBuilder();
    // Non-array user-specified types are not supported yet
    final JavaRowFormat format;
    Type elementType = getElementType();
    if (elementType == null) {
      format = JavaRowFormat.ARRAY;
    } else if (getRowType().getFieldCount() == 1 && isQueryable()) {
      format = JavaRowFormat.SCALAR;
    } else if (elementType instanceof Class
        && Object[].class.isAssignableFrom((Class<?>) elementType)) {
      format = JavaRowFormat.ARRAY;
    } else {
      format = JavaRowFormat.CUSTOM;
    }
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), format,
            false);
    final List<Expression> cursors = new ArrayList<>();
    for (int i = 0; i < getInputs().size(); i++) {
      final EnumerableRel child = (EnumerableRel) getInputs().get(i);
      final Result result = implementor.visitChild(this, i, child, Prefer.ARRAY);
      final Expression rows = bb.append("cursorRows", result.block);
      cursors.add(
          bb.append("cursor",
          Expressions.call(CursorInputs.class, "of",
              implementor.stash(child.getRowType(), RelDataType.class),
              result.physType.convertTo(rows, JavaRowFormat.ARRAY))));
    }
    final boolean resultSetCursors = !cursors.isEmpty()
        && ((RexCall) getCall()).getOperator() instanceof SqlUserDefinedTableFunction
        && ((SqlUserDefinedTableFunction) ((RexCall) getCall()).getOperator())
            .getFunction() instanceof TableFunctionImpl;
    final BlockBuilder callBuilder = resultSetCursors ? new BlockBuilder() : bb;
    final ParameterExpression resultSets = Expressions.parameter(ResultSet[].class, "cursors");
    RexToLixTranslator t =
        RexToLixTranslator.forAggregation(
            (JavaTypeFactory) getCluster().getTypeFactory(),
            callBuilder, (list, index, storageType) -> resultSetCursors
                ? Expressions.arrayIndex(resultSets, Expressions.constant(index))
                : cursors.get(index),
            implementor.getConformance(), implementor.getRexImplementorTable());
    t = t.setCorrelates(implementor.allCorrelateVariables);
    callBuilder.add(Expressions.return_(null, t.translate(getCall())));
    if (resultSetCursors) {
      final List<List<ColumnMetaData>> columns = new ArrayList<>();
      for (RelNode input : getInputs()) {
        columns.add(
            CalcitePrepareImpl.getColumnMetaDataList(
            implementor.getTypeFactory(), input.getRowType()));
      }
      bb.add(
          Expressions.return_(null,
          Expressions.call(CursorInputs.class, "enumerable",
              Expressions.newArrayInit(CursorInput.class, cursors),
              implementor.stash(columns, List.class),
              Expressions.call(BuiltInMethod.TIME_ZONE.method, DataContext.ROOT),
              Expressions.lambda(Function1.class, callBuilder.toBlock(), resultSets))));
    }
    return implementor.result(physType, bb.toBlock());
  }

  private Result tvfImplementorBasedImplement(
      EnumerableRelImplementor implementor, Prefer pref) {
    final JavaTypeFactory typeFactory = implementor.getTypeFactory();
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) getInputs().get(0);
    final Result result =
        implementor.visitChild(this, 0, child, pref);
    final PhysType physType =
        PhysTypeImpl.of(typeFactory, getRowType(), pref.prefer(result.format));
    final Expression inputEnumerable =
        builder.append("_input", result.block, false);
    final SqlConformance conformance =
        (SqlConformance) implementor.map.getOrDefault("_conformance",
            SqlConformanceEnum.DEFAULT);

    builder.add(
        RexToLixTranslator.translateTableFunction(
            typeFactory,
            conformance,
            builder,
            DataContext.ROOT,
            (RexCall) getCall(),
            inputEnumerable,
            result.physType,
            physType));

    return implementor.result(physType, builder.toBlock());
  }
}
