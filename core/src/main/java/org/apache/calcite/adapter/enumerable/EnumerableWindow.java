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

import org.apache.calcite.adapter.enumerable.impl.WinAggAddContextImpl;
import org.apache.calcite.adapter.enumerable.impl.WinAggResetContextImpl;
import org.apache.calcite.adapter.enumerable.impl.WinAggResultContextImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.tree.BinaryExpression;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.DeclarationStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.runtime.SortedMultiMap;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/** Implementation of {@link org.apache.calcite.rel.core.Window} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableWindow extends Window implements EnumerableRel {
  /** Creates an EnumerableWindowRel. */
  EnumerableWindow(RelOptCluster cluster, RelTraitSet traits, RelNode child,
      List<RexLiteral> constants, RelDataType rowType, List<Group> groups) {
    super(cluster, traits, child, constants, rowType, groups);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableWindow(getCluster(), traitSet, sole(inputs),
        constants, rowType, groups);
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq)
        .multiplyBy(EnumerableConvention.COST_MULTIPLIER);
  }

  /** Implementation of {@link RexToLixTranslator.InputGetter}
   * suitable for generating implementations of windowed aggregate
   * functions. */
  private static class WindowRelInputGetter
      implements RexToLixTranslator.InputGetter {
    private final Expression row;
    private final PhysType rowPhysType;
    private final int actualInputFieldCount;
    private final List<Expression> constants;

    private WindowRelInputGetter(Expression row,
        PhysType rowPhysType, int actualInputFieldCount,
        List<Expression> constants) {
      this.row = row;
      this.rowPhysType = rowPhysType;
      this.actualInputFieldCount = actualInputFieldCount;
      this.constants = constants;
    }

    public Expression field(BlockBuilder list, int index, Type storageType) {
      if (index < actualInputFieldCount) {
        Expression current = list.append("current", row);
        return rowPhysType.fieldReference(current, index, storageType);
      }
      return constants.get(index - actualInputFieldCount);
    }
  }

  private void sampleOfTheGeneratedWindowedAggregate() {
    // Here's overview of the generated code
    // For each list of rows that have the same partitioning key, evaluate
    // all of the windowed aggregate functions.

    // builder
    Iterator<Integer[]> iterator = null;

    // builder3
    Integer[] rows = iterator.next();

    int prevStart = -1;
    int prevEnd = -1;

    for (int i = 0; i < rows.length; i++) {
      // builder4
      Integer row = rows[i];

      int start = 0;
      int end = 100;
      if (start != prevStart || end != prevEnd) {
        // builder5
        int actualStart = 0;
        if (start != prevStart || end < prevEnd) {
          // builder6
          // recompute
          actualStart = start;
          // implementReset
        } else { // must be start == prevStart && end > prevEnd
          actualStart = prevEnd + 1;
        }
        prevStart = start;
        prevEnd = end;

        if (start != -1) {
          for (int j = actualStart; j <= end; j++) {
            // builder7
            // implementAdd
          }
        }
        // implementResult
        // list.add(new Xxx(row.deptno, row.empid, sum, count));
      }
    }
    // multiMap.clear(); // allows gc
    // source = Linq4j.asEnumerable(list);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final JavaTypeFactory typeFactory = implementor.getTypeFactory();
    final EnumerableRel child = (EnumerableRel) getInput();
    final BlockBuilder builder = new BlockBuilder();
    final Result result = implementor.visitChild(this, 0, child, pref);
    Expression source_ = builder.append("source", result.block);

    final List<Expression> translatedConstants =
        new ArrayList<>(constants.size());
    for (RexLiteral constant : constants) {
      translatedConstants.add(
          RexToLixTranslator.translateLiteral(constant, constant.getType(),
              typeFactory, RexImpTable.NullAs.NULL));
    }

    PhysType inputPhysType = result.physType;

    ParameterExpression prevStart =
        Expressions.parameter(int.class, builder.newName("prevStart"));
    ParameterExpression prevEnd =
        Expressions.parameter(int.class, builder.newName("prevEnd"));

    builder.add(Expressions.declare(0, prevStart, null));
    builder.add(Expressions.declare(0, prevEnd, null));

    for (int windowIdx = 0; windowIdx < groups.size(); windowIdx++) {
      Group group = groups.get(windowIdx);
      // Comparator:
      // final Comparator<JdbcTest.Employee> comparator =
      //    new Comparator<JdbcTest.Employee>() {
      //      public int compare(JdbcTest.Employee o1,
      //          JdbcTest.Employee o2) {
      //        return Integer.compare(o1.empid, o2.empid);
      //      }
      //    };
      final Expression comparator_ =
          builder.append(
              "comparator",
              inputPhysType.generateComparator(
                  group.collation()));

      Pair<Expression, Expression> partitionIterator =
          getPartitionIterator(builder, source_, inputPhysType, group,
              comparator_);
      final Expression collectionExpr = partitionIterator.left;
      final Expression iterator_ = partitionIterator.right;

      List<AggImpState> aggs = new ArrayList<>();
      List<AggregateCall> aggregateCalls = group.getAggregateCalls(this);
      for (int aggIdx = 0; aggIdx < aggregateCalls.size(); aggIdx++) {
        AggregateCall call = aggregateCalls.get(aggIdx);
        if (call.ignoreNulls()) {
          throw new UnsupportedOperationException("IGNORE NULLS not supported");
        }
        aggs.add(new AggImpState(aggIdx, call, true));
      }

      // The output from this stage is the input plus the aggregate functions.
      final RelDataTypeFactory.Builder typeBuilder = typeFactory.builder();
      typeBuilder.addAll(inputPhysType.getRowType().getFieldList());
      for (AggImpState agg : aggs) {
        typeBuilder.add(agg.call.name, agg.call.type);
      }
      RelDataType outputRowType = typeBuilder.build();
      final PhysType outputPhysType =
          PhysTypeImpl.of(
              typeFactory, outputRowType, pref.prefer(result.format));

      final Expression list_ =
          builder.append(
              "list",
              Expressions.new_(
                  ArrayList.class,
                  Expressions.call(
                      collectionExpr, BuiltInMethod.COLLECTION_SIZE.method)),
              false);

      Pair<Expression, Expression> collationKey =
          getRowCollationKey(builder, inputPhysType, group, windowIdx);
      Expression keySelector = collationKey.left;
      Expression keyComparator = collationKey.right;
      final BlockBuilder builder3 = new BlockBuilder();
      final Expression rows_ =
          builder3.append(
              "rows",
              Expressions.convert_(
                  Expressions.call(
                      iterator_, BuiltInMethod.ITERATOR_NEXT.method),
                  Object[].class),
              false);

      builder3.add(
          Expressions.statement(
              Expressions.assign(prevStart, Expressions.constant(-1))));
      builder3.add(
          Expressions.statement(
              Expressions.assign(prevEnd,
                  Expressions.constant(Integer.MAX_VALUE))));

      final BlockBuilder builder4 = new BlockBuilder();

      final ParameterExpression i_ =
          Expressions.parameter(int.class, builder4.newName("i"));

      final Expression row_ =
          builder4.append(
              "row",
              RexToLixTranslator.convert(
                  Expressions.arrayIndex(rows_, i_),
                  inputPhysType.getJavaRowType()));

      final RexToLixTranslator.InputGetter inputGetter =
          new WindowRelInputGetter(row_, inputPhysType,
              result.physType.getRowType().getFieldCount(),
              translatedConstants);

      final RexToLixTranslator translator =
          RexToLixTranslator.forAggregation(typeFactory, builder4,
              inputGetter, implementor.getConformance());

      final List<Expression> outputRow = new ArrayList<>();
      int fieldCountWithAggResults =
          inputPhysType.getRowType().getFieldCount();
      for (int i = 0; i < fieldCountWithAggResults; i++) {
        outputRow.add(
            inputPhysType.fieldReference(
                row_, i,
                outputPhysType.getJavaFieldType(i)));
      }

      declareAndResetState(typeFactory, builder, result, windowIdx, aggs,
          outputPhysType, outputRow);

      // There are assumptions that minX==0. If ever change this, look for
      // frameRowCount, bounds checking, etc
      final Expression minX = Expressions.constant(0);
      final Expression partitionRowCount =
          builder3.append("partRows", Expressions.field(rows_, "length"));
      final Expression maxX = builder3.append("maxX",
          Expressions.subtract(
              partitionRowCount, Expressions.constant(1)));

      final Expression startUnchecked = builder4.append("start",
          translateBound(translator, i_, row_, minX, maxX, rows_,
              group, true,
              inputPhysType, comparator_, keySelector, keyComparator));
      final Expression endUnchecked = builder4.append("end",
          translateBound(translator, i_, row_, minX, maxX, rows_,
              group, false,
              inputPhysType, comparator_, keySelector, keyComparator));

      final Expression startX;
      final Expression endX;
      final Expression hasRows;
      if (group.isAlwaysNonEmpty()) {
        startX = startUnchecked;
        endX = endUnchecked;
        hasRows = Expressions.constant(true);
      } else {
        Expression startTmp =
            group.lowerBound.isUnbounded() || startUnchecked == i_
                ? startUnchecked
                : builder4.append("startTmp",
                    Expressions.call(null, BuiltInMethod.MATH_MAX.method,
                        startUnchecked, minX));
        Expression endTmp =
            group.upperBound.isUnbounded() || endUnchecked == i_
                ? endUnchecked
                : builder4.append("endTmp",
                    Expressions.call(null, BuiltInMethod.MATH_MIN.method,
                        endUnchecked, maxX));

        ParameterExpression startPe = Expressions.parameter(0, int.class,
            builder4.newName("startChecked"));
        ParameterExpression endPe = Expressions.parameter(0, int.class,
            builder4.newName("endChecked"));
        builder4.add(Expressions.declare(Modifier.FINAL, startPe, null));
        builder4.add(Expressions.declare(Modifier.FINAL, endPe, null));

        hasRows = builder4.append("hasRows",
            Expressions.lessThanOrEqual(startTmp, endTmp));
        builder4.add(
            Expressions.ifThenElse(hasRows,
                Expressions.block(
                    Expressions.statement(
                        Expressions.assign(startPe, startTmp)),
                    Expressions.statement(
                      Expressions.assign(endPe, endTmp))),
            Expressions.block(
                Expressions.statement(
                    Expressions.assign(startPe, Expressions.constant(-1))),
                Expressions.statement(
                    Expressions.assign(endPe, Expressions.constant(-1))))));
        startX = startPe;
        endX = endPe;
      }

      final BlockBuilder builder5 = new BlockBuilder(true, builder4);

      BinaryExpression rowCountWhenNonEmpty = Expressions.add(
          startX == minX ? endX : Expressions.subtract(endX, startX),
          Expressions.constant(1));

      final Expression frameRowCount;

      if (hasRows.equals(Expressions.constant(true))) {
        frameRowCount =
            builder4.append("totalRows", rowCountWhenNonEmpty);
      } else {
        frameRowCount =
            builder4.append("totalRows",
                Expressions.condition(hasRows, rowCountWhenNonEmpty,
                    Expressions.constant(0)));
      }

      ParameterExpression actualStart = Expressions.parameter(
          0, int.class, builder5.newName("actualStart"));

      final BlockBuilder builder6 = new BlockBuilder(true, builder5);
      builder6.add(
          Expressions.statement(Expressions.assign(actualStart, startX)));

      for (final AggImpState agg : aggs) {
        agg.implementor.implementReset(agg.context,
            new WinAggResetContextImpl(builder6, agg.state, i_, startX, endX,
                hasRows, partitionRowCount, frameRowCount));
      }

      Expression lowerBoundCanChange =
          group.lowerBound.isUnbounded() && group.lowerBound.isPreceding()
          ? Expressions.constant(false)
          : Expressions.notEqual(startX, prevStart);
      Expression needRecomputeWindow = Expressions.orElse(
          lowerBoundCanChange,
          Expressions.lessThan(endX, prevEnd));

      BlockStatement resetWindowState = builder6.toBlock();
      if (resetWindowState.statements.size() == 1) {
        builder5.add(
            Expressions.declare(0, actualStart,
                Expressions.condition(needRecomputeWindow, startX,
                    Expressions.add(prevEnd, Expressions.constant(1)))));
      } else {
        builder5.add(
            Expressions.declare(0, actualStart, null));
        builder5.add(
            Expressions.ifThenElse(needRecomputeWindow,
                resetWindowState,
                Expressions.statement(
                    Expressions.assign(actualStart,
                    Expressions.add(prevEnd, Expressions.constant(1))))));
      }

      if (lowerBoundCanChange instanceof BinaryExpression) {
        builder5.add(
            Expressions.statement(Expressions.assign(prevStart, startX)));
      }
      builder5.add(
          Expressions.statement(Expressions.assign(prevEnd, endX)));

      final BlockBuilder builder7 = new BlockBuilder(true, builder5);
      final DeclarationStatement jDecl =
          Expressions.declare(0, "j", actualStart);

      final PhysType inputPhysTypeFinal = inputPhysType;
      final Function<BlockBuilder, WinAggFrameResultContext>
          resultContextBuilder =
          getBlockBuilderWinAggFrameResultContextFunction(typeFactory,
              implementor.getConformance(), result, translatedConstants,
              comparator_, rows_, i_, startX, endX, minX, maxX,
              hasRows, frameRowCount, partitionRowCount,
              jDecl, inputPhysTypeFinal);

      final Function<AggImpState, List<RexNode>> rexArguments = agg -> {
        List<Integer> argList = agg.call.getArgList();
        List<RelDataType> inputTypes =
            EnumUtils.fieldRowTypes(
                result.physType.getRowType(),
                constants,
                argList);
        List<RexNode> args = new ArrayList<>(inputTypes.size());
        for (int i = 0; i < argList.size(); i++) {
          Integer idx = argList.get(i);
          args.add(new RexInputRef(idx, inputTypes.get(i)));
        }
        return args;
      };

      implementAdd(aggs, builder7, resultContextBuilder, rexArguments, jDecl);

      BlockStatement forBlock = builder7.toBlock();
      if (!forBlock.statements.isEmpty()) {
        // For instance, row_number does not use for loop to compute the value
        Statement forAggLoop = Expressions.for_(
            Arrays.asList(jDecl),
            Expressions.lessThanOrEqual(jDecl.parameter, endX),
            Expressions.preIncrementAssign(jDecl.parameter),
            forBlock);
        if (!hasRows.equals(Expressions.constant(true))) {
          forAggLoop = Expressions.ifThen(hasRows, forAggLoop);
        }
        builder5.add(forAggLoop);
      }

      if (implementResult(aggs, builder5, resultContextBuilder, rexArguments,
              true)) {
        builder4.add(
            Expressions.ifThen(
                Expressions.orElse(lowerBoundCanChange,
                    Expressions.notEqual(endX, prevEnd)),
                builder5.toBlock()));
      }

      implementResult(aggs, builder4, resultContextBuilder, rexArguments,
          false);

      builder4.add(
          Expressions.statement(
              Expressions.call(
                  list_,
                  BuiltInMethod.COLLECTION_ADD.method,
                  outputPhysType.record(outputRow))));

      builder3.add(
          Expressions.for_(
              Expressions.declare(0, i_, Expressions.constant(0)),
              Expressions.lessThan(
                  i_,
                  Expressions.field(rows_, "length")),
              Expressions.preIncrementAssign(i_),
              builder4.toBlock()));

      builder.add(
          Expressions.while_(
              Expressions.call(
                  iterator_,
                  BuiltInMethod.ITERATOR_HAS_NEXT.method),
              builder3.toBlock()));
      builder.add(
          Expressions.statement(
              Expressions.call(
                  collectionExpr,
                  BuiltInMethod.MAP_CLEAR.method)));

      // We're not assigning to "source". For each group, create a new
      // final variable called "source" or "sourceN".
      source_ =
          builder.append(
              "source",
              Expressions.call(
                  BuiltInMethod.AS_ENUMERABLE.method, list_));

      inputPhysType = outputPhysType;
    }

    //   return Linq4j.asEnumerable(list);
    builder.add(
        Expressions.return_(null, source_));
    return implementor.result(inputPhysType, builder.toBlock());
  }

  private Function<BlockBuilder, WinAggFrameResultContext>
      getBlockBuilderWinAggFrameResultContextFunction(
      final JavaTypeFactory typeFactory, final SqlConformance conformance,
      final Result result, final List<Expression> translatedConstants,
      final Expression comparator_,
      final Expression rows_, final ParameterExpression i_,
      final Expression startX, final Expression endX,
      final Expression minX, final Expression maxX,
      final Expression hasRows, final Expression frameRowCount,
      final Expression partitionRowCount,
      final DeclarationStatement jDecl,
      final PhysType inputPhysType) {
    return block -> new WinAggFrameResultContext() {
      public RexToLixTranslator rowTranslator(Expression rowIndex) {
        Expression row =
            getRow(rowIndex);
        final RexToLixTranslator.InputGetter inputGetter =
            new WindowRelInputGetter(row, inputPhysType,
                result.physType.getRowType().getFieldCount(),
                translatedConstants);

        return RexToLixTranslator.forAggregation(typeFactory,
            block, inputGetter, conformance);
      }

      public Expression computeIndex(Expression offset,
          WinAggImplementor.SeekType seekType) {
        Expression index;
        if (seekType == WinAggImplementor.SeekType.AGG_INDEX) {
          index = jDecl.parameter;
        } else if (seekType == WinAggImplementor.SeekType.SET) {
          index = i_;
        } else if (seekType == WinAggImplementor.SeekType.START) {
          index = startX;
        } else if (seekType == WinAggImplementor.SeekType.END) {
          index = endX;
        } else {
          throw new IllegalArgumentException("SeekSet " + seekType
              + " is not supported");
        }
        if (!Expressions.constant(0).equals(offset)) {
          index = block.append("idx", Expressions.add(index, offset));
        }
        return index;
      }

      private Expression checkBounds(Expression rowIndex,
          Expression minIndex, Expression maxIndex) {
        if (rowIndex == i_ || rowIndex == startX || rowIndex == endX) {
          // No additional bounds check required
          return hasRows;
        }

        //noinspection UnnecessaryLocalVariable
        Expression res = block.append("rowInFrame",
            Expressions.foldAnd(
                ImmutableList.of(hasRows,
                    Expressions.greaterThanOrEqual(rowIndex, minIndex),
                    Expressions.lessThanOrEqual(rowIndex, maxIndex))));

        return res;
      }

      public Expression rowInFrame(Expression rowIndex) {
        return checkBounds(rowIndex, startX, endX);
      }

      public Expression rowInPartition(Expression rowIndex) {
        return checkBounds(rowIndex, minX, maxX);
      }

      public Expression compareRows(Expression a, Expression b) {
        return Expressions.call(comparator_,
            BuiltInMethod.COMPARATOR_COMPARE.method,
            getRow(a), getRow(b));
      }

      public Expression getRow(Expression rowIndex) {
        return block.append(
            "jRow",
            RexToLixTranslator.convert(
                Expressions.arrayIndex(rows_, rowIndex),
                inputPhysType.getJavaRowType()));
      }

      public Expression index() {
        return i_;
      }

      public Expression startIndex() {
        return startX;
      }

      public Expression endIndex() {
        return endX;
      }

      public Expression hasRows() {
        return hasRows;
      }

      public Expression getFrameRowCount() {
        return frameRowCount;
      }

      public Expression getPartitionRowCount() {
        return partitionRowCount;
      }
    };
  }

  private Pair<Expression, Expression> getPartitionIterator(
      BlockBuilder builder,
      Expression source_,
      PhysType inputPhysType,
      Group group,
      Expression comparator_) {
    // Populate map of lists, one per partition
    //   final Map<Integer, List<Employee>> multiMap =
    //     new SortedMultiMap<Integer, List<Employee>>();
    //    source.foreach(
    //      new Function1<Employee, Void>() {
    //        public Void apply(Employee v) {
    //          final Integer k = v.deptno;
    //          multiMap.putMulti(k, v);
    //          return null;
    //        }
    //      });
    //   final List<Xxx> list = new ArrayList<Xxx>(multiMap.size());
    //   Iterator<Employee[]> iterator = multiMap.arrays(comparator);
    //
    if (group.keys.isEmpty()) {
      // If partition key is empty, no need to partition.
      //
      //   final List<Employee> tempList =
      //       source.into(new ArrayList<Employee>());
      //   Iterator<Employee[]> iterator =
      //       SortedMultiMap.singletonArrayIterator(comparator, tempList);
      //   final List<Xxx> list = new ArrayList<Xxx>(tempList.size());

      final Expression tempList_ = builder.append(
          "tempList",
          Expressions.convert_(
              Expressions.call(
                  source_,
                  BuiltInMethod.INTO.method,
                  Expressions.new_(ArrayList.class)),
              List.class));
      return Pair.of(tempList_,
          builder.append(
            "iterator",
            Expressions.call(
                null,
                BuiltInMethod.SORTED_MULTI_MAP_SINGLETON.method,
                comparator_,
                tempList_)));
    }
    Expression multiMap_ =
        builder.append(
            "multiMap", Expressions.new_(SortedMultiMap.class));
    final BlockBuilder builder2 = new BlockBuilder();
    final ParameterExpression v_ =
        Expressions.parameter(inputPhysType.getJavaRowType(),
            builder2.newName("v"));

    Pair<Type, List<Expression>> selector =
        inputPhysType.selector(v_, group.keys.asList(), JavaRowFormat.CUSTOM);
    final ParameterExpression key_;
    if (selector.left instanceof Types.RecordType) {
      Types.RecordType keyJavaType = (Types.RecordType) selector.left;
      List<Expression> initExpressions = selector.right;
      key_ = Expressions.parameter(keyJavaType, "key");
      builder2.add(Expressions.declare(0, key_, null));
      builder2.add(
          Expressions.statement(
              Expressions.assign(key_, Expressions.new_(keyJavaType))));
      List<Types.RecordField> fieldList = keyJavaType.getRecordFields();
      for (int i = 0; i < initExpressions.size(); i++) {
        Expression right = initExpressions.get(i);
        builder2.add(
            Expressions.statement(
                Expressions.assign(
                    Expressions.field(key_, fieldList.get(i)), right)));
      }
    } else {
      DeclarationStatement declare =
          Expressions.declare(0, "key", selector.right.get(0));
      builder2.add(declare);
      key_ = declare.parameter;
    }
    builder2.add(
        Expressions.statement(
            Expressions.call(
                multiMap_,
                BuiltInMethod.SORTED_MULTI_MAP_PUT_MULTI.method,
                key_,
                v_)));
    builder2.add(
        Expressions.return_(
            null, Expressions.constant(null)));

    builder.add(
        Expressions.statement(
            Expressions.call(
                source_,
                BuiltInMethod.ENUMERABLE_FOREACH.method,
                Expressions.lambda(
                    builder2.toBlock(), v_))));

    return Pair.of(multiMap_,
      builder.append(
        "iterator",
        Expressions.call(
            multiMap_,
            BuiltInMethod.SORTED_MULTI_MAP_ARRAYS.method,
            comparator_)));
  }

  private Pair<Expression, Expression> getRowCollationKey(
      BlockBuilder builder, PhysType inputPhysType,
      Group group, int windowIdx) {
    if (!(group.isRows || (group.upperBound.isUnbounded()
        && group.lowerBound.isUnbounded()))) {
      Pair<Expression, Expression> pair =
          inputPhysType.generateCollationKey(
              group.collation().getFieldCollations());
      // optimize=false to prevent inlining of object create into for-loops
      return Pair.of(
          builder.append("keySelector" + windowIdx, pair.left, false),
          builder.append("keyComparator" + windowIdx, pair.right, false));
    } else {
      return Pair.of(null, null);
    }
  }

  private void declareAndResetState(final JavaTypeFactory typeFactory,
      BlockBuilder builder, final Result result, int windowIdx,
      List<AggImpState> aggs, PhysType outputPhysType,
      List<Expression> outputRow) {
    for (final AggImpState agg : aggs) {
      agg.context =
          new WinAggContext() {
            public SqlAggFunction aggregation() {
              return agg.call.getAggregation();
            }

            public RelDataType returnRelType() {
              return agg.call.type;
            }

            public Type returnType() {
              return EnumUtils.javaClass(typeFactory, returnRelType());
            }

            public List<? extends Type> parameterTypes() {
              return EnumUtils.fieldTypes(typeFactory,
                  parameterRelTypes());
            }

            public List<? extends RelDataType> parameterRelTypes() {
              return EnumUtils.fieldRowTypes(result.physType.getRowType(),
                  constants, agg.call.getArgList());
            }

            public List<ImmutableBitSet> groupSets() {
              throw new UnsupportedOperationException();
            }

            public List<Integer> keyOrdinals() {
              throw new UnsupportedOperationException();
            }

            public List<? extends RelDataType> keyRelTypes() {
              throw new UnsupportedOperationException();
            }

            public List<? extends Type> keyTypes() {
              throw new UnsupportedOperationException();
            }
          };
      String aggName = "a" + agg.aggIdx;
      if (CalciteSystemProperty.DEBUG.value()) {
        aggName = Util.toJavaId(agg.call.getAggregation().getName(), 0)
            .substring("ID$0$".length()) + aggName;
      }
      List<Type> state = agg.implementor.getStateType(agg.context);
      final List<Expression> decls = new ArrayList<>(state.size());
      for (int i = 0; i < state.size(); i++) {
        Type type = state.get(i);
        ParameterExpression pe =
            Expressions.parameter(type,
                builder.newName(aggName
                    + "s" + i + "w" + windowIdx));
        builder.add(Expressions.declare(0, pe, null));
        decls.add(pe);
      }
      agg.state = decls;
      Type aggHolderType = agg.context.returnType();
      Type aggStorageType =
          outputPhysType.getJavaFieldType(outputRow.size());
      if (Primitive.is(aggHolderType) && !Primitive.is(aggStorageType)) {
        aggHolderType = Primitive.box(aggHolderType);
      }
      ParameterExpression aggRes = Expressions.parameter(0,
          aggHolderType,
          builder.newName(aggName + "w" + windowIdx));

      builder.add(
          Expressions.declare(0, aggRes,
              Expressions.constant(Primitive.is(aggRes.getType())
                  ? Primitive.of(aggRes.getType()).defaultValue
                  : null,
                  aggRes.getType())));
      agg.result = aggRes;
      outputRow.add(aggRes);
      agg.implementor.implementReset(agg.context,
          new WinAggResetContextImpl(builder, agg.state,
              null, null, null, null, null, null));
    }
  }

  private void implementAdd(List<AggImpState> aggs,
      final BlockBuilder builder7,
      final Function<BlockBuilder, WinAggFrameResultContext> frame,
      final Function<AggImpState, List<RexNode>> rexArguments,
      final DeclarationStatement jDecl) {
    for (final AggImpState agg : aggs) {
      final WinAggAddContext addContext =
          new WinAggAddContextImpl(builder7, agg.state, frame) {
            public Expression currentPosition() {
              return jDecl.parameter;
            }

            public List<RexNode> rexArguments() {
              return rexArguments.apply(agg);
            }

            public RexNode rexFilterArgument() {
              return null; // REVIEW
            }
          };
      agg.implementor.implementAdd(agg.context, addContext);
    }
  }

  private boolean implementResult(List<AggImpState> aggs,
      final BlockBuilder builder,
      final Function<BlockBuilder, WinAggFrameResultContext> frame,
      final Function<AggImpState, List<RexNode>> rexArguments,
      boolean cachedBlock) {
    boolean nonEmpty = false;
    for (final AggImpState agg : aggs) {
      boolean needCache = true;
      if (agg.implementor instanceof WinAggImplementor) {
        WinAggImplementor imp = (WinAggImplementor) agg.implementor;
        needCache = imp.needCacheWhenFrameIntact();
      }
      if (needCache ^ cachedBlock) {
        // Regular aggregates do not change when the windowing frame keeps
        // the same. Ths
        continue;
      }
      nonEmpty = true;
      Expression res = agg.implementor.implementResult(agg.context,
          new WinAggResultContextImpl(builder, agg.state, frame) {
            public List<RexNode> rexArguments() {
              return rexArguments.apply(agg);
            }
          });
      // Several count(a) and count(b) might share the result
      Expression aggRes = builder.append("a" + agg.aggIdx + "res",
          RexToLixTranslator.convert(res, agg.result.getType()));
      builder.add(
          Expressions.statement(Expressions.assign(agg.result, aggRes)));
    }
    return nonEmpty;
  }

  private Expression translateBound(RexToLixTranslator translator,
      ParameterExpression i_, Expression row_, Expression min_,
      Expression max_, Expression rows_, Group group,
      boolean lower,
      PhysType physType, Expression rowComparator,
      Expression keySelector, Expression keyComparator) {
    RexWindowBound bound = lower ? group.lowerBound : group.upperBound;
    if (bound.isUnbounded()) {
      return bound.isPreceding() ? min_ : max_;
    }
    if (group.isRows) {
      if (bound.isCurrentRow()) {
        return i_;
      }
      RexNode node = bound.getOffset();
      Expression offs = translator.translate(node);
      // Floating offset does not make sense since we refer to array index.
      // Nulls do not make sense as well.
      offs = RexToLixTranslator.convert(offs, int.class);

      Expression b = i_;
      if (bound.isFollowing()) {
        b = Expressions.add(b, offs);
      } else {
        b = Expressions.subtract(b, offs);
      }
      return b;
    }
    Expression searchLower = min_;
    Expression searchUpper = max_;
    if (bound.isCurrentRow()) {
      if (lower) {
        searchUpper = i_;
      } else {
        searchLower = i_;
      }
    }

    List<RelFieldCollation> fieldCollations =
        group.collation().getFieldCollations();
    if (bound.isCurrentRow() && fieldCollations.size() != 1) {
      return Expressions.call(
          (lower
              ? BuiltInMethod.BINARY_SEARCH5_LOWER
              : BuiltInMethod.BINARY_SEARCH5_UPPER).method,
          rows_, row_, searchLower, searchUpper, keySelector, keyComparator);
    }
    assert fieldCollations.size() == 1
        : "When using range window specification, ORDER BY should have"
        + " exactly one expression."
        + " Actual collation is " + group.collation();
    // isRange
    int orderKey =
        fieldCollations.get(0).getFieldIndex();
    RelDataType keyType =
        physType.getRowType().getFieldList().get(orderKey).getType();
    Type desiredKeyType = translator.typeFactory.getJavaClass(keyType);
    if (bound.getOffset() == null) {
      desiredKeyType = Primitive.box(desiredKeyType);
    }
    Expression val = translator.translate(
        new RexInputRef(orderKey, keyType), desiredKeyType);
    if (!bound.isCurrentRow()) {
      RexNode node = bound.getOffset();
      Expression offs = translator.translate(node);
      // TODO: support date + interval somehow
      if (bound.isFollowing()) {
        val = Expressions.add(val, offs);
      } else {
        val = Expressions.subtract(val, offs);
      }
    }
    return Expressions.call(
        (lower
            ? BuiltInMethod.BINARY_SEARCH6_LOWER
            : BuiltInMethod.BINARY_SEARCH6_UPPER).method,
        rows_, val, searchLower, searchUpper, keySelector, keyComparator);
  }
}

// End EnumerableWindow.java
