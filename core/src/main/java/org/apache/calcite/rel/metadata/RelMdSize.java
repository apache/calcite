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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Default implementations of the
 * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Size}
 * metadata provider for the standard logical algebra.
 *
 * @see RelMetadataQuery#getAverageRowSize
 * @see RelMetadataQuery#getAverageColumnSizes
 * @see RelMetadataQuery#getAverageColumnSizesNotNull
 */
public class RelMdSize implements MetadataHandler<BuiltInMetadata.Size> {
  /** Source for
   * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Size}. */
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(new RelMdSize(),
          BuiltInMetadata.Size.Handler.class);

  /** Bytes per character (2). */
  public static final int BYTES_PER_CHARACTER = Character.SIZE / Byte.SIZE;

  //~ Constructors -----------------------------------------------------------

  protected RelMdSize() {}

  //~ Methods ----------------------------------------------------------------

  @Override public MetadataDef<BuiltInMetadata.Size> getDef() {
    return BuiltInMetadata.Size.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.Size#averageRowSize()},
   * invoked using reflection.
   *
   * @see org.apache.calcite.rel.metadata.RelMetadataQuery#getAverageRowSize
   */
  public @Nullable Double averageRowSize(RelNode rel, RelMetadataQuery mq) {
    final List<@Nullable Double> averageColumnSizes = mq.getAverageColumnSizes(rel);
    if (averageColumnSizes == null) {
      return null;
    }
    double d = 0d;
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    for (Pair<@Nullable Double, RelDataTypeField> p
        : Pair.zip(averageColumnSizes, fields)) {
      if (p.left == null) {
        Double fieldValueSize = averageFieldValueSize(p.right);
        if (fieldValueSize == null) {
          return null;
        }
        d += fieldValueSize;
      } else {
        d += p.left;
      }
    }
    return d;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.Size#averageColumnSizes()},
   * invoked using reflection.
   *
   * @see org.apache.calcite.rel.metadata.RelMetadataQuery#getAverageColumnSizes
   */
  public @Nullable List<@Nullable Double> averageColumnSizes(RelNode rel, RelMetadataQuery mq) {
    return null; // absolutely no idea
  }

  public @Nullable List<@Nullable Double> averageColumnSizes(Filter rel, RelMetadataQuery mq) {
    return mq.getAverageColumnSizes(rel.getInput());
  }

  public @Nullable List<@Nullable Double> averageColumnSizes(Sort rel, RelMetadataQuery mq) {
    return mq.getAverageColumnSizes(rel.getInput());
  }

  public @Nullable List<@Nullable Double> averageColumnSizes(TableModify rel, RelMetadataQuery mq) {
    return mq.getAverageColumnSizes(rel.getInput());
  }

  public @Nullable List<@Nullable Double> averageColumnSizes(Exchange rel, RelMetadataQuery mq) {
    return mq.getAverageColumnSizes(rel.getInput());
  }

  public @Nullable List<@Nullable Double> averageColumnSizes(Project rel, RelMetadataQuery mq) {
    final List<@Nullable Double> inputColumnSizes =
        mq.getAverageColumnSizesNotNull(rel.getInput());
    final ImmutableNullableList.Builder<@Nullable Double> sizes = ImmutableNullableList.builder();
    for (RexNode project : rel.getProjects()) {
      sizes.add(averageRexSize(project, inputColumnSizes));
    }
    return sizes.build();
  }

  public @Nullable List<@Nullable Double> averageColumnSizes(Calc rel, RelMetadataQuery mq) {
    final List<@Nullable Double> inputColumnSizes =
        mq.getAverageColumnSizesNotNull(rel.getInput());
    final ImmutableNullableList.Builder<@Nullable Double> sizes = ImmutableNullableList.builder();
    rel.getProgram().split().left.forEach(
        exp -> sizes.add(averageRexSize(exp, inputColumnSizes)));
    return sizes.build();
  }

  public @Nullable List<@Nullable Double> averageColumnSizes(Values rel, RelMetadataQuery mq) {
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    final ImmutableNullableList.Builder<@Nullable Double> list = ImmutableNullableList.builder();
    for (int i = 0; i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      if (rel.getTuples().isEmpty()) {
        list.add(averageTypeValueSize(field.getType()));
      } else {
        double d = 0;
        for (ImmutableList<RexLiteral> literals : rel.getTuples()) {
          d += typeValueSize(field.getType(),
              literals.get(i).getValueAs(Comparable.class));
        }
        d /= rel.getTuples().size();
        list.add(d);
      }
    }
    return list.build();
  }

  public @Nullable List<@Nullable Double> averageColumnSizes(TableScan rel, RelMetadataQuery mq) {
    final BuiltInMetadata.Size.Handler handler =
        rel.getTable().unwrap(BuiltInMetadata.Size.Handler.class);
    if (handler != null) {
      return handler.averageColumnSizes(rel, mq);
    }
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    final ImmutableNullableList.Builder<@Nullable Double> list = ImmutableNullableList.builder();
    for (RelDataTypeField field : fields) {
      list.add(averageTypeValueSize(field.getType()));
    }
    return list.build();
  }

  public List<@Nullable Double> averageColumnSizes(Aggregate rel, RelMetadataQuery mq) {
    final List<@Nullable Double> inputColumnSizes =
        mq.getAverageColumnSizesNotNull(rel.getInput());
    final ImmutableNullableList.Builder<@Nullable Double> list = ImmutableNullableList.builder();
    for (int key : rel.getGroupSet()) {
      list.add(inputColumnSizes.get(key));
    }
    for (AggregateCall aggregateCall : rel.getAggCallList()) {
      list.add(averageTypeValueSize(aggregateCall.type));
    }
    return list.build();
  }

  public @Nullable List<@Nullable Double> averageColumnSizes(Join rel, RelMetadataQuery mq) {
    return averageJoinColumnSizes(rel, mq);
  }

  private static @Nullable List<@Nullable Double> averageJoinColumnSizes(Join rel,
      RelMetadataQuery mq) {
    boolean semiOrAntijoin = !rel.getJoinType().projectsRight();
    final RelNode left = rel.getLeft();
    final RelNode right = rel.getRight();
    final @Nullable List<@Nullable Double> lefts = mq.getAverageColumnSizes(left);
    final @Nullable List<@Nullable Double> rights =
        semiOrAntijoin ? null : mq.getAverageColumnSizes(right);
    if (lefts == null && rights == null) {
      return null;
    }
    final int fieldCount = rel.getRowType().getFieldCount();
    @Nullable Double[] sizes = new Double[fieldCount];
    if (lefts != null) {
      lefts.toArray(sizes);
    }
    if (rights != null) {
      final int leftCount = left.getRowType().getFieldCount();
      for (int i = 0; i < rights.size(); i++) {
        sizes[leftCount + i] = rights.get(i);
      }
    }
    return ImmutableNullableList.copyOf(sizes);
  }

  public @Nullable List<@Nullable Double> averageColumnSizes(Intersect rel, RelMetadataQuery mq) {
    return mq.getAverageColumnSizes(rel.getInput(0));
  }

  public @Nullable List<@Nullable Double> averageColumnSizes(Minus rel, RelMetadataQuery mq) {
    return mq.getAverageColumnSizes(rel.getInput(0));
  }

  public @Nullable List<@Nullable Double> averageColumnSizes(Union rel, RelMetadataQuery mq) {
    final int fieldCount = rel.getRowType().getFieldCount();
    List<List<@Nullable Double>> inputColumnSizeList = new ArrayList<>();
    for (RelNode input : rel.getInputs()) {
      final List<@Nullable Double> inputSizes = mq.getAverageColumnSizes(input);
      if (inputSizes != null) {
        inputColumnSizeList.add(inputSizes);
      }
    }
    switch (inputColumnSizeList.size()) {
    case 0:
      return null; // all were null
    case 1:
      return inputColumnSizeList.get(0); // all but one were null
    default:
      break;
    }
    final ImmutableNullableList.Builder<@Nullable Double> sizes =
        ImmutableNullableList.builder();
    int nn = 0;
    for (int i = 0; i < fieldCount; i++) {
      double d = 0d;
      int n = 0;
      for (List<@Nullable Double> inputColumnSizes : inputColumnSizeList) {
        Double d2 = inputColumnSizes.get(i);
        if (d2 != null) {
          d += d2;
          ++n;
          ++nn;
        }
      }
      sizes.add(n > 0 ? d / n : null);
    }
    if (nn == 0) {
      return null; // all columns are null
    }
    return sizes.build();
  }

  /** Estimates the average size (in bytes) of a value of a field, knowing
   * nothing more than its type.
   *
   * <p>We assume that the proportion of nulls is negligible, even if the field
   * is nullable.
   */
  protected @Nullable Double averageFieldValueSize(RelDataTypeField field) {
    return averageTypeValueSize(field.getType());
  }

  /** Estimates the average size (in bytes) of a value of a type.
   *
   * <p>We assume that the proportion of nulls is negligible, even if the type
   * is nullable.
   */
  public @Nullable Double averageTypeValueSize(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case BOOLEAN:
    case TINYINT:
      return 1d;
    case SMALLINT:
      return 2d;
    case INTEGER:
    case REAL:
    case DECIMAL:
    case DATE:
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      return 4d;
    case BIGINT:
    case DOUBLE:
    case FLOAT: // sic
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return 8d;
    case BINARY:
      return (double) type.getPrecision();
    case VARBINARY:
      return Math.min((double) type.getPrecision(), 100d);
    case CHAR:
      return (double) type.getPrecision() * BYTES_PER_CHARACTER;
    case VARCHAR:
      // Even in large (say VARCHAR(2000)) columns most strings are small
      return Math.min((double) type.getPrecision() * BYTES_PER_CHARACTER, 100d);
    case ROW:
      double average = 0.0;
      for (RelDataTypeField field : type.getFieldList()) {
        Double size = averageTypeValueSize(field.getType());
        if (size != null) {
          average += size;
        }
      }
      return average;
    default:
      return null;
    }
  }

  /** Estimates the average size (in bytes) of a value of a type.
   *
   * <p>Nulls count as 1 byte.
   */
  public double typeValueSize(RelDataType type, @Nullable Comparable value) {
    if (value == null) {
      return 1d;
    }
    switch (type.getSqlTypeName()) {
    case BOOLEAN:
    case TINYINT:
      return 1d;
    case SMALLINT:
      return 2d;
    case INTEGER:
    case FLOAT:
    case REAL:
    case DATE:
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      return 4d;
    case BIGINT:
    case DOUBLE:
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return 8d;
    case BINARY:
    case VARBINARY:
      return ((ByteString) value).length();
    case CHAR:
    case VARCHAR:
      return ((NlsString) value).getValue().length() * BYTES_PER_CHARACTER;
    default:
      return 32;
    }
  }

  public @Nullable Double averageRexSize(RexNode node,
      List<? extends @Nullable Double> inputColumnSizes) {
    switch (node.getKind()) {
    case INPUT_REF:
      return inputColumnSizes.get(((RexInputRef) node).getIndex());
    case LITERAL:
      return typeValueSize(node.getType(),
          ((RexLiteral) node).getValueAs(Comparable.class));
    default:
      if (node instanceof RexCall) {
        RexCall call = (RexCall) node;
        for (RexNode operand : call.getOperands()) {
          // It's a reasonable assumption that a function's result will have
          // similar size to its argument of a similar type. For example,
          // UPPER(c) has the same average size as c.
          if (operand.getType().getSqlTypeName()
              == node.getType().getSqlTypeName()) {
            return averageRexSize(operand, inputColumnSizes);
          }
        }
      }
      return averageTypeValueSize(node.getType());
    }
  }
}
