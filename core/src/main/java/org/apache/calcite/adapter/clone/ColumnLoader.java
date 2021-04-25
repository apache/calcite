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
package org.apache.calcite.adapter.clone;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Column loader.
 *
 * @param <T> Element type of source table
 */
class ColumnLoader<T> {
  static final int[] INT_B = {0x2, 0xC, 0xF0, 0xFF00, 0xFFFF0000};
  static final int[] INT_S = {1, 2, 4, 8, 16};
  static final long[] LONG_B = {
      0x2, 0xC, 0xF0, 0xFF00, 0xFFFF0000, 0xFFFFFFFF00000000L};
  static final int[] LONG_S = {1, 2, 4, 8, 16, 32};

  public final List<T> list = new ArrayList<>();
  public final List<ArrayTable.Column> representationValues = new ArrayList<>();
  private final JavaTypeFactory typeFactory;
  public final int sortField;

  /** Creates a column loader, and performs the load.
   *
   * @param typeFactory Type factory
   * @param sourceTable Source data
   * @param protoRowType Logical row type
   * @param repList Physical row types, or null if not known */
  @SuppressWarnings("method.invocation.invalid")
  ColumnLoader(JavaTypeFactory typeFactory,
      Enumerable<T> sourceTable,
      RelProtoDataType protoRowType,
      @Nullable List<ColumnMetaData.Rep> repList) {
    this.typeFactory = typeFactory;
    final RelDataType rowType = protoRowType.apply(typeFactory);
    if (repList == null) {
      repList =
          Collections.nCopies(rowType.getFieldCount(),
              ColumnMetaData.Rep.OBJECT);
    }
    sourceTable.into(list);
    final int[] sorts = {-1};
    load(rowType, repList, sorts);
    this.sortField = sorts[0];
  }

  static int nextPowerOf2(int v) {
    v--;
    v |= v >>> 1;
    v |= v >>> 2;
    v |= v >>> 4;
    v |= v >>> 8;
    v |= v >>> 16;
    v++;
    return v;
  }

  static long nextPowerOf2(long v) {
    v--;
    v |= v >>> 1;
    v |= v >>> 2;
    v |= v >>> 4;
    v |= v >>> 8;
    v |= v >>> 16;
    v |= v >>> 32;
    v++;
    return v;
  }

  static int log2(int v) {
    int r = 0;
    for (int i = 4; i >= 0; i--) {
      if ((v & INT_B[i]) != 0) {
        v >>= INT_S[i];
        r |= INT_S[i];
      }
    }
    return r;
  }

  static int log2(long v) {
    int r = 0;
    for (int i = 5; i >= 0; i--) {
      if ((v & LONG_B[i]) != 0) {
        v >>= LONG_S[i];
        r |= LONG_S[i];
      }
    }
    return r;
  }

  static int[] invert(int[] targets) {
    final int[] sources = new int[targets.length];
    for (int i = 0; i < targets.length; i++) {
      sources[targets[i]] = i;
    }
    return sources;
  }

  static boolean isIdentity(int[] sources) {
    for (int i = 0; i < sources.length; i++) {
      if (sources[i] != i) {
        return false;
      }
    }
    return true;
  }

  public int size() {
    return list.size();
  }

  private void load(final RelDataType elementType,
      List<ColumnMetaData.Rep> repList, int[] sort) {
    final List<Type> types =
        new AbstractList<Type>() {
          final List<RelDataTypeField> fields =
              elementType.getFieldList();
          @Override public Type get(int index) {
            return typeFactory.getJavaClass(
                fields.get(index).getType());
          }

          @Override public int size() {
            return fields.size();
          }
        };
    int[] sources = null;
    for (final Ord<Type> pair : Ord.zip(types)) {
      @SuppressWarnings("unchecked")
      final List<?> sliceList =
          types.size() == 1
              ? list
              : new AbstractList<Object>() {
                final int slice = pair.i;

                @Override public Object get(int index) {
                  T row = requireNonNull(list.get(index), () -> "null value at index " + index);
                  return ((Object[]) row)[slice];
                }

                @Override public int size() {
                  return list.size();
                }
              };
      final List<?> list2 =
          wrap(
              repList.get(pair.i),
              sliceList,
              elementType.getFieldList().get(pair.i).getType());
      final Class clazz = pair.e instanceof Class
          ? (Class) pair.e
          : Object.class;
      ValueSet valueSet = new ValueSet(clazz);
      for (Object o : list2) {
        valueSet.add((Comparable) o);
      }
      if (sort != null
          && sort[0] < 0
          && valueSet.map.keySet().size() == list.size()) {
        // We have discovered a the first unique key in the table.
        sort[0] = pair.i;
        // map.keySet().size() == list.size() above implies list contains only non-null elements
        @SuppressWarnings("assignment.type.incompatible")
        final Comparable[] values =
            valueSet.values.toArray(new Comparable[0]);
        final Kev[] kevs = new Kev[list.size()];
        for (int i = 0; i < kevs.length; i++) {
          kevs[i] = new Kev(i, values[i]);
        }
        Arrays.sort(kevs);
        sources = new int[list.size()];
        for (int i = 0; i < sources.length; i++) {
          sources[i] = kevs[i].source;
        }

        if (isIdentity(sources)) {
          // Table was already sorted. Clear the permutation.
          // We've already set sort[0], so we won't check for another
          // sorted column.
          sources = null;
        } else {
          // Re-sort all previous columns.
          for (int i = 0; i < pair.i; i++) {
            representationValues.set(
                i, representationValues.get(i).permute(sources));
          }
        }
      }
      representationValues.add(valueSet.freeze(pair.i, sources));
    }
  }

  /** Adapt for some types that we represent differently internally than their
   * JDBC types. {@link java.sql.Timestamp} values that are not null are
   * converted to {@code long}, but nullable timestamps are acquired using
   * {@link java.sql.ResultSet#getObject(int)} and therefore the Timestamp
   * value needs to be converted to a {@link Long}. Similarly
   * {@link java.sql.Date} and {@link java.sql.Time} values to
   * {@link Integer}. */
  private static List<? extends @Nullable Object> wrap(ColumnMetaData.Rep rep, List<?> list,
      RelDataType type) {
    if (true) {
      return list;
    }
    switch (type.getSqlTypeName()) {
    case TIMESTAMP:
      switch (rep) {
      case OBJECT:
      case JAVA_SQL_TIMESTAMP:
        final List<@Nullable Long> longs = Util.transform((List<@Nullable Timestamp>) list,
            (Timestamp t) -> t == null ? null : t.getTime());
        return longs;
      default:
        break;
      }
      break;
    case TIME:
      switch (rep) {
      case OBJECT:
      case JAVA_SQL_TIME:
        return Util.<@Nullable Time, @Nullable Integer>transform(
            (List<@Nullable Time>) list, (Time t) -> t == null
                ? null
                : (int) (t.getTime() % DateTimeUtils.MILLIS_PER_DAY));
      default:
        break;
      }
      break;
    case DATE:
      switch (rep) {
      case OBJECT:
      case JAVA_SQL_DATE:
        return Util.<@Nullable Date, @Nullable Integer>transform(
            (List<@Nullable Date>) list, (Date d) -> d == null
                ? null
                : (int) (d.getTime() / DateTimeUtils.MILLIS_PER_DAY));
      default:
        break;
      }
      break;
    default:
      break;
    }
    return list;
  }

  /**
   * Set of values of a column, created during the load process, and converted
   * to a serializable (and more compact) form before load completes.
   */
  static class ValueSet {
    final Class clazz;
    final Map<Comparable, Comparable> map = new HashMap<>();
    final List<@Nullable Comparable> values = new ArrayList<>();
    @Nullable Comparable min;
    @Nullable Comparable max;
    boolean containsNull;

    ValueSet(Class clazz) {
      this.clazz = clazz;
    }

    void add(@Nullable Comparable e) {
      if (e != null) {
        final Comparable old = e;
        e = map.get(e);
        if (e == null) {
          e = old;
          map.put(e, e);
          //noinspection unchecked
          if (min == null || min.compareTo(e) > 0) {
            min = e;
          }
          //noinspection unchecked
          if (max == null || max.compareTo(e) < 0) {
            max = e;
          }
        }
      } else {
        containsNull = true;
      }
      values.add(e);
    }

    /** Freezes the contents of this value set into a column, optionally
     * re-ordering if {@code sources} is specified. */
    ArrayTable.Column freeze(int ordinal, int @Nullable [] sources) {
      ArrayTable.Representation representation = chooseRep(ordinal);
      final int cardinality = map.size() + (containsNull ? 1 : 0);
      final Object data = representation.freeze(this, sources);
      return new ArrayTable.Column(representation, data, cardinality);
    }

    ArrayTable.Representation chooseRep(int ordinal) {
      Primitive primitive = Primitive.of(clazz);
      Primitive boxPrimitive = Primitive.ofBox(clazz);
      Primitive p = primitive != null ? primitive : boxPrimitive;
      if (!containsNull && p != null) {
        switch (p) {
        case FLOAT:
        case DOUBLE:
          return new ArrayTable.PrimitiveArray(ordinal, p, p);
        case OTHER:
        case VOID:
          throw new AssertionError("wtf?!");
        default:
          break;
        }
        Comparable min = this.min;
        Comparable max = this.max;
        if (canBeLong(min) && canBeLong(max)) {
          return chooseFixedRep(
              ordinal, p, toLong(min), toLong(max));
        }
      }

      // We don't want to use a dictionary if:
      // (a) there are so many values that an object pointer (with one
      //     indirection) has about as many bits as a code (with two
      //     indirections); or
      // (b) if there are very few copies of each value.
      // The condition kind of captures this, but needs to be tuned.
      final int codeCount = map.size() + (containsNull ? 1 : 0);
      final int codeBitCount = log2(nextPowerOf2(codeCount));
      if (codeBitCount < 10 && values.size() > 2000) {
        final ArrayTable.Representation representation =
            chooseFixedRep(-1, Primitive.INT, 0, codeCount - 1);
        return new ArrayTable.ObjectDictionary(ordinal, representation);
      }
      return new ArrayTable.ObjectArray(ordinal);
    }

    private static long toLong(Object o) {
      // We treat Boolean and Character as if they were subclasses of
      // Number but actually they are not.
      if (o instanceof Boolean) {
        return (Boolean) o ? 1 : 0;
      } else if (o instanceof Character) {
        return (long) (Character) o;
      } else {
        return ((Number) o).longValue();
      }
    }

    @EnsuresNonNullIf(result = true, expression = "#1")
    private static boolean canBeLong(@Nullable Object o) {
      return o instanceof Boolean
          || o instanceof Character
          || o instanceof Number;
    }

    /** Chooses a representation for a fixed-precision primitive type
     * (boolean, byte, char, short, int, long).
     *
     * @param ordinal Ordinal of this column in table
     * @param p Type that values are to be returned as (not necessarily the
     *     same as they will be stored)
     * @param min Minimum value to be encoded
     * @param max Maximum value to be encoded (inclusive)
     */
    private static ArrayTable.Representation chooseFixedRep(
        int ordinal, Primitive p, long min, long max) {
      if (min == max) {
        return new ArrayTable.Constant(ordinal);
      }
      final int bitCountMax = log2(nextPowerOf2(abs2(max) + 1));
      int bitCount; // 1 for sign
      boolean signed;

      if (min >= 0) {
        signed = false;
        bitCount = bitCountMax;
      } else {
        signed = true;
        int bitCountMin = log2(nextPowerOf2(abs2(min) + 1));
        bitCount = Math.max(bitCountMin, bitCountMax) + 1;
      }

      // Must be a fixed point primitive.
      if (bitCount > 21 && bitCount < 32) {
        // Can't get more than 2 into a word.
        signed = true;
        bitCount = 32;
      }
      if (bitCount >= 33 && bitCount < 64) {
        // Can't get more than one into a word.
        signed = true;
        bitCount = 64;
      }
      if (signed) {
        switch (bitCount) {
        case 8:
          return new ArrayTable.PrimitiveArray(
              ordinal, Primitive.BYTE, p);
        case 16:
          return new ArrayTable.PrimitiveArray(
              ordinal, Primitive.SHORT, p);
        case 32:
          return new ArrayTable.PrimitiveArray(
              ordinal, Primitive.INT, p);
        case 64:
          return new ArrayTable.PrimitiveArray(
              ordinal, Primitive.LONG, p);
        default:
          break;
        }
      }
      return new ArrayTable.BitSlicedPrimitiveArray(
          ordinal, bitCount, p, signed);
    }

    /** Two's complement absolute on int value. */
    @SuppressWarnings("unused")
    private static int abs2(int v) {
      // -128 becomes +127
      return v < 0 ? ~v : v;
    }

    /** Two's complement absolute on long value. */
    private static long abs2(long v) {
      // -128 becomes +127
      return v < 0 ? ~v : v;
    }
  }

  /** Key-value pair. */
  private static class Kev implements Comparable<Kev> {
    private final int source;
    private final Comparable key;

    Kev(int source, Comparable key) {
      this.source = source;
      this.key = key;
    }

    @Override public int compareTo(Kev o) {
      //noinspection unchecked
      return key.compareTo(o.key);
    }
  }
}
