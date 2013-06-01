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
package net.hydromatic.optiq.impl.clone;

import net.hydromatic.linq4j.Ord;
import net.hydromatic.linq4j.expressions.Primitive;
import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.Functions;

import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;

import java.lang.reflect.Type;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

/**
 * Column loader.
 */
class ColumnLoader<T> {
  static final int[] INT_B = {0x2, 0xC, 0xF0, 0xFF00, 0xFFFF0000};
  static final int[] INT_S = {1, 2, 4, 8, 16};
  static final long[] LONG_B = {
      0x2, 0xC, 0xF0, 0xFF00, 0xFFFF0000, 0xFFFFFFFF00000000L};
  static final int[] LONG_S = {1, 2, 4, 8, 16, 32};

  private static final Function1<Timestamp, Long> TIMESTAMP_TO_LONG =
      new Function1<Timestamp, Long>() {
        public Long apply(Timestamp a0) {
          return a0 == null ? null : a0.getTime();
        }
      };

  private static final Function1<Time, Integer> TIME_TO_INT =
      new Function1<Time, Integer>() {
        public Integer apply(Time a0) {
          return a0 == null ? null : (int) (a0.getTime() % 86400000);
        }
      };

  private static final Function1<Date, Integer> DATE_TO_INT =
      new Function1<Date, Integer>() {
        public Integer apply(Date a0) {
          return a0 == null ? null : (int) (a0.getTime() / 86400000);
        }
      };

  public final List<T> list = new ArrayList<T>();
  public final List<ArrayTable.Column> representationValues =
      new ArrayList<ArrayTable.Column>();
  private final JavaTypeFactory typeFactory;
  public final int sortField;

  /** Creates a column loader, and performs the load. */
  ColumnLoader(
      JavaTypeFactory typeFactory,
      Table<T> sourceTable,
      RelDataType elementType) {
    this.typeFactory = typeFactory;
    sourceTable.into(list);
    final int[] sorts = {-1};
    load(elementType, sorts);
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

  public int size() {
    return list.size();
  }

  private void load(final RelDataType elementType, int[] sort) {
    final List<Type> types =
        new AbstractList<Type>() {
          final List<RelDataTypeField> fields =
              elementType.getFieldList();
          public Type get(int index) {
            return typeFactory.getJavaClass(
                fields.get(index).getType());
          }

          public int size() {
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

                public Object get(int index) {
                  return ((Object[]) list.get(index))[slice];
                }

                public int size() {
                  return list.size();
                }
              };
      final List<?> list2 =
          wrap(
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
        final Comparable[] values =
            valueSet.values.toArray(new Comparable[list.size()]);
        final Kev[] kevs = new Kev[list.size()];
        for (int i = 0; i < kevs.length; i++) {
          kevs[i] = new Kev(i, values[i]);
        }
        Arrays.sort(kevs);
        sources = new int[list.size()];
        for (int i = 0; i < sources.length; i++) {
          sources[i] = kevs[i].source;
        }

        boolean identity = true;
        for (int i = 0; i < sources.length; i++) {
          if (sources[i] != i) {
            identity = false;
            break;
          }
        }
        if (identity) {
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
  private static List wrap(List list, RelDataType type) {
    if (type.isNullable()) {
      switch (type.getSqlTypeName()) {
      case TIMESTAMP:
        return Functions.adapt(list, TIMESTAMP_TO_LONG);
      case TIME:
        return Functions.adapt(list, TIME_TO_INT);
      case DATE:
        return Functions.adapt(list, DATE_TO_INT);
      }
    }
    return list;
  }

  /**
   * Set of values of a column, created during the load process, and converted
   * to a serializable (and more compact) form before load completes.
   */
  static class ValueSet {
    final Class clazz;
    final Map<Comparable, Comparable> map =
        new HashMap<Comparable, Comparable>();
    final List<Comparable> values = new ArrayList<Comparable>();
    Comparable min;
    Comparable max;
    boolean containsNull;

    ValueSet(Class clazz) {
      this.clazz = clazz;
    }

    void add(Comparable e) {
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
    ArrayTable.Column freeze(int ordinal, int[] sources) {
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
        }
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

    private long toLong(Object o) {
      // We treat Boolean and Character as if they were subclasses of
      // Number but actually they are not.
      if (o instanceof Boolean) {
        return ((Boolean) o ? 1 : 0);
      } else if (o instanceof Character) {
        return (long) (Character) o;
      } else {
        return ((Number) o).longValue();
      }
    }

    private boolean canBeLong(Object o) {
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
    private ArrayTable.Representation chooseFixedRep(
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
        }
      }
      return new ArrayTable.BitSlicedPrimitiveArray(
          ordinal, bitCount, p, signed);
    }

    /** Two's complement absolute on int value. */
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

  private static class Kev implements Comparable<Kev> {
    private final int source;
    private final Comparable key;

    public Kev(int source, Comparable key) {
      this.source = source;
      this.key = key;
    }

    public int compareTo(Kev o) {
      //noinspection unchecked
      return key.compareTo(o.key);
    }
  }
}

// End ColumnLoader.java
