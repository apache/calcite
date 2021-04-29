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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of table that reads rows from column stores, one per column.
 * Column store formats are chosen based on the type and distribution of the
 * values in the column; see {@link Representation} and
 * {@link RepresentationType}.
 */
class ArrayTable extends AbstractQueryableTable implements ScannableTable {
  private final RelProtoDataType protoRowType;
  private final Supplier<Content> supplier;

  /** Creates an ArrayTable. */
  ArrayTable(Type elementType, RelProtoDataType protoRowType,
      Supplier<Content> supplier) {
    super(elementType);
    this.protoRowType = protoRowType;
    this.supplier = supplier;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  @Override public Statistic getStatistic() {
    final List<ImmutableBitSet> keys = new ArrayList<>();
    final Content content = supplier.get();
    for (Ord<Column> ord : Ord.zip(content.columns)) {
      if (ord.e.cardinality == content.size) {
        keys.add(ImmutableBitSet.of(ord.i));
      }
    }
    return Statistics.of(content.size, keys, content.collations);
  }

  @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
    return new AbstractEnumerable<@Nullable Object[]>() {
      @Override public Enumerator<@Nullable Object[]> enumerator() {
        final Content content = supplier.get();
        return content.arrayEnumerator();
      }
    };
  }

  @Override public <T> Queryable<T> asQueryable(final QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this,
        tableName) {
      @SuppressWarnings("unchecked")
      @Override public Enumerator<T> enumerator() {
        final Content content = supplier.get();
        return content.enumerator();
      }
    };
  }

  /** How a column's values are represented. */
  enum RepresentationType {
    /** Constant. Contains only one value.
     *
     * <p>We can't store 0-bit values in
     * an array: we'd have no way of knowing how many there were.</p>
     *
     * @see Constant
     */
    CONSTANT,

    /** Object array. Null values are represented by null. Values may or may
     * not be canonized; if canonized, = and != can be implemented using
     * pointer.
     *
     * @see ObjectArray
     */
    OBJECT_ARRAY,

    /**
     * Array of primitives. Null values not possible. Only for primitive
     * types (and not optimal for boolean).
     *
     * @see PrimitiveArray
     */
    PRIMITIVE_ARRAY,

    /** Bit-sliced primitive array. Values are {@code bitCount} bits each,
     * and interpreted as signed. Stored as an array of long values.
     *
     * <p>If gcd(bitCount, 64) != 0, some values will cross boundaries.
     * bits each. But for all of those values except 4, there is a primitive
     * type (8 byte, 16 short, 32 int) which is more efficient.
     *
     * @see BitSlicedPrimitiveArray
     */
    BIT_SLICED_PRIMITIVE_ARRAY,

    /**
     * Dictionary of primitives. Use one of the previous methods to store
     * unsigned offsets into the dictionary. Dictionary is canonized and
     * sorted, so v1 &lt; v2 if and only if code(v1) &lt; code(v2). The
     * dictionary may or may not contain a null value.
     *
     * <p>The dictionary is not beneficial unless the codes are
     * significantly shorter than the values. A column of {@code long}
     * values with many duplicates is a win; a column of mostly distinct
     * {@code short} values is likely a loss. The other win is if there are
     * null values; otherwise the best option would be an
     * {@link #OBJECT_ARRAY}.</p>
     *
     * @see PrimitiveDictionary
     */
    PRIMITIVE_DICTIONARY,

    /**
     * Dictionary of objects. Use one of the previous methods to store
     * unsigned offsets into the dictionary.
     *
     * @see ObjectDictionary
     */
    OBJECT_DICTIONARY,

    /**
     * Compressed string table. Block of char data. Strings represented
     * using an unsigned offset into the table (stored using one of the
     * previous methods).
     *
     * <p>First 2 bytes are unsigned length; subsequent bytes are string
     * contents. The null value, strings longer than 64k and strings that
     * occur very commonly are held in an 'exceptions' array and are
     * recognized by their high offsets. Other strings are created on demand
     * (this reduces the number of objects that need to be created during
     * deserialization from cache.</p>
     *
     * @see StringDictionary
     */
    STRING_DICTIONARY,

    /**
     * Compressed byte array table. Similar to compressed string table.
     *
     * @see ByteStringDictionary
     */
    BYTE_STRING_DICTIONARY,
  }

  /** Column definition and value set. */
  public static class Column {
    final Representation representation;
    final Object dataSet;
    final int cardinality;

    Column(Representation representation, Object data, int cardinality) {
      this.representation = representation;
      this.dataSet = data;
      this.cardinality = cardinality;
    }

    public Column permute(int[] sources) {
      return new Column(
          representation,
          representation.permute(dataSet, sources),
          cardinality);
    }

    @Override public String toString() {
      return "Column(representation=" + representation
          + ", value=" + representation.toString(dataSet) + ")";
    }

    /** Returns a list view onto a data set. */
    public static List asList(final Representation representation,
        final Object dataSet) {
      // Cache size. It might be expensive to compute.
      final int size = representation.size(dataSet);
      return new AbstractList() {
        @Override public @Nullable Object get(int index) {
          return representation.getObject(dataSet, index);
        }

        @Override public int size() {
          return size;
        }
      };
    }
  }

  /** Representation of the values of a column. */
  public interface Representation {
    /** Returns the representation type. */
    RepresentationType getType();

    /** Converts a value set into a compact representation. If
     * {@code sources} is not null, permutes. */
    Object freeze(ColumnLoader.ValueSet valueSet, int @Nullable [] sources);

    @Nullable Object getObject(Object dataSet, int ordinal);
    int getInt(Object dataSet, int ordinal);

    /** Creates a data set that is the same as a given data set
     * but re-ordered. */
    Object permute(Object dataSet, int[] sources);

    /** Returns the number of elements in a data set. (Some representations
     * return the capacity, which may be slightly larger than the actual
     * size.) */
    int size(Object dataSet);

    /** Converts a data set to a string. */
    String toString(Object dataSet);
  }

  /** Representation that stores the column values in an array. */
  public static class ObjectArray implements Representation {
    final int ordinal;

    ObjectArray(int ordinal) {
      this.ordinal = ordinal;
    }

    @Override public String toString() {
      return "ObjectArray(ordinal=" + ordinal + ")";
    }

    @Override public RepresentationType getType() {
      return RepresentationType.OBJECT_ARRAY;
    }

    @Override public Object freeze(ColumnLoader.ValueSet valueSet, int @Nullable [] sources) {
      // We assume the values have been canonized.
      final List<Comparable> list = permuteList(valueSet.values, sources);
      return list.toArray(new Comparable[0]);
    }

    @Override public Object permute(Object dataSet, int[] sources) {
      @Nullable Comparable[] list = (@Nullable Comparable[]) dataSet;
      final int size = list.length;
      final @Nullable Comparable[] comparables = new Comparable[size];
      for (int i = 0; i < size; i++) {
        comparables[i] = list[sources[i]];
      }
      return comparables;
    }

    @Override public @Nullable Object getObject(Object dataSet, int ordinal) {
      return ((@Nullable Comparable[]) dataSet)[ordinal];
    }

    @Override public int getInt(Object dataSet, int ordinal) {
      Number value = (Number) getObject(dataSet, ordinal);
      return requireNonNull(value, "value").intValue();
    }

    @Override public int size(Object dataSet) {
      return ((Comparable[]) dataSet).length;
    }

    @Override public String toString(Object dataSet) {
      return Arrays.toString((Comparable[]) dataSet);
    }
  }

  /** Representation that stores the values of a column in an array of
   * primitive values. */
  public static class PrimitiveArray implements Representation {
    final int ordinal;
    private final Primitive primitive;
    private final Primitive p;

    PrimitiveArray(int ordinal, Primitive primitive, Primitive p) {
      this.ordinal = ordinal;
      this.primitive = primitive;
      this.p = p;
    }

    @Override public String toString() {
      return "PrimitiveArray(ordinal=" + ordinal
          + ", primitive=" + primitive
          + ", p=" + p
          + ")";
    }

    @Override public RepresentationType getType() {
      return RepresentationType.PRIMITIVE_ARRAY;
    }

    @Override public Object freeze(ColumnLoader.ValueSet valueSet, int @Nullable [] sources) {
      //noinspection unchecked
      return primitive.toArray2(
          permuteList((List) valueSet.values, sources));
    }

    @Override public Object permute(Object dataSet, int[] sources) {
      return primitive.permute(dataSet, sources);
    }

    @Override public @Nullable Object getObject(Object dataSet, int ordinal) {
      return p.arrayItem(dataSet, ordinal);
    }

    @Override public int getInt(Object dataSet, int ordinal) {
      return Array.getInt(dataSet, ordinal);
    }

    @Override public int size(Object dataSet) {
      return Array.getLength(dataSet);
    }

    @Override public String toString(Object dataSet) {
      return p.arrayToString(dataSet);
    }
  }

  /** Representation that stores column values in a dictionary of
   * primitive values, then uses a short code for each row. */
  public static class PrimitiveDictionary implements Representation {
    PrimitiveDictionary() {
    }

    @Override public String toString() {
      return "PrimitiveDictionary()";
    }

    @Override public RepresentationType getType() {
      return RepresentationType.PRIMITIVE_DICTIONARY;
    }

    @Override public Object freeze(ColumnLoader.ValueSet valueSet, int @Nullable [] sources) {
      throw new UnsupportedOperationException(); // TODO:
    }

    @Override public Object permute(Object dataSet, int[] sources) {
      throw new UnsupportedOperationException(); // TODO:
    }

    @Override public Object getObject(Object dataSet, int ordinal) {
      throw new UnsupportedOperationException(); // TODO:
    }

    @Override public int getInt(Object dataSet, int ordinal) {
      throw new UnsupportedOperationException(); // TODO:
    }

    @Override public int size(Object dataSet) {
      throw new UnsupportedOperationException(); // TODO:
    }

    @Override public String toString(Object dataSet) {
      throw new UnsupportedOperationException(); // TODO:
    }
  }

  /** Representation that stores the values of a column as a
   * dictionary of objects. */
  public static class ObjectDictionary implements Representation {
    final int ordinal;
    final Representation representation;

    ObjectDictionary(
        int ordinal,
        Representation representation) {
      this.ordinal = ordinal;
      this.representation = representation;
    }

    @Override public String toString() {
      return "ObjectDictionary(ordinal=" + ordinal
          + ", representation=" + representation
          + ")";
    }

    @Override public RepresentationType getType() {
      return RepresentationType.OBJECT_DICTIONARY;
    }

    @Override public Object freeze(ColumnLoader.ValueSet valueSet, int @Nullable [] sources) {
      final int n = valueSet.map.keySet().size();
      int extra = valueSet.containsNull ? 1 : 0;
      @SuppressWarnings("all")
      @Nullable Comparable[] codeValues =
          valueSet.map.keySet().toArray(new Comparable[n + extra]);
      // codeValues[0..n] is non-null since valueSet.map.keySet is non-null
      // There might be null at the very end, however, it won't participate in Arrays.sort
      @SuppressWarnings("assignment.type.incompatible")
      Comparable[] nonNullCodeValues = codeValues;
      Arrays.sort(nonNullCodeValues, 0, n);
      ColumnLoader.ValueSet codeValueSet =
          new ColumnLoader.ValueSet(int.class);
      final List<Comparable> list = permuteList(valueSet.values, sources);
      for (Comparable value : list) {
        int code;
        if (value == null) {
          code = n;
        } else {
          code = Arrays.binarySearch(codeValues, value);
          assert code >= 0 : code + ", " + value;
        }
        codeValueSet.add(code);
      }
      Object codes = representation.freeze(codeValueSet, null);
      return Pair.of(codes, codeValues);
    }

    private static Pair<Object, @Nullable Comparable[]> unfreeze(Object value) {
      return (Pair<Object, @Nullable Comparable[]>) value;
    }

    @Override public Object permute(Object dataSet, int[] sources) {
      final Pair<Object, @Nullable Comparable[]> pair = unfreeze(dataSet);
      Object codes = pair.left;
      @Nullable Comparable[] codeValues = pair.right;
      return Pair.of(representation.permute(codes, sources), codeValues);
    }

    @Override public @Nullable Object getObject(Object dataSet, int ordinal) {
      final Pair<Object, @Nullable Comparable[]> pair = unfreeze(dataSet);
      int code = representation.getInt(pair.left, ordinal);
      return pair.right[code];
    }

    @Override public int getInt(Object dataSet, int ordinal) {
      Number value = (Number) getObject(dataSet, ordinal);
      return requireNonNull(value, "value").intValue();
    }

    @Override public int size(Object dataSet) {
      final Pair<Object, @Nullable Comparable[]> pair = unfreeze(dataSet);
      return representation.size(pair.left);
    }

    @Override public String toString(Object dataSet) {
      return Column.asList(this, dataSet).toString();
    }
  }

  /** Representation that stores string column values. */
  public static class StringDictionary implements Representation {
    StringDictionary() {
    }

    @Override public String toString() {
      return "StringDictionary()";
    }

    @Override public RepresentationType getType() {
      return RepresentationType.STRING_DICTIONARY;
    }

    @Override public Object freeze(ColumnLoader.ValueSet valueSet, int @Nullable [] sources) {
      throw new UnsupportedOperationException(); // TODO:
    }

    @Override public Object permute(Object dataSet, int[] sources) {
      throw new UnsupportedOperationException(); // TODO:
    }

    @Override public Object getObject(Object dataSet, int ordinal) {
      throw new UnsupportedOperationException(); // TODO:
    }

    @Override public int getInt(Object dataSet, int ordinal) {
      throw new UnsupportedOperationException(); // TODO:
    }

    @Override public int size(Object dataSet) {
      throw new UnsupportedOperationException(); // TODO:
    }

    @Override public String toString(Object dataSet) {
      return Column.asList(this, dataSet).toString();
    }
  }

  /** Representation that stores byte-string column values. */
  public static class ByteStringDictionary implements Representation {
    ByteStringDictionary() {
    }

    @Override public String toString() {
      return "ByteStringDictionary()";
    }

    @Override public RepresentationType getType() {
      return RepresentationType.BYTE_STRING_DICTIONARY;
    }

    @Override public Object freeze(ColumnLoader.ValueSet valueSet, int @Nullable [] sources) {
      throw new UnsupportedOperationException(); // TODO:
    }

    @Override public Object permute(Object dataSet, int[] sources) {
      throw new UnsupportedOperationException(); // TODO:
    }

    @Override public Object getObject(Object dataSet, int ordinal) {
      throw new UnsupportedOperationException(); // TODO:
    }

    @Override public int getInt(Object dataSet, int ordinal) {
      throw new UnsupportedOperationException(); // TODO:
    }

    @Override public int size(Object dataSet) {
      throw new UnsupportedOperationException(); // TODO:
    }

    @Override public String toString(Object dataSet) {
      return Column.asList(this, dataSet).toString();
    }
  }

  /** Representation of a column that has the same value for every row. */
  public static class Constant implements Representation {
    final int ordinal;

    Constant(int ordinal) {
      this.ordinal = ordinal;
    }

    @Override public String toString() {
      return "Constant(ordinal=" + ordinal + ")";
    }

    @Override public RepresentationType getType() {
      return RepresentationType.CONSTANT;
    }

    @Override public Object freeze(ColumnLoader.ValueSet valueSet, int @Nullable [] sources) {
      final int size = valueSet.values.size();
      return Pair.of(size == 0 ? null : valueSet.values.get(0), size);
    }

    private static Pair<@Nullable Object, Integer> unfreeze(Object value) {
      return (Pair<@Nullable Object, Integer>) value;
    }

    @Override public Object permute(Object dataSet, int[] sources) {
      return dataSet;
    }

    @Override public @Nullable Object getObject(Object dataSet, int ordinal) {
      Pair<@Nullable Object, Integer> pair = unfreeze(dataSet);
      return pair.left;
    }

    @Override public int getInt(Object dataSet, int ordinal) {
      @Nullable Number value = (Number) getObject(dataSet, ordinal);
      return requireNonNull(value, "value").intValue();
    }

    @Override public int size(Object dataSet) {
      Pair<@Nullable Object, Integer> pair = unfreeze(dataSet);
      return pair.right;
    }

    @Override public String toString(Object dataSet) {
      Pair<@Nullable Object, Integer> pair = unfreeze(dataSet);
      return Collections.nCopies(pair.right, pair.left).toString();
    }
  }

  /** Representation that stores numeric values in a bit-sliced
   * array. Each value does not necessarily occupy 8, 16, 32 or 64
   * bits (the number of bits used by the built-in types). This
   * representation is often used to store the value codes for a
   * dictionary-based representation. */
  public static class BitSlicedPrimitiveArray implements Representation {
    final int ordinal;
    final int bitCount;
    final Primitive primitive;
    final boolean signed;

    BitSlicedPrimitiveArray(
        int ordinal, int bitCount, Primitive primitive, boolean signed) {
      assert bitCount > 0;
      this.ordinal = ordinal;
      this.bitCount = bitCount;
      this.primitive = primitive;
      this.signed = signed;
    }

    @Override public String toString() {
      return "BitSlicedPrimitiveArray(ordinal=" + ordinal
          + ", bitCount=" + bitCount
          + ", primitive=" + primitive
          + ", signed=" + signed + ")";
    }

    @Override public RepresentationType getType() {
      return RepresentationType.BIT_SLICED_PRIMITIVE_ARRAY;
    }

    @Override public Object freeze(ColumnLoader.ValueSet valueSet, int @Nullable [] sources) {
      final int chunksPerWord = 64 / bitCount;
      final List<@Nullable Comparable> valueList =
          permuteList(valueSet.values, sources);
      final int valueCount = valueList.size();
      final int wordCount =
          (valueCount + (chunksPerWord - 1)) / chunksPerWord;
      final int remainingChunkCount = valueCount % chunksPerWord;
      final long[] longs = new long[wordCount];
      final int n = valueCount / chunksPerWord;
      int i;
      int k = 0;
      if (valueCount > 0
          && valueList.get(0) instanceof Boolean) {
        @SuppressWarnings("unchecked")
        final List<Boolean> booleans = (List) valueList;
        for (i = 0; i < n; i++) {
          long v = 0;
          for (int j = 0; j < chunksPerWord; j++) {
            v |= booleans.get(k++) ? (1 << (bitCount * j)) : 0;
          }
          longs[i] = v;
        }
        if (remainingChunkCount > 0) {
          long v = 0;
          for (int j = 0; j < remainingChunkCount; j++) {
            v |= booleans.get(k++) ? (1 << (bitCount * j)) : 0;
          }
          longs[i] = v;
        }
      } else {
        @SuppressWarnings("unchecked")
        final List<Number> numbers = (List) valueList;
        for (i = 0; i < n; i++) {
          long v = 0;
          for (int j = 0; j < chunksPerWord; j++) {
            v |= numbers.get(k++).longValue() << (bitCount * j);
          }
          longs[i] = v;
        }
        if (remainingChunkCount > 0) {
          long v = 0;
          for (int j = 0; j < remainingChunkCount; j++) {
            v |= numbers.get(k++).longValue() << (bitCount * j);
          }
          longs[i] = v;
        }
      }
      return longs;
    }

    @Override public Object permute(Object dataSet, int[] sources) {
      final long[] longs0 = (long[]) dataSet;
      int n = sources.length;
      final long[] longs = new long[longs0.length];
      for (int i = 0; i < n; i++) {
        orLong(
            bitCount, longs, i,
            getLong(bitCount, longs0, sources[i]));
      }
      return longs;
    }

    @Override public Object getObject(Object dataSet, int ordinal) {
      final long[] longs = (long[]) dataSet;
      final int chunksPerWord = 64 / bitCount;
      final int word = ordinal / chunksPerWord;
      final long v = longs[word];
      final int chunk = ordinal % chunksPerWord;
      final int mask = (1 << bitCount) - 1;
      final int signMask = 1 << (bitCount - 1);
      final int shift = chunk * bitCount;
      final long w = v >> shift;
      long x = w & mask;
      if (signed && (x & signMask) != 0) {
        x = -x;
      }
      switch (primitive) {
      case BOOLEAN:
        return x != 0;
      case BYTE:
        return (byte) x;
      case CHAR:
        return (char) x;
      case SHORT:
        return (short) x;
      case INT:
        return (int) x;
      case LONG:
        return x;
      default:
        throw new AssertionError(primitive + " unexpected");
      }
    }

    @Override public int getInt(Object dataSet, int ordinal) {
      final long[] longs = (long[]) dataSet;
      final int chunksPerWord = 64 / bitCount;
      final int word = ordinal / chunksPerWord;
      final long v = longs[word];
      final int chunk = ordinal % chunksPerWord;
      final int mask = (1 << bitCount) - 1;
      final int signMask = 1 << (bitCount - 1);
      final int shift = chunk * bitCount;
      final long w = v >> shift;
      long x = w & mask;
      if (signed && (x & signMask) != 0) {
        x = -x;
      }
      return (int) x;
    }

    public static long getLong(int bitCount, long[] values, int ordinal) {
      return getLong(
          bitCount, 64 / bitCount, (1L << bitCount) - 1L,
          values, ordinal);
    }

    public static long getLong(
        int bitCount,
        int chunksPerWord,
        long mask,
        long[] values,
        int ordinal) {
      final int word = ordinal / chunksPerWord;
      final int chunk = ordinal % chunksPerWord;
      final long value = values[word];
      final int shift = chunk * bitCount;
      return (value >> shift) & mask;
    }

    public static void orLong(
        int bitCount, long[] values, int ordinal, long value) {
      orLong(bitCount, 64 / bitCount, values, ordinal, value);
    }

    public static void orLong(
        int bitCount, int chunksPerWord, long[] values, int ordinal,
        long value) {
      final int word = ordinal / chunksPerWord;
      final int chunk = ordinal % chunksPerWord;
      final int shift = chunk * bitCount;
      values[word] |= value << shift;
    }

    @Override public int size(Object dataSet) {
      final long[] longs = (long[]) dataSet;
      final int chunksPerWord = 64 / bitCount;
      return longs.length * chunksPerWord; // may be slightly too high
    }

    @Override public String toString(Object dataSet) {
      return Column.asList(this, dataSet).toString();
    }
  }

  private static <E> List<E> permuteList(
      final List<E> list, final int @Nullable [] sources) {
    if (sources == null) {
      return list;
    }
    return new AbstractList<E>() {
      @Override public E get(int index) {
        return list.get(sources[index]);
      }

      @Override public int size() {
        return list.size();
      }
    };
  }

  /** Contents of a table. */
  public static class Content {
    private final List<Column> columns;
    private final int size;
    private final ImmutableList<RelCollation> collations;

    Content(List<? extends Column> columns, int size,
        Iterable<? extends RelCollation> collations) {
      this.columns = ImmutableList.copyOf(columns);
      this.size = size;
      this.collations = ImmutableList.copyOf(collations);
    }

    @Deprecated // to be removed before 2.0
    Content(List<? extends Column> columns, int size, int sortField) {
      this(columns, size,
          sortField >= 0
              ? RelCollations.createSingleton(sortField)
              : ImmutableList.of());
    }

    @SuppressWarnings("unchecked")
    public <T> Enumerator<T> enumerator() {
      if (columns.size() == 1) {
        return (Enumerator<T>) new ObjectEnumerator(size, columns.get(0));
      } else {
        return (Enumerator<T>) new ArrayEnumerator(size, columns);
      }
    }

    public Enumerator<@Nullable Object[]> arrayEnumerator() {
      return new ArrayEnumerator(size, columns);
    }

    /** Enumerator over a table with a single column; each element
     * returned is an object. */
    private static class ObjectEnumerator implements Enumerator<@Nullable Object> {
      final int rowCount;
      final Object dataSet;
      final Representation representation;
      int i = -1;

      ObjectEnumerator(int rowCount, Column column) {
        this.rowCount = rowCount;
        this.dataSet = column.dataSet;
        this.representation = column.representation;
      }

      @Override public @Nullable Object current() {
        return representation.getObject(dataSet, i);
      }

      @Override public boolean moveNext() {
        return ++i < rowCount;
      }

      @Override public void reset() {
        i = -1;
      }

      @Override public void close() {
      }
    }

    /** Enumerator over a table with more than one column; each element
     * returned is an array. */
    private static class ArrayEnumerator implements Enumerator<@Nullable Object[]> {
      final int rowCount;
      final List<Column> columns;
      int i = -1;

      ArrayEnumerator(int rowCount, List<Column> columns) {
        this.rowCount = rowCount;
        this.columns = columns;
      }

      @Override public @Nullable Object[] current() {
        @Nullable Object[] objects = new Object[columns.size()];
        for (int j = 0; j < objects.length; j++) {
          final Column pair = columns.get(j);
          objects[j] = pair.representation.getObject(pair.dataSet, i);
        }
        return objects;
      }

      @Override public boolean moveNext() {
        return ++i < rowCount;
      }

      @Override public void reset() {
        i = -1;
      }

      @Override public void close() {
      }
    }
  }
}
