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

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Primitive;
import net.hydromatic.optiq.*;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Util;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Implementation of table that reads rows from column stores, one per column.
 * Column store formats are chosen based on the type and distribution of the
 * values in the column; see {@link Representation} and
 * {@link RepresentationType}.
 */
class ArrayTable<T>
    extends BaseQueryable<T>
    implements Table<T>
{
  private final Schema schema;
  private final RelDataType relDataType;
  private final List<Column> columns;
  private final int size;
  private final int sortField;

  /** Creates an ArrayTable. */
  public ArrayTable(
      Schema schema,
      Type elementType,
      RelDataType relDataType,
      Expression expression,
      List<Column> columns,
      int size,
      int sortField) {
    super(schema.getQueryProvider(), elementType, expression);
    this.schema = schema;
    this.relDataType = relDataType;
    this.columns = columns;
    this.size = size;
    this.sortField = sortField;

    assert relDataType.getFieldCount() == columns.size();
  }

  public DataContext getDataContext() {
    return schema;
  }

  public RelDataType getRowType() {
    return relDataType;
  }

  public Statistic getStatistic() {
    final ArrayList<BitSet> keys = new ArrayList<BitSet>();
    for (Ord<Column> ord : Ord.zip(columns)) {
      if (ord.e.cardinality == size) {
        keys.add(Util.bitSetOf(ord.i));
      }
    }
    return Statistics.of(size, keys);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Enumerator<T> enumerator() {
    return new Enumerator() {
      final int rowCount = size;
      final int columnCount = columns.size();
      int i = -1;

      public Object[] current() {
        Object[] objects = new Object[columnCount];
        for (int j = 0; j < objects.length; j++) {
          final Column pair = columns.get(j);
          objects[j] = pair.representation.getObject(pair.dataSet, i);
        }
        return objects;
      }

      public boolean moveNext() {
        return (++i < rowCount);
      }

      public void reset() {
        i = -1;
      }
    };
  }

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

  public static class Column {
    final Representation representation;
    final Object dataSet;
    final int cardinality;

    Column(Representation representation, Object data, int cardinality) {
      this.representation = representation;
      this.dataSet = data;
      this.cardinality = cardinality;
    }

    public Column permute(int[] targets) {
      return new Column(
          representation,
          representation.permute(dataSet, targets),
          cardinality);
    }
  }

  public interface Representation {
    RepresentationType getType();

    /** Converts a value set into a compact representation. If
     * {@code sources} is not null, permutes. */
    Object freeze(ColumnLoader.ValueSet valueSet, int[] sources);

    Object getObject(Object dataSet, int ordinal);
    int getInt(Object dataSet, int ordinal);

    /** Creates a data set that is the same as a given data set
     * but re-ordered. */
    Object permute(Object dataSet, int[] sources);
  }

  public static class ObjectArray implements Representation {
    final int ordinal;

    public ObjectArray(int ordinal) {
      this.ordinal = ordinal;
    }

    public RepresentationType getType() {
      return RepresentationType.OBJECT_ARRAY;
    }

    public Object freeze(ColumnLoader.ValueSet valueSet, int[] sources) {
      // We assume the values have been canonized.
      final List<Comparable> list = permuteList(valueSet.values, sources);
      return list.toArray(new Comparable[list.size()]);
    }

    public Object permute(Object dataSet, int[] sources) {
      Comparable[] list = (Comparable[]) dataSet;
      final int size = list.length;
      final Comparable[] comparables = new Comparable[size];
      for (int i = 0; i < size; i++) {
        comparables[i] = list[sources[i]];
      }
      return comparables;
    }

    public Object getObject(Object dataSet, int ordinal) {
      return ((Comparable[]) dataSet)[ordinal];
    }

    public int getInt(Object dataSet, int ordinal) {
      return ((Number) getObject(dataSet, ordinal)).intValue();
    }
  }

  public static class PrimitiveArray implements Representation {
    final int ordinal;
    private final Primitive primitive;
    private final Primitive p;

    public PrimitiveArray(int ordinal, Primitive primitive, Primitive p) {
      this.ordinal = ordinal;
      this.primitive = primitive;
      this.p = p;
    }

    public RepresentationType getType() {
      return RepresentationType.PRIMITIVE_ARRAY;
    }

    public Object freeze(ColumnLoader.ValueSet valueSet, int[] sources) {
      //noinspection unchecked
      return primitive.toArray2(
          permuteList((List) valueSet.values, sources));
    }

    public Object permute(Object dataSet, int[] sources) {
      return primitive.permute(dataSet, sources);
    }

    public Object getObject(Object dataSet, int ordinal) {
      return p.arrayItem(dataSet, ordinal);
    }

    public int getInt(Object dataSet, int ordinal) {
      return Array.getInt(dataSet, ordinal);
    }
  }

  public static class PrimitiveDictionary implements Representation {
    public RepresentationType getType() {
      return RepresentationType.PRIMITIVE_DICTIONARY;
    }

    public Object freeze(ColumnLoader.ValueSet valueSet, int[] sources) {
      throw new UnsupportedOperationException(); // TODO:
    }

    public Object permute(Object dataSet, int[] sources) {
      throw new UnsupportedOperationException(); // TODO:
    }

    public Object getObject(Object dataSet, int ordinal) {
      throw new UnsupportedOperationException(); // TODO:
    }

    public int getInt(Object dataSet, int ordinal) {
      throw new UnsupportedOperationException(); // TODO:
    }
  }

  public static class ObjectDictionary implements Representation {
    final int ordinal;
    final Representation representation;

    public ObjectDictionary(
        int ordinal,
        Representation representation) {
      this.ordinal = ordinal;
      this.representation = representation;
    }

    public RepresentationType getType() {
      return RepresentationType.OBJECT_DICTIONARY;
    }

    public Object freeze(ColumnLoader.ValueSet valueSet, int[] sources) {
      final int n = valueSet.map.keySet().size();
      int extra = valueSet.containsNull ? 1 : 0;
      Comparable[] codeValues =
          valueSet.map.keySet().toArray(new Comparable[n + extra]);
      Arrays.sort(codeValues, 0, n);
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

    public Object permute(Object dataSet, int[] sources) {
      final Pair<Object, Comparable[]> pair =
          (Pair<Object, Comparable[]>) dataSet;
      Object codes = pair.left;
      Comparable[] codeValues = pair.right;
      return Pair.of(representation.permute(codes, sources), codeValues);
    }

    public Object getObject(Object dataSet, int ordinal) {
      Pair<Object, Object[]> pair = (Pair<Object, Object[]>) dataSet;
      int code = representation.getInt(pair.left, ordinal);
      return pair.right[code];
    }

    public int getInt(Object dataSet, int ordinal) {
      return ((Number) getObject(dataSet, ordinal)).intValue();
    }
  }

  public static class StringDictionary implements Representation {
    public RepresentationType getType() {
      return RepresentationType.STRING_DICTIONARY;
    }

    public Object freeze(ColumnLoader.ValueSet valueSet, int[] sources) {
      throw new UnsupportedOperationException(); // TODO:
    }

    public Object permute(Object dataSet, int[] sources) {
      throw new UnsupportedOperationException(); // TODO:
    }

    public Object getObject(Object dataSet, int ordinal) {
      throw new UnsupportedOperationException(); // TODO:
    }

    public int getInt(Object dataSet, int ordinal) {
      throw new UnsupportedOperationException(); // TODO:
    }
  }

  public static class ByteStringDictionary implements Representation {
    public RepresentationType getType() {
      return RepresentationType.BYTE_STRING_DICTIONARY;
    }

    public Object freeze(ColumnLoader.ValueSet valueSet, int[] sources) {
      throw new UnsupportedOperationException(); // TODO:
    }

    public Object permute(Object dataSet, int[] sources) {
      throw new UnsupportedOperationException(); // TODO:
    }

    public Object getObject(Object dataSet, int ordinal) {
      throw new UnsupportedOperationException(); // TODO:
    }

    public int getInt(Object dataSet, int ordinal) {
      throw new UnsupportedOperationException(); // TODO:
    }
  }

  public static class Constant implements Representation {
    final int ordinal;

    public Constant(int ordinal) {
      this.ordinal = ordinal;
    }

    public RepresentationType getType() {
      return RepresentationType.CONSTANT;
    }

    public Object freeze(ColumnLoader.ValueSet valueSet, int[] sources) {
      final int size = valueSet.values.size();
      return Pair.of(size == 0 ? null : valueSet.values.get(0), size);
    }

    public Object permute(Object dataSet, int[] sources) {
      return dataSet;
    }

    public Object getObject(Object dataSet, int ordinal) {
      Column pair = (Column) dataSet;
      return pair.dataSet;
    }

    public int getInt(Object dataSet, int ordinal) {
      Pair<Object, Integer> pair = (Pair<Object, Integer>) dataSet;
      return ((Number) pair.left).intValue();
    }
  }

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

    public RepresentationType getType() {
      return RepresentationType.BIT_SLICED_PRIMITIVE_ARRAY;
    }

    public Object freeze(ColumnLoader.ValueSet valueSet, int[] sources) {
      final int chunksPerWord = 64 / bitCount;
      final List<Comparable> valueList =
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
        final List<Boolean> booleans = (List) valueSet.values;
        for (i = 0; i < n; i++) {
          long v = 0;
          for (int j = 0; j < chunksPerWord; j++) {
            v |= (booleans.get(k++) ? (1 << (bitCount * j)) : 0);
          }
          longs[i] = v;
        }
        if (remainingChunkCount > 0) {
          long v = 0;
          for (int j = 0; j < remainingChunkCount; j++) {
            v |= (booleans.get(k++) ? (1 << (bitCount * j)) : 0);
          }
          longs[i] = v;
        }
      } else {
        @SuppressWarnings("unchecked")
        final List<Number> numbers = (List) valueSet.values;
        for (i = 0; i < n; i++) {
          long v = 0;
          for (int j = 0; j < chunksPerWord; j++) {
            v |= (numbers.get(k++).longValue() << (bitCount * j));
          }
          longs[i] = v;
        }
        if (remainingChunkCount > 0) {
          long v = 0;
          for (int j = 0; j < remainingChunkCount; j++) {
            v |= (numbers.get(k++).longValue() << (bitCount * j));
          }
          longs[i] = v;
        }
      }
      return longs;
    }

    public Object permute(Object dataSet, int[] sources) {
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

    public Object getObject(Object dataSet, int ordinal) {
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

    public int getInt(Object dataSet, int ordinal) {
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
  }

  private static <E> List<E> permuteList(
      final List<E> list, final int[] sources) {
    if (sources == null) {
      return list;
    }
    return new AbstractList<E>() {
      public E get(int index) {
        return list.get(sources[index]);
      }

      public int size() {
        return list.size();
      }
    };
  }
}

// End ArrayTable.java
