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
package net.hydromatic.optiq;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.FunctionExpression;
import net.hydromatic.linq4j.expressions.Primitive;
import net.hydromatic.linq4j.expressions.Types;
import net.hydromatic.linq4j.function.*;

import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.impl.jdbc.JdbcSchema;
import net.hydromatic.optiq.runtime.*;

import org.eigenbase.rel.metadata.Metadata;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlExplainLevel;

import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import javax.sql.DataSource;

import static org.eigenbase.rel.metadata.BuiltInMetadata.*;

/**
 * Builtin methods.
 */
public enum BuiltinMethod {
  QUERYABLE_SELECT(Queryable.class, "select", FunctionExpression.class),
  QUERYABLE_AS_ENUMERABLE(Queryable.class, "asEnumerable"),
  QUERYABLE_TABLE_AS_QUERYABLE(QueryableTable.class, "asQueryable",
      QueryProvider.class, SchemaPlus.class, String.class),
  AS_QUERYABLE(Enumerable.class, "asQueryable"),
  ABSTRACT_ENUMERABLE_CTOR(AbstractEnumerable.class),
  INTO(ExtendedEnumerable.class, "into", Collection.class),
  SCHEMA_GET_SUB_SCHEMA(Schema.class, "getSubSchema", String.class),
  SCHEMA_GET_TABLE(Schema.class, "getTable", String.class),
  SCHEMA_PLUS_UNWRAP(SchemaPlus.class, "unwrap", Class.class),
  SCHEMAS_QUERYABLE(Schemas.class, "queryable", DataContext.class,
      SchemaPlus.class, Class.class, String.class),
  REFLECTIVE_SCHEMA_GET_TARGET(ReflectiveSchema.class, "getTarget"),
  DATA_CONTEXT_GET(DataContext.class, "get", String.class),
  DATA_CONTEXT_GET_ROOT_SCHEMA(DataContext.class, "getRootSchema"),
  JDBC_SCHEMA_DATA_SOURCE(JdbcSchema.class, "getDataSource"),
  RESULT_SET_ENUMERABLE_OF(ResultSetEnumerable.class, "of", DataSource.class,
      String.class, Function1.class),
  JOIN(ExtendedEnumerable.class, "join", Enumerable.class, Function1.class,
      Function1.class, Function2.class),
  SEMI_JOIN(Enumerables.class, "semiJoin", Enumerable.class, Enumerable.class,
      Function1.class, Function1.class),
  SELECT(ExtendedEnumerable.class, "select", Function1.class),
  SELECT2(ExtendedEnumerable.class, "select", Function2.class),
  SELECT_MANY(ExtendedEnumerable.class, "selectMany", Function1.class),
  WHERE(ExtendedEnumerable.class, "where", Predicate1.class),
  WHERE2(ExtendedEnumerable.class, "where", Predicate2.class),
  DISTINCT(ExtendedEnumerable.class, "distinct"),
  DISTINCT2(ExtendedEnumerable.class, "distinct", EqualityComparer.class),
  GROUP_BY(ExtendedEnumerable.class, "groupBy", Function1.class),
  GROUP_BY2(ExtendedEnumerable.class, "groupBy", Function1.class,
      Function0.class, Function2.class, Function2.class),
  AGGREGATE(ExtendedEnumerable.class, "aggregate", Object.class,
      Function2.class, Function1.class),
  ORDER_BY(ExtendedEnumerable.class, "orderBy", Function1.class,
      Comparator.class),
  UNION(ExtendedEnumerable.class, "union", Enumerable.class),
  CONCAT(ExtendedEnumerable.class, "concat", Enumerable.class),
  INTERSECT(ExtendedEnumerable.class, "intersect", Enumerable.class),
  EXCEPT(ExtendedEnumerable.class, "except", Enumerable.class),
  SKIP(ExtendedEnumerable.class, "skip", int.class),
  TAKE(ExtendedEnumerable.class, "take", int.class),
  SINGLETON_ENUMERABLE(Linq4j.class, "singletonEnumerable", Object.class),
  NULLS_COMPARATOR(Functions.class, "nullsComparator", boolean.class,
      boolean.class),
  ARRAY_COMPARER(Functions.class, "arrayComparer"),
  FUNCTION0_APPLY(Function0.class, "apply"),
  FUNCTION1_APPLY(Function1.class, "apply", Object.class),
  ARRAYS_AS_LIST(FlatLists.class, "of", Object[].class),
  LIST2(FlatLists.class, "of", Object.class, Object.class),
  LIST3(FlatLists.class, "of", Object.class, Object.class, Object.class),
  IDENTITY_COMPARER(Functions.class, "identityComparer"),
  IDENTITY_SELECTOR(Functions.class, "identitySelector"),
  AS_ENUMERABLE(Linq4j.class, "asEnumerable", Object[].class),
  AS_ENUMERABLE2(Linq4j.class, "asEnumerable", Iterable.class),
  ENUMERABLE_TO_LIST(ExtendedEnumerable.class, "toList"),
  LIST_TO_ENUMERABLE(SqlFunctions.class, "listToEnumerable"),
  AS_LIST(Primitive.class, "asList", Object.class),
  ENUMERATOR_CURRENT(Enumerator.class, "current"),
  ENUMERATOR_MOVE_NEXT(Enumerator.class, "moveNext"),
  ENUMERATOR_CLOSE(Enumerator.class, "close"),
  ENUMERATOR_RESET(Enumerator.class, "reset"),
  ENUMERABLE_ENUMERATOR(Enumerable.class, "enumerator"),
  ENUMERABLE_FOREACH(Enumerable.class, "foreach", Function1.class),
  TYPED_GET_ELEMENT_TYPE(Typed.class, "getElementType"),
  BINDABLE_BIND(Bindable.class, "bind", DataContext.class),
  RESULT_SET_GET_DATE2(ResultSet.class, "getDate", int.class, Calendar.class),
  RESULT_SET_GET_TIME2(ResultSet.class, "getTime", int.class, Calendar.class),
  RESULT_SET_GET_TIMESTAMP2(ResultSet.class, "getTimestamp", int.class,
      Calendar.class),
  TIME_ZONE_GET_OFFSET(TimeZone.class, "getOffset", long.class),
  LONG_VALUE(Number.class, "longValue"),
  COMPARATOR_COMPARE(Comparator.class, "compare", Object.class, Object.class),
  COLLECTIONS_REVERSE_ORDER(Collections.class, "reverseOrder"),
  COLLECTIONS_EMPTY_LIST(Collections.class, "emptyList"),
  COLLECTIONS_SINGLETON_LIST(Collections.class, "singletonList", Object.class),
  COLLECTION_SIZE(Collection.class, "size"),
  MAP_CLEAR(Map.class, "clear"),
  MAP_GET(Map.class, "get", Object.class),
  MAP_PUT(Map.class, "put", Object.class, Object.class),
  COLLECTION_ADD(Collection.class, "add", Object.class),
  LIST_GET(List.class, "get", int.class),
  ITERATOR_HAS_NEXT(Iterator.class, "hasNext"),
  ITERATOR_NEXT(Iterator.class, "next"),
  MATH_MAX(Math.class, "max", int.class, int.class),
  MATH_MIN(Math.class, "min", int.class, int.class),
  SORTED_MULTI_MAP_PUT_MULTI(SortedMultiMap.class, "putMulti", Object.class,
      Object.class),
  SORTED_MULTI_MAP_ARRAYS(SortedMultiMap.class, "arrays", Comparator.class),
  SORTED_MULTI_MAP_SINGLETON(SortedMultiMap.class, "singletonArrayIterator",
      Comparator.class, List.class),
  BINARY_SEARCH5_LOWER(BinarySearch.class, "lowerBound", Object[].class,
      Object.class, int.class, int.class, Comparator.class),
  BINARY_SEARCH5_UPPER(BinarySearch.class, "upperBound", Object[].class,
      Object.class, int.class, int.class, Comparator.class),
  BINARY_SEARCH6_LOWER(BinarySearch.class, "lowerBound", Object[].class,
      Object.class, int.class, int.class, Function1.class, Comparator.class),
  BINARY_SEARCH6_UPPER(BinarySearch.class, "upperBound", Object[].class,
      Object.class, int.class, int.class, Function1.class, Comparator.class),
  ARRAY_ITEM(SqlFunctions.class, "arrayItem", List.class, int.class),
  MAP_ITEM(SqlFunctions.class, "mapItem", Map.class, Object.class),
  ANY_ITEM(SqlFunctions.class, "item", Object.class, Object.class),
  UPPER(SqlFunctions.class, "upper", String.class),
  LOWER(SqlFunctions.class, "lower", String.class),
  INITCAP(SqlFunctions.class, "initcap", String.class),
  SUBSTRING(SqlFunctions.class, "substring", String.class, int.class,
      int.class),
  CHAR_LENGTH(SqlFunctions.class, "charLength", String.class),
  STRING_CONCAT(SqlFunctions.class, "concat", String.class, String.class),
  OVERLAY(SqlFunctions.class, "overlay", String.class, String.class, int.class),
  OVERLAY3(SqlFunctions.class, "overlay", String.class, String.class, int.class,
      int.class),
  POSITION(SqlFunctions.class, "position", String.class, String.class),
  TRUNCATE(SqlFunctions.class, "truncate", String.class, int.class),
  TRIM(SqlFunctions.class, "trim", boolean.class, boolean.class, String.class,
      String.class),
  LTRIM(SqlFunctions.class, "ltrim", String.class),
  RTRIM(SqlFunctions.class, "rtrim", String.class),
  LIKE(SqlFunctions.class, "like", String.class, String.class),
  SIMILAR(SqlFunctions.class, "similar", String.class, String.class),
  IS_TRUE(SqlFunctions.class, "isTrue", Boolean.class),
  IS_NOT_FALSE(SqlFunctions.class, "isNotFalse", Boolean.class),
  NOT(SqlFunctions.class, "not", Boolean.class),
  MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION(ModifiableTable.class,
      "getModifiableCollection"),
  STRING_TO_BOOLEAN(SqlFunctions.class, "toBoolean", String.class),
  STRING_TO_DATE(SqlFunctions.class, "dateStringToUnixDate", String.class),
  STRING_TO_TIME(SqlFunctions.class, "timeStringToUnixDate", String.class),
  STRING_TO_TIMESTAMP(SqlFunctions.class, "timestampStringToUnixDate",
      String.class),
  UNIX_DATE_TO_STRING(SqlFunctions.class, "unixDateToString", int.class),
  UNIX_TIME_TO_STRING(SqlFunctions.class, "unixTimeToString", int.class),
  UNIX_TIMESTAMP_TO_STRING(SqlFunctions.class, "unixTimestampToString",
      long.class),
  INTERVAL_YEAR_MONTH_TO_STRING(SqlFunctions.class, "intervalYearMonthToString",
      int.class, SqlFunctions.TimeUnitRange.class),
  INTERVAL_DAY_TIME_TO_STRING(SqlFunctions.class, "intervalDayTimeToString",
      long.class, SqlFunctions.TimeUnitRange.class, int.class),
  UNIX_DATE_EXTRACT(SqlFunctions.class, "unixDateExtract",
      SqlFunctions.TimeUnitRange.class, long.class),
  CURRENT_TIMESTAMP(SqlFunctions.class, "currentTimestamp", DataContext.class),
  CURRENT_TIME(SqlFunctions.class, "currentTime", DataContext.class),
  CURRENT_DATE(SqlFunctions.class, "currentDate", DataContext.class),
  LOCAL_TIMESTAMP(SqlFunctions.class, "localTimestamp", DataContext.class),
  LOCAL_TIME(SqlFunctions.class, "localTime", DataContext.class),
  BOOLEAN_TO_STRING(SqlFunctions.class, "toString", boolean.class),
  JDBC_ARRAY_TO_LIST(SqlFunctions.class, "arrayToList", java.sql.Array.class),
  OBJECT_TO_STRING(Object.class, "toString"),
  OBJECTS_EQUAL(com.google.common.base.Objects.class, "equal", Object.class,
      Object.class),
  ROUND_LONG(SqlFunctions.class, "round", long.class, long.class),
  ROUND_INT(SqlFunctions.class, "round", int.class, int.class),
  DATE_TO_INT(SqlFunctions.class, "toInt", java.util.Date.class),
  DATE_TO_INT_OPTIONAL(SqlFunctions.class, "toIntOptional",
      java.util.Date.class),
  TIME_TO_INT(SqlFunctions.class, "toInt", Time.class),
  TIME_TO_INT_OPTIONAL(SqlFunctions.class, "toIntOptional", Time.class),
  TIMESTAMP_TO_LONG(SqlFunctions.class, "toLong", java.util.Date.class),
  TIMESTAMP_TO_LONG_OFFSET(SqlFunctions.class, "toLong", java.util.Date.class,
      TimeZone.class),
  TIMESTAMP_TO_LONG_OPTIONAL(SqlFunctions.class, "toLongOptional",
      Timestamp.class),
  TIMESTAMP_TO_LONG_OPTIONAL_OFFSET(SqlFunctions.class, "toLongOptional",
      Timestamp.class, TimeZone.class),
  SLICE(SqlFunctions.class, "slice", List.class),
  ELEMENT(SqlFunctions.class, "element", List.class),
  SELECTIVITY(Selectivity.class, "getSelectivity", RexNode.class),
  UNIQUE_KEYS(UniqueKeys.class, "getUniqueKeys", boolean.class),
  COLUMN_UNIQUENESS(ColumnUniqueness.class, "areColumnsUnique", BitSet.class,
      boolean.class),
  ROW_COUNT(RowCount.class, "getRowCount"),
  DISTINCT_ROW_COUNT(DistinctRowCount.class, "getDistinctRowCount",
      BitSet.class, RexNode.class),
  PERCENTAGE_ORIGINAL_ROWS(PercentageOriginalRows.class,
      "getPercentageOriginalRows"),
  POPULATION_SIZE(PopulationSize.class, "getPopulationSize", BitSet.class),
  COLUMN_ORIGIN(ColumnOrigin.class, "getColumnOrigins", int.class),
  CUMULATIVE_COST(CumulativeCost.class, "getCumulativeCost"),
  NON_CUMULATIVE_COST(NonCumulativeCost.class, "getNonCumulativeCost"),
  EXPLAIN_VISIBILITY(ExplainVisibility.class, "isVisibleInExplain",
      SqlExplainLevel.class),
  DATA_CONTEXT_GET_QUERY_PROVIDER(DataContext.class, "getQueryProvider"),
  PREDICATES(Predicates.class, "getPredicates"),
  METADATA_REL(Metadata.class, "rel");

  public final Method method;
  public final Constructor constructor;

  public static final ImmutableMap<Method, BuiltinMethod> MAP;

  static {
    final ImmutableMap.Builder<Method, BuiltinMethod> builder =
        ImmutableMap.builder();
    for (BuiltinMethod value : BuiltinMethod.values()) {
      if (value.method != null) {
        builder.put(value.method, value);
      }
    }
    MAP = builder.build();
  }

  BuiltinMethod(Class clazz, String methodName, Class... argumentTypes) {
    this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
    this.constructor = null;
  }

  BuiltinMethod(Class clazz, Class... argumentTypes) {
    this.method = null;
    this.constructor = Types.lookupConstructor(clazz, argumentTypes);
  }
}

// End BuiltinMethod.java
