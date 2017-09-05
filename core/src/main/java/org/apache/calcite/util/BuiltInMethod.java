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
package org.apache.calcite.util;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.interpreter.Scalar;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.CorrelateJoinType;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.ExtendedEnumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.metadata.BuiltInMetadata.AllPredicates;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Collation;
import org.apache.calcite.rel.metadata.BuiltInMetadata.ColumnOrigin;
import org.apache.calcite.rel.metadata.BuiltInMetadata.ColumnUniqueness;
import org.apache.calcite.rel.metadata.BuiltInMetadata.CumulativeCost;
import org.apache.calcite.rel.metadata.BuiltInMetadata.DistinctRowCount;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Distribution;
import org.apache.calcite.rel.metadata.BuiltInMetadata.ExplainVisibility;
import org.apache.calcite.rel.metadata.BuiltInMetadata.ExpressionLineage;
import org.apache.calcite.rel.metadata.BuiltInMetadata.MaxRowCount;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Memory;
import org.apache.calcite.rel.metadata.BuiltInMetadata.MinRowCount;
import org.apache.calcite.rel.metadata.BuiltInMetadata.NodeTypes;
import org.apache.calcite.rel.metadata.BuiltInMetadata.NonCumulativeCost;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Parallelism;
import org.apache.calcite.rel.metadata.BuiltInMetadata.PercentageOriginalRows;
import org.apache.calcite.rel.metadata.BuiltInMetadata.PopulationSize;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Predicates;
import org.apache.calcite.rel.metadata.BuiltInMetadata.RowCount;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Selectivity;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Size;
import org.apache.calcite.rel.metadata.BuiltInMetadata.TableReferences;
import org.apache.calcite.rel.metadata.BuiltInMetadata.UniqueKeys;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.BinarySearch;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Enumerables;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.RandomFunction;
import org.apache.calcite.runtime.ResultSetEnumerable;
import org.apache.calcite.runtime.SortedMultiMap;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.runtime.SqlFunctions.FlatProductInputType;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlExplainLevel;

import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

import javax.sql.DataSource;

/**
 * Built-in methods.
 */
public enum BuiltInMethod {
  QUERYABLE_SELECT(Queryable.class, "select", FunctionExpression.class),
  QUERYABLE_AS_ENUMERABLE(Queryable.class, "asEnumerable"),
  QUERYABLE_TABLE_AS_QUERYABLE(QueryableTable.class, "asQueryable",
      QueryProvider.class, SchemaPlus.class, String.class),
  AS_QUERYABLE(Enumerable.class, "asQueryable"),
  ABSTRACT_ENUMERABLE_CTOR(AbstractEnumerable.class),
  INTO(ExtendedEnumerable.class, "into", Collection.class),
  REMOVE_ALL(ExtendedEnumerable.class, "removeAll", Collection.class),
  SCHEMA_GET_SUB_SCHEMA(Schema.class, "getSubSchema", String.class),
  SCHEMA_GET_TABLE(Schema.class, "getTable", String.class),
  SCHEMA_PLUS_UNWRAP(SchemaPlus.class, "unwrap", Class.class),
  SCHEMAS_ENUMERABLE_SCANNABLE(Schemas.class, "enumerable",
      ScannableTable.class, DataContext.class),
  SCHEMAS_ENUMERABLE_FILTERABLE(Schemas.class, "enumerable",
      FilterableTable.class, DataContext.class),
  SCHEMAS_ENUMERABLE_PROJECTABLE_FILTERABLE(Schemas.class, "enumerable",
      ProjectableFilterableTable.class, DataContext.class),
  SCHEMAS_QUERYABLE(Schemas.class, "queryable", DataContext.class,
      SchemaPlus.class, Class.class, String.class),
  REFLECTIVE_SCHEMA_GET_TARGET(ReflectiveSchema.class, "getTarget"),
  DATA_CONTEXT_GET(DataContext.class, "get", String.class),
  DATA_CONTEXT_GET_ROOT_SCHEMA(DataContext.class, "getRootSchema"),
  JDBC_SCHEMA_DATA_SOURCE(JdbcSchema.class, "getDataSource"),
  ROW_VALUE(Row.class, "getObject", int.class),
  ROW_AS_COPY(Row.class, "asCopy", Object[].class),
  RESULT_SET_ENUMERABLE_OF(ResultSetEnumerable.class, "of", DataSource.class,
      String.class, Function1.class),
  JOIN(ExtendedEnumerable.class, "join", Enumerable.class, Function1.class,
      Function1.class, Function2.class),
  MERGE_JOIN(EnumerableDefaults.class, "mergeJoin", Enumerable.class,
      Enumerable.class, Function1.class, Function1.class, Function2.class,
      boolean.class, boolean.class),
  SLICE0(Enumerables.class, "slice0", Enumerable.class),
  SEMI_JOIN(EnumerableDefaults.class, "semiJoin", Enumerable.class,
      Enumerable.class, Function1.class, Function1.class),
  THETA_JOIN(EnumerableDefaults.class, "thetaJoin", Enumerable.class,
      Enumerable.class, Predicate2.class, Function2.class, boolean.class,
      boolean.class),
  CORRELATE_JOIN(ExtendedEnumerable.class, "correlateJoin",
      CorrelateJoinType.class, Function1.class, Function2.class),
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
  GROUP_BY_MULTIPLE(EnumerableDefaults.class, "groupByMultiple",
      Enumerable.class, List.class, Function0.class, Function2.class,
      Function2.class),
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
  EMPTY_ENUMERABLE(Linq4j.class, "emptyEnumerable"),
  NULLS_COMPARATOR(Functions.class, "nullsComparator", boolean.class,
      boolean.class),
  ARRAY_COMPARER(Functions.class, "arrayComparer"),
  FUNCTION0_APPLY(Function0.class, "apply"),
  FUNCTION1_APPLY(Function1.class, "apply", Object.class),
  ARRAYS_AS_LIST(Arrays.class, "asList", Object[].class),
  ARRAY(SqlFunctions.class, "array", Object[].class),
  FLAT_PRODUCT(SqlFunctions.class, "flatProduct", int[].class, boolean.class,
      FlatProductInputType[].class),
  LIST_N(FlatLists.class, "copyOf", Comparable[].class),
  LIST2(FlatLists.class, "of", Object.class, Object.class),
  LIST3(FlatLists.class, "of", Object.class, Object.class, Object.class),
  LIST4(FlatLists.class, "of", Object.class, Object.class, Object.class,
      Object.class),
  LIST5(FlatLists.class, "of", Object.class, Object.class, Object.class,
      Object.class, Object.class),
  LIST6(FlatLists.class, "of", Object.class, Object.class, Object.class,
      Object.class, Object.class, Object.class),
  COMPARABLE_EMPTY_LIST(FlatLists.class, "COMPARABLE_EMPTY_LIST", true),
  IDENTITY_COMPARER(Functions.class, "identityComparer"),
  IDENTITY_SELECTOR(Functions.class, "identitySelector"),
  AS_ENUMERABLE(Linq4j.class, "asEnumerable", Object[].class),
  AS_ENUMERABLE2(Linq4j.class, "asEnumerable", Iterable.class),
  ENUMERABLE_TO_LIST(ExtendedEnumerable.class, "toList"),
  AS_LIST(Primitive.class, "asList", Object.class),
  ENUMERATOR_CURRENT(Enumerator.class, "current"),
  ENUMERATOR_MOVE_NEXT(Enumerator.class, "moveNext"),
  ENUMERATOR_CLOSE(Enumerator.class, "close"),
  ENUMERATOR_RESET(Enumerator.class, "reset"),
  ENUMERABLE_ENUMERATOR(Enumerable.class, "enumerator"),
  ENUMERABLE_FOREACH(Enumerable.class, "foreach", Function1.class),
  TYPED_GET_ELEMENT_TYPE(ArrayBindable.class, "getElementType"),
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
  ARRAY_ITEM(SqlFunctions.class, "arrayItemOptional", List.class, int.class),
  MAP_ITEM(SqlFunctions.class, "mapItemOptional", Map.class, Object.class),
  ANY_ITEM(SqlFunctions.class, "itemOptional", Object.class, Object.class),
  UPPER(SqlFunctions.class, "upper", String.class),
  LOWER(SqlFunctions.class, "lower", String.class),
  INITCAP(SqlFunctions.class, "initcap", String.class),
  SUBSTRING(SqlFunctions.class, "substring", String.class, int.class,
      int.class),
  CHAR_LENGTH(SqlFunctions.class, "charLength", String.class),
  STRING_CONCAT(SqlFunctions.class, "concat", String.class, String.class),
  FLOOR_DIV(DateTimeUtils.class, "floorDiv", long.class, long.class),
  FLOOR_MOD(DateTimeUtils.class, "floorMod", long.class, long.class),
  ADD_MONTHS(SqlFunctions.class, "addMonths", long.class, int.class),
  ADD_MONTHS_INT(SqlFunctions.class, "addMonths", int.class, int.class),
  SUBTRACT_MONTHS(SqlFunctions.class, "subtractMonths", long.class,
      long.class),
  FLOOR(SqlFunctions.class, "floor", int.class, int.class),
  CEIL(SqlFunctions.class, "ceil", int.class, int.class),
  OVERLAY(SqlFunctions.class, "overlay", String.class, String.class, int.class),
  OVERLAY3(SqlFunctions.class, "overlay", String.class, String.class, int.class,
      int.class),
  POSITION(SqlFunctions.class, "position", String.class, String.class),
  RAND(RandomFunction.class, "rand"),
  RAND_SEED(RandomFunction.class, "randSeed", int.class),
  RAND_INTEGER(RandomFunction.class, "randInteger", int.class),
  RAND_INTEGER_SEED(RandomFunction.class, "randIntegerSeed", int.class,
      int.class),
  TRUNCATE(SqlFunctions.class, "truncate", String.class, int.class),
  TRUNCATE_OR_PAD(SqlFunctions.class, "truncateOrPad", String.class, int.class),
  TRIM(SqlFunctions.class, "trim", boolean.class, boolean.class, String.class,
      String.class),
  REPLACE(SqlFunctions.class, "replace", String.class, String.class,
      String.class),
  TRANSLATE3(SqlFunctions.class, "translate3", String.class, String.class, String.class),
  LTRIM(SqlFunctions.class, "ltrim", String.class),
  RTRIM(SqlFunctions.class, "rtrim", String.class),
  LIKE(SqlFunctions.class, "like", String.class, String.class),
  SIMILAR(SqlFunctions.class, "similar", String.class, String.class),
  IS_TRUE(SqlFunctions.class, "isTrue", Boolean.class),
  IS_NOT_FALSE(SqlFunctions.class, "isNotFalse", Boolean.class),
  NOT(SqlFunctions.class, "not", Boolean.class),
  LESSER(SqlFunctions.class, "lesser", Comparable.class, Comparable.class),
  GREATER(SqlFunctions.class, "greater", Comparable.class, Comparable.class),
  MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION(ModifiableTable.class,
      "getModifiableCollection"),
  SCANNABLE_TABLE_SCAN(ScannableTable.class, "scan", DataContext.class),
  STRING_TO_BOOLEAN(SqlFunctions.class, "toBoolean", String.class),
  INTERNAL_TO_DATE(SqlFunctions.class, "internalToDate", int.class),
  INTERNAL_TO_TIME(SqlFunctions.class, "internalToTime", int.class),
  INTERNAL_TO_TIMESTAMP(SqlFunctions.class, "internalToTimestamp", long.class),
  STRING_TO_DATE(DateTimeUtils.class, "dateStringToUnixDate", String.class),
  STRING_TO_TIME(DateTimeUtils.class, "timeStringToUnixDate", String.class),
  STRING_TO_TIMESTAMP(DateTimeUtils.class, "timestampStringToUnixDate", String.class),
  STRING_TO_TIME_WITH_LOCAL_TIME_ZONE(SqlFunctions.class, "toTimeWithLocalTimeZone",
      String.class),
  TIME_STRING_TO_TIME_WITH_LOCAL_TIME_ZONE(SqlFunctions.class, "toTimeWithLocalTimeZone",
      String.class, TimeZone.class),
  STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE(SqlFunctions.class, "toTimestampWithLocalTimeZone",
      String.class),
  TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE(SqlFunctions.class,
      "toTimestampWithLocalTimeZone", String.class, TimeZone.class),
  TIME_WITH_LOCAL_TIME_ZONE_TO_TIME(SqlFunctions.class, "timeWithLocalTimeZoneToTime",
      int.class, TimeZone.class),
  TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP(SqlFunctions.class, "timeWithLocalTimeZoneToTimestamp",
      String.class, int.class, TimeZone.class),
  TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE(SqlFunctions.class,
      "timeWithLocalTimeZoneToTimestampWithLocalTimeZone", String.class, int.class),
  TIME_WITH_LOCAL_TIME_ZONE_TO_STRING(SqlFunctions.class, "timeWithLocalTimeZoneToString",
      int.class, TimeZone.class),
  TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE(SqlFunctions.class, "timestampWithLocalTimeZoneToDate",
      long.class, TimeZone.class),
  TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME(SqlFunctions.class, "timestampWithLocalTimeZoneToTime",
      long.class, TimeZone.class),
  TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME_WITH_LOCAL_TIME_ZONE(SqlFunctions.class,
      "timestampWithLocalTimeZoneToTimeWithLocalTimeZone", long.class),
  TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP(SqlFunctions.class,
      "timestampWithLocalTimeZoneToTimestamp", long.class, TimeZone.class),
  TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_STRING(SqlFunctions.class,
      "timestampWithLocalTimeZoneToString", long.class, TimeZone.class),
  UNIX_DATE_TO_STRING(DateTimeUtils.class, "unixDateToString", int.class),
  UNIX_TIME_TO_STRING(DateTimeUtils.class, "unixTimeToString", int.class),
  UNIX_TIMESTAMP_TO_STRING(DateTimeUtils.class, "unixTimestampToString",
      long.class),
  INTERVAL_YEAR_MONTH_TO_STRING(DateTimeUtils.class,
      "intervalYearMonthToString", int.class, TimeUnitRange.class),
  INTERVAL_DAY_TIME_TO_STRING(DateTimeUtils.class, "intervalDayTimeToString",
      long.class, TimeUnitRange.class, int.class),
  UNIX_DATE_EXTRACT(DateTimeUtils.class, "unixDateExtract",
      TimeUnitRange.class, long.class),
  UNIX_DATE_FLOOR(DateTimeUtils.class, "unixDateFloor",
      TimeUnitRange.class, int.class),
  UNIX_DATE_CEIL(DateTimeUtils.class, "unixDateCeil",
      TimeUnitRange.class, int.class),
  UNIX_TIMESTAMP_FLOOR(DateTimeUtils.class, "unixTimestampFloor",
      TimeUnitRange.class, long.class),
  UNIX_TIMESTAMP_CEIL(DateTimeUtils.class, "unixTimestampCeil",
      TimeUnitRange.class, long.class),
  CURRENT_TIMESTAMP(SqlFunctions.class, "currentTimestamp", DataContext.class),
  CURRENT_TIME(SqlFunctions.class, "currentTime", DataContext.class),
  CURRENT_DATE(SqlFunctions.class, "currentDate", DataContext.class),
  LOCAL_TIMESTAMP(SqlFunctions.class, "localTimestamp", DataContext.class),
  LOCAL_TIME(SqlFunctions.class, "localTime", DataContext.class),
  TIME_ZONE(SqlFunctions.class, "timeZone", DataContext.class),
  BOOLEAN_TO_STRING(SqlFunctions.class, "toString", boolean.class),
  JDBC_ARRAY_TO_LIST(SqlFunctions.class, "arrayToList", java.sql.Array.class),
  OBJECT_TO_STRING(Object.class, "toString"),
  OBJECTS_EQUAL(Objects.class, "equals", Object.class, Object.class),
  HASH(Utilities.class, "hash", int.class, Object.class),
  COMPARE(Utilities.class, "compare", Comparable.class, Comparable.class),
  COMPARE_NULLS_FIRST(Utilities.class, "compareNullsFirst", Comparable.class,
      Comparable.class),
  COMPARE_NULLS_LAST(Utilities.class, "compareNullsLast", Comparable.class,
      Comparable.class),
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
  SEQUENCE_CURRENT_VALUE(SqlFunctions.class, "sequenceCurrentValue",
      String.class),
  SEQUENCE_NEXT_VALUE(SqlFunctions.class, "sequenceNextValue", String.class),
  SLICE(SqlFunctions.class, "slice", List.class),
  ELEMENT(SqlFunctions.class, "element", List.class),
  SELECTIVITY(Selectivity.class, "getSelectivity", RexNode.class),
  UNIQUE_KEYS(UniqueKeys.class, "getUniqueKeys", boolean.class),
  AVERAGE_ROW_SIZE(Size.class, "averageRowSize"),
  AVERAGE_COLUMN_SIZES(Size.class, "averageColumnSizes"),
  IS_PHASE_TRANSITION(Parallelism.class, "isPhaseTransition"),
  SPLIT_COUNT(Parallelism.class, "splitCount"),
  MEMORY(Memory.class, "memory"),
  CUMULATIVE_MEMORY_WITHIN_PHASE(Memory.class, "cumulativeMemoryWithinPhase"),
  CUMULATIVE_MEMORY_WITHIN_PHASE_SPLIT(Memory.class,
      "cumulativeMemoryWithinPhaseSplit"),
  COLUMN_UNIQUENESS(ColumnUniqueness.class, "areColumnsUnique",
      ImmutableBitSet.class, boolean.class),
  COLLATIONS(Collation.class, "collations"),
  DISTRIBUTION(Distribution.class, "distribution"),
  NODE_TYPES(NodeTypes.class, "getNodeTypes"),
  ROW_COUNT(RowCount.class, "getRowCount"),
  MAX_ROW_COUNT(MaxRowCount.class, "getMaxRowCount"),
  MIN_ROW_COUNT(MinRowCount.class, "getMinRowCount"),
  DISTINCT_ROW_COUNT(DistinctRowCount.class, "getDistinctRowCount",
      ImmutableBitSet.class, RexNode.class),
  PERCENTAGE_ORIGINAL_ROWS(PercentageOriginalRows.class,
      "getPercentageOriginalRows"),
  POPULATION_SIZE(PopulationSize.class, "getPopulationSize",
      ImmutableBitSet.class),
  COLUMN_ORIGIN(ColumnOrigin.class, "getColumnOrigins", int.class),
  EXPRESSION_LINEAGE(ExpressionLineage.class, "getExpressionLineage", RexNode.class),
  TABLE_REFERENCES(TableReferences.class, "getTableReferences"),
  CUMULATIVE_COST(CumulativeCost.class, "getCumulativeCost"),
  NON_CUMULATIVE_COST(NonCumulativeCost.class, "getNonCumulativeCost"),
  PREDICATES(Predicates.class, "getPredicates"),
  ALL_PREDICATES(AllPredicates.class, "getAllPredicates"),
  EXPLAIN_VISIBILITY(ExplainVisibility.class, "isVisibleInExplain",
      SqlExplainLevel.class),
  SCALAR_EXECUTE1(Scalar.class, "execute", Context.class),
  SCALAR_EXECUTE2(Scalar.class, "execute", Context.class, Object[].class),
  CONTEXT_VALUES(Context.class, "values", true),
  CONTEXT_ROOT(Context.class, "root", true),
  DATA_CONTEXT_GET_QUERY_PROVIDER(DataContext.class, "getQueryProvider"),
  METADATA_REL(Metadata.class, "rel");

  public final Method method;
  public final Constructor constructor;
  public final Field field;

  public static final ImmutableMap<Method, BuiltInMethod> MAP;

  static {
    final ImmutableMap.Builder<Method, BuiltInMethod> builder =
        ImmutableMap.builder();
    for (BuiltInMethod value : BuiltInMethod.values()) {
      if (value.method != null) {
        builder.put(value.method, value);
      }
    }
    MAP = builder.build();
  }

  BuiltInMethod(Method method, Constructor constructor, Field field) {
    this.method = method;
    this.constructor = constructor;
    this.field = field;
  }

  /** Defines a method. */
  BuiltInMethod(Class clazz, String methodName, Class... argumentTypes) {
    this(Types.lookupMethod(clazz, methodName, argumentTypes), null, null);
  }

  /** Defines a constructor. */
  BuiltInMethod(Class clazz, Class... argumentTypes) {
    this(null, Types.lookupConstructor(clazz, argumentTypes), null);
  }

  /** Defines a field. */
  BuiltInMethod(Class clazz, String fieldName, boolean dummy) {
    this(null, null, Types.lookupField(clazz, fieldName));
    assert dummy : "dummy value for method overloading must be true";
  }
}

// End BuiltInMethod.java
