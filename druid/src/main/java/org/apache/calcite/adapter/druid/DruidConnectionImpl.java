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
package org.apache.calcite.adapter.druid;

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.interpreter.Sink;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Interval;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.calcite.runtime.HttpUtils.post;
import static org.apache.calcite.util.DateTimeStringUtils.ISO_DATETIME_FRACTIONAL_SECOND_FORMAT;
import static org.apache.calcite.util.DateTimeStringUtils.getDateFormatter;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link DruidConnection}.
 */
class DruidConnectionImpl implements DruidConnection {
  private final String url;
  private final String coordinatorUrl;

  public static final String DEFAULT_RESPONSE_TIMESTAMP_COLUMN = "timestamp";
  private static final SimpleDateFormat UTC_TIMESTAMP_FORMAT;
  private static final SimpleDateFormat TIMESTAMP_FORMAT;

  static {
    UTC_TIMESTAMP_FORMAT =
        getDateFormatter(ISO_DATETIME_FRACTIONAL_SECOND_FORMAT);
    TIMESTAMP_FORMAT = getDateFormatter(DateTimeUtils.TIMESTAMP_FORMAT_STRING);
  }

  DruidConnectionImpl(String url, String coordinatorUrl) {
    this.url = requireNonNull(url, "url");
    this.coordinatorUrl = requireNonNull(coordinatorUrl, "coordinatorUrl");
  }

  /** Executes a query request.
   *
   * @param queryType Query type
   * @param data Data to post
   * @param sink Sink to which to send the parsed rows
   * @param fieldNames Names of fields
   * @param fieldTypes Types of fields (never null, but elements may be null)
   * @param page Page definition (in/out)
   */
  public void request(QueryType queryType, String data, Sink sink,
      List<String> fieldNames, List<ColumnMetaData.Rep> fieldTypes,
      Page page) {
    final String url = this.url + "/druid/v2/?pretty";
    final Map<String, String> requestHeaders =
        ImmutableMap.of("Content-Type", "application/json");
    if (CalciteSystemProperty.DEBUG.value()) {
      System.out.println(data);
    }
    try (InputStream in0 = post(url, data, requestHeaders, 10000, 1800000);
         InputStream in = traceResponse(in0)) {
      parse(queryType, in, sink, fieldNames, fieldTypes, page);
    } catch (IOException e) {
      throw new RuntimeException("Error while processing druid request ["
          + data + "]", e);
    }
  }

  /** Parses the output of a query, sending the results to a
   * {@link Sink}. */
  private static void parse(QueryType queryType, InputStream in, Sink sink,
      List<String> fieldNames, List<ColumnMetaData.Rep> fieldTypes, Page page) {
    final JsonFactory factory = new JsonFactory();
    final Row.RowBuilder rowBuilder = Row.newBuilder(fieldNames.size());

    if (CalciteSystemProperty.DEBUG.value()) {
      try {
        final byte[] bytes = AvaticaUtils.readFullyToBytes(in);
        System.out.println("Response: "
            + new String(bytes, StandardCharsets.UTF_8)); // CHECKSTYLE: IGNORE 0
        in = new ByteArrayInputStream(bytes);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    int posTimestampField = -1;
    for (int i = 0; i < fieldTypes.size(); i++) {
      /*@TODO This need to be revisited. The logic seems implying that only
      one column of type timestamp is present, this is not necessarily true,
      see https://issues.apache.org/jira/browse/CALCITE-2175
      */
      if (fieldTypes.get(i) == ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP) {
        posTimestampField = i;
        break;
      }
    }

    try (JsonParser parser = factory.createParser(in)) {
      switch (queryType) {
      case TIMESERIES:
        if (parser.nextToken() == JsonToken.START_ARRAY) {
          while (parser.nextToken() == JsonToken.START_OBJECT) {
           // loop until token equal to "}"
            final Long timeValue = extractTimestampField(parser);
            if (parser.nextToken() == JsonToken.FIELD_NAME
                    && parser.currentName().equals("result")
                    && parser.nextToken() == JsonToken.START_OBJECT) {
              if (posTimestampField != -1) {
                rowBuilder.set(posTimestampField, timeValue);
              }
              parseFields(fieldNames, fieldTypes, rowBuilder, parser);
              sink.send(rowBuilder.build());
              rowBuilder.reset();
            }
            expect(parser, JsonToken.END_OBJECT);
          }
        }
        break;

      case TOP_N:
        if (parser.nextToken() == JsonToken.START_ARRAY
            && parser.nextToken() == JsonToken.START_OBJECT) {
          final Long timeValue = extractTimestampField(parser);
          if (parser.nextToken() == JsonToken.FIELD_NAME
              && parser.currentName().equals("result")
              && parser.nextToken() == JsonToken.START_ARRAY) {
            while (parser.nextToken() == JsonToken.START_OBJECT) {
              // loop until token equal to "}"
              if (posTimestampField != -1) {
                rowBuilder.set(posTimestampField, timeValue);
              }
              parseFields(fieldNames, fieldTypes, rowBuilder, parser);
              sink.send(rowBuilder.build());
              rowBuilder.reset();
            }
          }
        }
        break;

      case SELECT:
        if (parser.nextToken() == JsonToken.START_ARRAY
            && parser.nextToken() == JsonToken.START_OBJECT) {
          page.pagingIdentifier = null;
          page.offset = -1;
          page.totalRowCount = 0;
          expectScalarField(parser, DEFAULT_RESPONSE_TIMESTAMP_COLUMN);
          if (parser.nextToken() == JsonToken.FIELD_NAME
              && parser.currentName().equals("result")
              && parser.nextToken() == JsonToken.START_OBJECT) {
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
              if (parser.currentName().equals("pagingIdentifiers")
                  && parser.nextToken() == JsonToken.START_OBJECT) {
                JsonToken token = parser.nextToken();
                while (parser.getCurrentToken() == JsonToken.FIELD_NAME) {
                  page.pagingIdentifier = parser.currentName();
                  if (parser.nextToken() == JsonToken.VALUE_NUMBER_INT) {
                    page.offset = parser.getIntValue();
                  }
                  token = parser.nextToken();
                }
                expect(token, JsonToken.END_OBJECT);
              } else if (parser.currentName().equals("events")
                  && parser.nextToken() == JsonToken.START_ARRAY) {
                while (parser.nextToken() == JsonToken.START_OBJECT) {
                  expectScalarField(parser, "segmentId");
                  expectScalarField(parser, "offset");
                  if (parser.nextToken() == JsonToken.FIELD_NAME
                      && parser.currentName().equals("event")
                      && parser.nextToken() == JsonToken.START_OBJECT) {
                    parseFields(fieldNames, fieldTypes, posTimestampField, rowBuilder, parser);
                    sink.send(rowBuilder.build());
                    rowBuilder.reset();
                    page.totalRowCount += 1;
                  }
                  expect(parser, JsonToken.END_OBJECT);
                }
                parser.nextToken();
              } else if (parser.currentName().equals("dimensions")
                  || parser.currentName().equals("metrics")) {
                expect(parser, JsonToken.START_ARRAY);
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                  // empty
                }
              }
            }
          }
        }
        break;

      case GROUP_BY:
        if (parser.nextToken() == JsonToken.START_ARRAY) {
          while (parser.nextToken() == JsonToken.START_OBJECT) {
            expectScalarField(parser, "version");
            final Long timeValue = extractTimestampField(parser);
            if (parser.nextToken() == JsonToken.FIELD_NAME
                && parser.currentName().equals("event")
                && parser.nextToken() == JsonToken.START_OBJECT) {
              if (posTimestampField != -1) {
                rowBuilder.set(posTimestampField, timeValue);
              }
              parseFields(fieldNames, fieldTypes, posTimestampField, rowBuilder, parser);
              sink.send(rowBuilder.build());
              rowBuilder.reset();
            }
            expect(parser, JsonToken.END_OBJECT);
          }
        }
        break;

      case SCAN:
        if (parser.nextToken() == JsonToken.START_ARRAY) {
          while (parser.nextToken() == JsonToken.START_OBJECT) {
            expectScalarField(parser, "segmentId");

            expect(parser, JsonToken.FIELD_NAME);
            if (parser.currentName().equals("columns")) {
              expect(parser, JsonToken.START_ARRAY);
              while (parser.nextToken() != JsonToken.END_ARRAY) {
                // Skip the columns list
              }
            }
            if (parser.nextToken() == JsonToken.FIELD_NAME
                && parser.currentName().equals("events")
                && parser.nextToken() == JsonToken.START_ARRAY) {
              // Events is Array of Arrays where each array is a row
              while (parser.nextToken() == JsonToken.START_ARRAY) {
                for (String field : fieldNames) {
                  parseFieldForName(fieldNames, fieldTypes, posTimestampField, rowBuilder, parser,
                      field);
                }
                expect(parser, JsonToken.END_ARRAY);
                Row row = rowBuilder.build();
                sink.send(row);
                rowBuilder.reset();
                page.totalRowCount += 1;
              }
            }
            expect(parser, JsonToken.END_OBJECT);
          }
        }
        break;
      default:
        break;
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void parseFields(List<String> fieldNames, List<ColumnMetaData.Rep> fieldTypes,
      Row.RowBuilder rowBuilder, JsonParser parser) throws IOException {
    parseFields(fieldNames, fieldTypes, -1, rowBuilder, parser);
  }

  private static void parseFields(List<String> fieldNames, List<ColumnMetaData.Rep> fieldTypes,
      int posTimestampField, Row.RowBuilder rowBuilder, JsonParser parser) throws IOException {
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      parseField(fieldNames, fieldTypes, posTimestampField, rowBuilder, parser);
    }
  }

  private static void parseField(List<String> fieldNames, List<ColumnMetaData.Rep> fieldTypes,
      int posTimestampField, Row.RowBuilder rowBuilder, JsonParser parser) throws IOException {
    final String fieldName = parser.currentName();
    parseFieldForName(fieldNames, fieldTypes, posTimestampField, rowBuilder, parser, fieldName);
  }

  @SuppressWarnings("JavaUtilDate")
  private static void parseFieldForName(List<String> fieldNames,
      List<ColumnMetaData.Rep> fieldTypes,
      int posTimestampField, Row.RowBuilder rowBuilder, JsonParser parser, String fieldName)
      throws IOException {
    // Move to next token, which is name's value
    JsonToken token = parser.nextToken();

    boolean isTimestampColumn = fieldName.equals(DEFAULT_RESPONSE_TIMESTAMP_COLUMN);
    int i = fieldNames.indexOf(fieldName);
    ColumnMetaData.Rep type = null;
    if (i < 0) {
      if (!isTimestampColumn) {
        // Field not present
        return;
      }
    } else {
      type = fieldTypes.get(i);
    }

    if (isTimestampColumn || ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP == type) {
      final int fieldPos = posTimestampField != -1 ? posTimestampField : i;
      if (token == JsonToken.VALUE_NUMBER_INT) {
        rowBuilder.set(posTimestampField, parser.getLongValue());
        return;
      } else {
        // We don't have any way to figure out the format of time upfront since we only have
        // org.apache.calcite.avatica.ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP as type to represent
        // both timestamp and timestamp with local timezone.
        // Logic where type is inferred can be found at DruidQuery.DruidQueryNode.getPrimitive()
        // Thus need to guess via try and catch
        synchronized (UTC_TIMESTAMP_FORMAT) {
          // synchronized block to avoid race condition
          try {
            // First try to parse as Timestamp with timezone.
            rowBuilder
                .set(fieldPos, UTC_TIMESTAMP_FORMAT.parse(parser.getText()).getTime());
          } catch (ParseException e) {
            // swallow the exception and try timestamp format
            try {
              rowBuilder
                  .set(fieldPos, TIMESTAMP_FORMAT.parse(parser.getText()).getTime());
            } catch (ParseException e2) {
              // unknown format should not happen
              throw new RuntimeException(e2);
            }
          }
        }
        return;
      }
    }

    switch (token) {
    case VALUE_NUMBER_INT:
      if (type == null) {
        type = ColumnMetaData.Rep.LONG;
      }
      // fall through
    case VALUE_NUMBER_FLOAT:
      if (type == null) {
        // JSON's "float" is 64-bit floating point
        type = ColumnMetaData.Rep.DOUBLE;
      }
      switch (type) {
      case BYTE:
        rowBuilder.set(i, parser.getByteValue());
        break;
      case SHORT:
        rowBuilder.set(i, parser.getShortValue());
        break;
      case INTEGER:
        rowBuilder.set(i, parser.getIntValue());
        break;
      case LONG:
        rowBuilder.set(i, parser.getLongValue());
        break;
      case DOUBLE:
        rowBuilder.set(i, parser.getDoubleValue());
        break;
      default:
        break;
      }
      break;
    case VALUE_TRUE:
      rowBuilder.set(i, true);
      break;
    case VALUE_FALSE:
      rowBuilder.set(i, false);
      break;
    case VALUE_NULL:
      break;
    case VALUE_STRING:
    default:
      final String s = parser.getText();
      if (type != null) {
        switch (type) {
        case LONG:
        case PRIMITIVE_LONG:
        case SHORT:
        case PRIMITIVE_SHORT:
        case INTEGER:
        case PRIMITIVE_INT:
          switch (s) {
          case "Infinity":
          case "-Infinity":
          case "NaN":
            throw new RuntimeException("/ by zero");
          default:
            break;
          }
          rowBuilder.set(i, Long.valueOf(s));
          break;
        case FLOAT:
        case PRIMITIVE_FLOAT:
        case PRIMITIVE_DOUBLE:
        case NUMBER:
        case DOUBLE:
          switch (s) {
          case "Infinity":
            rowBuilder.set(i, Double.POSITIVE_INFINITY);
            return;
          case "-Infinity":
            rowBuilder.set(i, Double.NEGATIVE_INFINITY);
            return;
          case "NaN":
            rowBuilder.set(i, Double.NaN);
            return;
          default:
            break;
          }
          rowBuilder.set(i, Double.valueOf(s));
          break;
        default:
          break;
        }
      } else {
        rowBuilder.set(i, s);
      }
    }
  }

  private static void expect(JsonParser parser, JsonToken token) throws IOException {
    expect(parser.nextToken(), token);
  }

  private static void expect(JsonToken token, JsonToken expected) {
    if (token != expected) {
      throw new RuntimeException("expected " + expected + ", got " + token);
    }
  }

  private static void expectScalarField(JsonParser parser, String name)
      throws IOException {
    expect(parser, JsonToken.FIELD_NAME);
    if (!parser.currentName().equals(name)) {
      throw new RuntimeException("expected field " + name + ", got "
          + parser.currentName());
    }
    final JsonToken t = parser.nextToken();
    switch (t) {
    case VALUE_NULL:
    case VALUE_FALSE:
    case VALUE_TRUE:
    case VALUE_NUMBER_INT:
    case VALUE_NUMBER_FLOAT:
    case VALUE_STRING:
      break;
    default:
      throw new RuntimeException("expected scalar field, got  " + t);
    }
  }

  @SuppressWarnings("unused")
  private static void expectObjectField(JsonParser parser, String name)
      throws IOException {
    expect(parser, JsonToken.FIELD_NAME);
    if (!parser.currentName().equals(name)) {
      throw new RuntimeException("expected field " + name + ", got "
          + parser.currentName());
    }
    expect(parser, JsonToken.START_OBJECT);
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      // empty
    }
  }

  @SuppressWarnings("JavaUtilDate")
  private static @Nullable Long extractTimestampField(JsonParser parser)
      throws IOException {
    expect(parser, JsonToken.FIELD_NAME);
    if (!parser.currentName().equals(DEFAULT_RESPONSE_TIMESTAMP_COLUMN)) {
      throw new RuntimeException("expected field " + DEFAULT_RESPONSE_TIMESTAMP_COLUMN + ", got "
          + parser.currentName());
    }
    parser.nextToken();
    try {
      final Date parse;
      // synchronized block to avoid race condition
      synchronized (UTC_TIMESTAMP_FORMAT) {
        parse = UTC_TIMESTAMP_FORMAT.parse(parser.getText());
      }
      return parse.getTime();
    } catch (ParseException e) {
      // ignore bad value
    }
    return null;
  }

  /** Executes a request and returns the resulting rows as an
   * {@link Enumerable}, running the parser in a thread provided by
   * {@code service}. */
  public Enumerable<Row> enumerable(final QueryType queryType,
      final String request, final List<String> fieldNames,
      final ExecutorService service)
      throws IOException {
    return new AbstractEnumerable<Row>() {
      @Override public Enumerator<Row> enumerator() {
        final BlockingQueueEnumerator<Row> enumerator =
            new BlockingQueueEnumerator<>();
        final RunnableQueueSink sink = new RunnableQueueSink() {
          @Override public void send(Row row) throws InterruptedException {
            enumerator.queue.put(row);
          }

          @Override public void end() {
            enumerator.done.set(true);
          }

          @SuppressWarnings("deprecation")
          @Override public void setSourceEnumerable(Enumerable<Row> enumerable)
              throws InterruptedException {
            for (Row row : enumerable) {
              send(row);
            }
            end();
          }

          @Override public void run() {
            try {
              final Page page = new Page();
              final List<ColumnMetaData.Rep> fieldTypes =
                  Collections.nCopies(fieldNames.size(), null);
              request(queryType, request, this, fieldNames, fieldTypes, page);
              enumerator.done.set(true);
            } catch (Throwable e) {
              enumerator.throwableHolder.set(e);
              enumerator.done.set(true);
            }
          }
        };
        service.execute(sink);
        return enumerator;
      }
    };
  }

  /** Reads segment metadata, and populates a list of columns and metrics. */
  void metadata(String dataSourceName, String timestampColumnName,
      List<Interval> intervals,
      Map<String, SqlTypeName> fieldBuilder, Set<String> metricNameBuilder,
      Map<String, List<ComplexMetric>> complexMetrics) {
    final String url = this.url + "/druid/v2/?pretty";
    final Map<String, String> requestHeaders =
        ImmutableMap.of("Content-Type", "application/json");
    final String data = DruidQuery.metadataQuery(dataSourceName, intervals);
    if (CalciteSystemProperty.DEBUG.value()) {
      System.out.println("Druid: " + data);
    }
    try (InputStream in0 = post(url, data, requestHeaders, 10000, 1800000);
         InputStream in = traceResponse(in0)) {
      final ObjectMapper mapper = new ObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      final CollectionType listType =
          mapper.getTypeFactory().constructCollectionType(List.class,
              JsonSegmentMetadata.class);
      final List<JsonSegmentMetadata> list = mapper.readValue(in, listType);
      in.close();
      fieldBuilder.put(timestampColumnName, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
      for (JsonSegmentMetadata o : list) {
        for (Map.Entry<String, JsonColumn> entry : o.columns.entrySet()) {
          if (entry.getKey().equals(DruidTable.DEFAULT_TIMESTAMP_COLUMN)) {
            // timestamp column
            continue;
          }
          final DruidType druidType;
          try {
            druidType = DruidType.getTypeFromMetaData(entry.getValue().type);
          } catch (AssertionError e) {
            // ignore exception; not a supported type
            continue;
          }
          fieldBuilder.put(entry.getKey(), druidType.sqlType);
        }
        if (o.aggregators != null) {
          for (Map.Entry<String, JsonAggregator> entry
              : o.aggregators.entrySet()) {
            if (!fieldBuilder.containsKey(entry.getKey())) {
              continue;
            }
            DruidType type = DruidType.getTypeFromMetaData(entry.getValue().type);
            if (type.isComplex()) {
              // Each complex type will get their own alias, equal to their actual name.
              // Maybe we should have some smart string replacement strategies to make the column
              // names more natural.
              List<ComplexMetric> metricList = new ArrayList<>();
              metricList.add(new ComplexMetric(entry.getKey(), type));
              complexMetrics.put(entry.getKey(), metricList);
            } else {
              metricNameBuilder.add(entry.getKey());
            }
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Reads data source names from Druid. */
  Set<String> tableNames() {
    final Map<String, String> requestHeaders =
        ImmutableMap.of("Content-Type", "application/json");
    final String data = null;
    final String url = coordinatorUrl + "/druid/coordinator/v1/metadata/datasources";
    if (CalciteSystemProperty.DEBUG.value()) {
      System.out.println("Druid: table names" + data + "; " + url);
    }
    try (InputStream in0 = post(url, data, requestHeaders, 10000, 1800000);
         InputStream in = traceResponse(in0)) {
      final ObjectMapper mapper = new ObjectMapper();
      final CollectionType listType =
          mapper.getTypeFactory().constructCollectionType(List.class,
              String.class);
      final List<String> list = mapper.readValue(in, listType);
      return ImmutableSet.copyOf(list);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static InputStream traceResponse(InputStream in) {
    if (CalciteSystemProperty.DEBUG.value()) {
      try {
        final byte[] bytes = AvaticaUtils.readFullyToBytes(in);
        in.close();
        System.out.println("Response: "
            + new String(bytes, StandardCharsets.UTF_8)); // CHECKSTYLE: IGNORE 0
        in = new ByteArrayInputStream(bytes);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return in;
  }

  /** A {@link Sink} that is also {@link Runnable}. */
  private interface RunnableQueueSink extends Sink, Runnable {
  }

  /** An {@link Enumerator} that gets its rows from a {@link BlockingQueue}.
   * There are other fields to signal errors and end-of-data.
   *
   * @param <E> element type */
  private static class BlockingQueueEnumerator<E> implements Enumerator<E> {
    final BlockingQueue<E> queue = new ArrayBlockingQueue<>(1000);
    final AtomicBoolean done = new AtomicBoolean(false);
    final Holder<Throwable> throwableHolder = Holder.empty();

    E next;

    @Override public E current() {
      if (next == null) {
        throw new NoSuchElementException();
      }
      return next;
    }

    @Override public boolean moveNext() {
      for (;;) {
        next = queue.poll();
        if (next != null) {
          return true;
        }
        if (done.get()) {
          close();
          return false;
        }
      }
    }

    @Override public void reset() {}

    @Override public void close() {
      final Throwable e = throwableHolder.get();
      if (e != null) {
        throwableHolder.set(null);
        throw Util.throwAsRuntime(e);
      }
    }
  }

  /** Progress through a large fetch. */
  static class Page {
    @Nullable String pagingIdentifier;
    int offset = -1;
    int totalRowCount = 0;

    @Override public String toString() {
      return "{" + pagingIdentifier + ": " + offset + "}";
    }
  }

  /** Result of a "segmentMetadata" call, populated by Jackson. */
  @SuppressWarnings({ "WeakerAccess", "unused" })
  private static class JsonSegmentMetadata {
    public String id;
    public List<String> intervals;
    public Map<String, JsonColumn> columns;
    public long size;
    public long numRows;
    public Map<String, JsonAggregator> aggregators;
  }

  /** Element of the "columns" collection in the result of a
   * "segmentMetadata" call, populated by Jackson. */
  @SuppressWarnings({ "WeakerAccess", "unused" })
  private static class JsonColumn {
    public String type;
    public boolean hasMultipleValues;
    public int size;
    public Integer cardinality;
    public String errorMessage;
  }

  /** Element of the "aggregators" collection in the result of a
   * "segmentMetadata" call, populated by Jackson. */
  @SuppressWarnings({ "WeakerAccess", "unused" })
  private static class JsonAggregator {
    public String type;
    public String name;
    public String fieldName;

    DruidType druidType() {
      return DruidType.getTypeFromMetric(type);
    }
  }
}
