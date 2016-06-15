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
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.interpreter.Sink;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Holder;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.calcite.runtime.HttpUtils.post;

/**
 * Implementation of {@link DruidConnection}.
 */
class DruidConnectionImpl implements DruidConnection {
  private final String url;
  private final String coordinatorUrl;

  DruidConnectionImpl(String url, String coordinatorUrl) {
    this.url = Preconditions.checkNotNull(url);
    this.coordinatorUrl = Preconditions.checkNotNull(coordinatorUrl);
  }

  /** Executes a query request.
   *
   * @param queryType Query type
   * @param data Data to post
   * @param sink Sink to which to send the parsed rows
   * @param fieldNames Names of fields
   * @param fieldTypes Types of fields (never null, but elements may be null)
   * @param page Page definition (in/out)
   * @throws IOException on error
   */
  public void request(QueryType queryType, String data, Sink sink,
      List<String> fieldNames, List<Primitive> fieldTypes, Page page)
      throws IOException {
    final String url = this.url + "/druid/v2/?pretty";
    final Map<String, String> requestHeaders =
        ImmutableMap.of("Content-Type", "application/json");
    if (CalcitePrepareImpl.DEBUG) {
      System.out.println(data);
    }
    try (InputStream in0 = post(url, data, requestHeaders, 10000, 1800000);
         InputStream in = traceResponse(in0)) {
      parse(queryType, in, sink, fieldNames, fieldTypes, page);
    }
  }

  /** Parses the output of a {@code topN} query, sending the results to a
   * {@link Sink}. */
  private void parse(QueryType queryType, InputStream in, Sink sink,
      List<String> fieldNames, List<Primitive> fieldTypes, Page page) {
    final JsonFactory factory = new JsonFactory();
    final Row.RowBuilder rowBuilder = Row.newBuilder(fieldNames.size());

    if (CalcitePrepareImpl.DEBUG) {
      try {
        final byte[] bytes = AvaticaUtils.readFullyToBytes(in);
        System.out.println("Response: " + new String(bytes));
        in = new ByteArrayInputStream(bytes);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    try (final JsonParser parser = factory.createParser(in)) {
      switch (queryType) {
      case TOP_N:
        if (parser.nextToken() == JsonToken.START_ARRAY
            && parser.nextToken() == JsonToken.START_OBJECT) {
          expectScalarField(parser, "timestamp");
          if (parser.nextToken() == JsonToken.FIELD_NAME
              && parser.getCurrentName().equals("result")
              && parser.nextToken() == JsonToken.START_ARRAY) {
            while (parser.nextToken() == JsonToken.START_OBJECT) {
              // loop until token equal to "}"
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
          expectScalarField(parser, "timestamp");
          if (parser.nextToken() == JsonToken.FIELD_NAME
              && parser.getCurrentName().equals("result")
              && parser.nextToken() == JsonToken.START_OBJECT) {
            if (parser.nextToken() == JsonToken.FIELD_NAME
                && parser.getCurrentName().equals("pagingIdentifiers")
                && parser.nextToken() == JsonToken.START_OBJECT) {
              switch (parser.nextToken()) {
              case FIELD_NAME:
                page.pagingIdentifier = parser.getCurrentName();
                if (parser.nextToken() == JsonToken.VALUE_NUMBER_INT) {
                  page.offset = parser.getIntValue();
                }
                expect(parser, JsonToken.END_OBJECT);
                break;
              case END_OBJECT:
              }
            }
            if (parser.nextToken() == JsonToken.FIELD_NAME
                && parser.getCurrentName().equals("events")
                && parser.nextToken() == JsonToken.START_ARRAY) {
              while (parser.nextToken() == JsonToken.START_OBJECT) {
                expectScalarField(parser, "segmentId");
                expectScalarField(parser, "offset");
                if (parser.nextToken() == JsonToken.FIELD_NAME
                    && parser.getCurrentName().equals("event")
                    && parser.nextToken() == JsonToken.START_OBJECT) {
                  parseFields(fieldNames, fieldTypes, rowBuilder, parser);
                  sink.send(rowBuilder.build());
                  rowBuilder.reset();
                }
                expect(parser, JsonToken.END_OBJECT);
              }
              parser.nextToken();
            }
          }
        }
        break;

      case GROUP_BY:
        if (parser.nextToken() == JsonToken.START_ARRAY) {
          while (parser.nextToken() == JsonToken.START_OBJECT) {
            expectScalarField(parser, "version");
            expectScalarField(parser, "timestamp");
            if (parser.nextToken() == JsonToken.FIELD_NAME
                && parser.getCurrentName().equals("event")
                && parser.nextToken() == JsonToken.START_OBJECT) {
              parseFields(fieldNames, fieldTypes, rowBuilder, parser);
              sink.send(rowBuilder.build());
              rowBuilder.reset();
            }
            expect(parser, JsonToken.END_OBJECT);
          }
        }
      }
    } catch (IOException | InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  private void parseFields(List<String> fieldNames, List<Primitive> fieldTypes,
      Row.RowBuilder rowBuilder, JsonParser parser) throws IOException {
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      parseField(fieldNames, fieldTypes, rowBuilder, parser);
    }
  }

  private void parseField(List<String> fieldNames, List<Primitive> fieldTypes,
      Row.RowBuilder rowBuilder, JsonParser parser) throws IOException {
    final String fieldName = parser.getCurrentName();

    // Move to next token, which is name's value
    JsonToken token = parser.nextToken();
    int i = fieldNames.indexOf(fieldName);
    if (i < 0) {
      return;
    }
    switch (token) {
    case VALUE_NUMBER_INT:
    case VALUE_NUMBER_FLOAT:
      Primitive type = fieldTypes.get(i);
      if (type == null) {
        if (token == JsonToken.VALUE_NUMBER_INT) {
          type = Primitive.INT;
        } else {
          type = Primitive.FLOAT;
        }
      }
      switch (type) {
      case BYTE:
        rowBuilder.set(i, parser.getIntValue());
        break;
      case SHORT:
        rowBuilder.set(i, parser.getShortValue());
        break;
      case INT:
        rowBuilder.set(i, parser.getIntValue());
        break;
      case LONG:
        rowBuilder.set(i, parser.getLongValue());
        break;
      case FLOAT:
        rowBuilder.set(i, parser.getFloatValue());
        break;
      case DOUBLE:
        rowBuilder.set(i, parser.getDoubleValue());
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
    default:
      rowBuilder.set(i, parser.getText());
    }
  }

  private void expect(JsonParser parser, JsonToken token) throws IOException {
    final JsonToken t = parser.nextToken();
    if (t != token) {
      throw new RuntimeException("expected " + token + ", got " + t);
    }
  }

  private void expectScalarField(JsonParser parser, String name)
      throws IOException {
    expect(parser, JsonToken.FIELD_NAME);
    if (!parser.getCurrentName().equals(name)) {
      throw new RuntimeException("expected field " + name + ", got "
          + parser.getCurrentName());
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

  private void expectObjectField(JsonParser parser, String name)
      throws IOException {
    expect(parser, JsonToken.FIELD_NAME);
    if (!parser.getCurrentName().equals(name)) {
      throw new RuntimeException("expected field " + name + ", got "
          + parser.getCurrentName());
    }
    expect(parser, JsonToken.START_OBJECT);
    while (parser.nextToken() != JsonToken.END_OBJECT) {
        // empty
    }
  }

  /** Executes a request and returns the resulting rows as an
   * {@link Enumerable}, running the parser in a thread provided by
   * {@code service}. */
  public Enumerable<Row> enumerable(final QueryType queryType,
      final String request, final List<String> fieldNames,
      final ExecutorService service)
      throws IOException {
    return new AbstractEnumerable<Row>() {
      public Enumerator<Row> enumerator() {
        final BlockingQueueEnumerator<Row> enumerator =
            new BlockingQueueEnumerator<>();
        final RunnableQueueSink sink = new RunnableQueueSink() {
          public void send(Row row) throws InterruptedException {
            enumerator.queue.put(row);
          }

          public void end() {
            enumerator.done.set(true);
          }

          public void setSourceEnumerable(Enumerable<Row> enumerable)
              throws InterruptedException {
            for (Row row : enumerable) {
              send(row);
            }
            end();
          }

          public void run() {
            try {
              final Page page = new Page();
              final List<Primitive> fieldTypes =
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
  void metadata(String dataSourceName, List<String> intervals,
      Map<String, SqlTypeName> fieldBuilder, Set<String> metricNameBuilder) {
    final String url = this.url + "/druid/v2/?pretty";
    final Map<String, String> requestHeaders =
        ImmutableMap.of("Content-Type", "application/json");
    final String data = DruidQuery.metadataQuery(dataSourceName, intervals);
    if (CalcitePrepareImpl.DEBUG) {
      System.out.println("Druid: " + data);
    }
    try (InputStream in0 = post(url, data, requestHeaders, 10000, 1800000);
         InputStream in = traceResponse(in0)) {
      final ObjectMapper mapper = new ObjectMapper();
      final CollectionType listType =
          mapper.getTypeFactory().constructCollectionType(List.class,
              JsonSegmentMetadata.class);
      final List<JsonSegmentMetadata> list = mapper.readValue(in, listType);
      in.close();
      for (JsonSegmentMetadata o : list) {
        for (Map.Entry<String, JsonColumn> entry : o.columns.entrySet()) {
          fieldBuilder.put(entry.getKey(), entry.getValue().sqlType());
        }
        if (o.aggregators != null) {
          for (Map.Entry<String, JsonAggregator> entry
              : o.aggregators.entrySet()) {
            fieldBuilder.put(entry.getKey(), entry.getValue().sqlType());
            metricNameBuilder.add(entry.getKey());
          }
        }
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /** Reads data source names from Druid. */
  Set<String> tableNames() {
    final Map<String, String> requestHeaders =
        ImmutableMap.of("Content-Type", "application/json");
    final String data = null;
    final String url = coordinatorUrl + "/druid/coordinator/v1/metadata/datasources";
    if (CalcitePrepareImpl.DEBUG) {
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
      throw Throwables.propagate(e);
    }
  }

  private InputStream traceResponse(InputStream in) {
    if (CalcitePrepareImpl.DEBUG) {
      try {
        final byte[] bytes = AvaticaUtils.readFullyToBytes(in);
        in.close();
        System.out.println("Response: " + new String(bytes));
        in = new ByteArrayInputStream(bytes);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    return in;
  }

  /** A {@link Sink} that is also {@link Runnable}. */
  private interface RunnableQueueSink extends Sink, Runnable {
  }

  /** An {@link Enumerator} that gets its rows from a {@link BlockingQueue}.
   * There are other fields to signal errors and end-of-data. */
  private static class BlockingQueueEnumerator<E> implements Enumerator<E> {
    final BlockingQueue<E> queue = new ArrayBlockingQueue<>(1000);
    final AtomicBoolean done = new AtomicBoolean(false);
    final Holder<Throwable> throwableHolder = Holder.of(null);

    E next;

    public E current() {
      if (next == null) {
        throw new NoSuchElementException();
      }
      return next;
    }

    public boolean moveNext() {
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

    public void reset() {}

    public void close() {
      final Throwable throwable = throwableHolder.get();
      if (throwable != null) {
        throwableHolder.set(null);
        throw Throwables.propagate(throwable);
      }
    }
  }

  /** Progress through a large fetch. */
  static class Page {
    String pagingIdentifier = null;
    int offset = -1;

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
    public int size;
    public int numRows;
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

    SqlTypeName sqlType() {
      return sqlType(type);
    }

    static SqlTypeName sqlType(String type) {
      switch (type) {
      case "LONG":
        return SqlTypeName.BIGINT;
      case "DOUBLE":
        return SqlTypeName.DOUBLE;
      case "FLOAT":
        return SqlTypeName.REAL;
      case "STRING":
        return SqlTypeName.VARCHAR;
      case "hyperUnique":
        return SqlTypeName.VARBINARY;
      default:
        throw new AssertionError("unknown type " + type);
      }
    }
  }

  /** Element of the "aggregators" collection in the result of a
   * "segmentMetadata" call, populated by Jackson. */
  @SuppressWarnings({ "WeakerAccess", "unused" })
  private static class JsonAggregator {
    public String type;
    public String name;
    public String fieldName;

    SqlTypeName sqlType() {
      if (type.startsWith("long")) {
        return SqlTypeName.BIGINT;
      }
      if (type.startsWith("double")) {
        return SqlTypeName.DOUBLE;
      }
      if (type.equals("hyperUnique")) {
        return SqlTypeName.VARBINARY;
      }
      throw new AssertionError("unknown type " + type);
    }
  }
}

// End DruidConnectionImpl.java
