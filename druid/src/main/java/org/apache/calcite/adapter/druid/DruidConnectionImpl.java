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
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.util.Holder;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.calcite.runtime.HttpUtils.post;

/**
 * Implementation of {@link DruidConnection}.
 */
class DruidConnectionImpl implements DruidConnection {
  final String url;

  public DruidConnectionImpl(String url) {
    this.url = url;
  }

  public void request(QueryType queryType, String data, Sink sink,
      List<String> fieldNames, Page page) throws IOException {
    if (CalcitePrepareImpl.DEBUG) {
      System.out.println(data);
    }
    final Map<String, String> requestHeaders =
        ImmutableMap.of("Content-Type", "application/json");
    final InputStream in = post(url, data, requestHeaders, 10000, 1800000);
    parse(queryType, in, sink, fieldNames, page);
  }

  /** Parses the output of a {@code topN} query, sending the results to a
   * {@link Sink}. */
  private void parse(QueryType queryType, InputStream in, Sink sink,
      List<String> fieldNames, Page page) {
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
              parseFields(fieldNames, rowBuilder, parser);
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
                  parseFields(fieldNames, rowBuilder, parser);
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
              parseFields(fieldNames, rowBuilder, parser);
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

  private void parseFields(List<String> fieldNames, Row.RowBuilder rowBuilder,
      JsonParser parser) throws IOException {
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      parseField(fieldNames, rowBuilder, parser);
    }
  }

  private void parseField(List<String> fieldNames, Row.RowBuilder rowBuilder,
      JsonParser parser) throws IOException {
    final String fieldName = parser.getCurrentName();

    // Move to next token, which is name's value
    JsonToken token = parser.nextToken();
    int i = fieldNames.indexOf(fieldName);
    if (i < 0) {
      return;
    }
    switch (token) {
    case VALUE_NUMBER_INT:
      rowBuilder.set(i, parser.getIntValue());
      break;
    case VALUE_NUMBER_FLOAT:
      rowBuilder.set(i, parser.getDoubleValue());
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
              request(queryType, request, this, fieldNames, page);
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
}

// End DruidConnectionImpl.java
