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
package org.apache.calcite.adapter.csv;

import org.apache.calcite.util.Source;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Extension to {@link CSVReader} that can read newly appended file content.
 */
class CsvStreamReader extends CSVReader implements Closeable {
  protected CSVParser parser;
  protected int skipLines;
  protected Tailer tailer;
  protected Queue<String> contentQueue;

  /**
   * The default line to start reading.
   */
  public static final int DEFAULT_SKIP_LINES = 0;

  /**
   * The default file monitor delay.
   */
  public static final long DEFAULT_MONITOR_DELAY = 2000;

  CsvStreamReader(Source source) {
    this(source,
      CSVParser.DEFAULT_SEPARATOR,
      CSVParser.DEFAULT_QUOTE_CHARACTER,
      CSVParser.DEFAULT_ESCAPE_CHARACTER,
      DEFAULT_SKIP_LINES,
      CSVParser.DEFAULT_STRICT_QUOTES,
      CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE);
  }

  /**
   * Creates a CsvStreamReader with supplied separator and quote char.
   *
   * @param source The file to an underlying CSV source
   * @param separator The delimiter to use for separating entries
   * @param quoteChar The character to use for quoted elements
   * @param escape The character to use for escaping a separator or quote
   * @param line The line number to skip for start reading
   * @param strictQuotes Sets if characters outside the quotes are ignored
   * @param ignoreLeadingWhiteSpace If true, parser should ignore
   *  white space before a quote in a field
   */
  private CsvStreamReader(Source source, char separator, char quoteChar,
      char escape, int line, boolean strictQuotes,
      boolean ignoreLeadingWhiteSpace) {
    super(new StringReader("")); // dummy call to base constructor
    contentQueue = new ArrayDeque<>();
    TailerListener listener = new CsvContentListener(contentQueue);
    tailer = Tailer.create(source.file(), listener, DEFAULT_MONITOR_DELAY,
        false, true, 4096);
    this.parser = new CSVParser(separator, quoteChar, escape, strictQuotes,
        ignoreLeadingWhiteSpace);
    this.skipLines = line;
    try {
      // wait for tailer to capture data
      Thread.sleep(DEFAULT_MONITOR_DELAY);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Reads the next line from the buffer and converts to a string array.
   *
   * @return a string array with each comma-separated element as a separate entry.
   *
   * @throws IOException if bad things happen during the read
   */
  public String[] readNext() throws IOException {
    String[] result = null;
    do {
      String nextLine = getNextLine();
      if (nextLine == null) {
        return null;
      }
      String[] r = parser.parseLineMulti(nextLine);
      if (r.length > 0) {
        if (result == null) {
          result = r;
        } else {
          String[] t = new String[result.length + r.length];
          System.arraycopy(result, 0, t, 0, result.length);
          System.arraycopy(r, 0, t, result.length, r.length);
          result = t;
        }
      }
    } while (parser.isPending());
    return result;
  }

  /**
   * Reads the next line from the file.
   *
   * @return the next line from the file without trailing newline
   *
   * @throws IOException if bad things happen during the read
   */
  private String getNextLine() throws IOException {
    return contentQueue.poll();
  }

  /**
   * Closes the underlying reader.
   *
   * @throws IOException if the close fails
   */
  public void close() throws IOException {
  }

  /** Watches for content being appended to a CSV file. */
  private static class CsvContentListener extends TailerListenerAdapter {
    final Queue<String> contentQueue;

    CsvContentListener(Queue<String> contentQueue) {
      this.contentQueue = contentQueue;
    }

    @Override public void handle(String line) {
      this.contentQueue.add(line);
    }
  }
}

// End CsvStreamReader.java
