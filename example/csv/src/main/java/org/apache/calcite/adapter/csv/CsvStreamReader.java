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

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;

import au.com.bytecode.opencsv.CSVParser;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * CSVSreamReader that can read newly appended file content
 */
public class CsvStreamReader implements Closeable {
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

  public CsvStreamReader(File csvFile) {
    this(
      csvFile,
      CSVParser.DEFAULT_SEPARATOR,
      CSVParser.DEFAULT_QUOTE_CHARACTER,
      CSVParser.DEFAULT_ESCAPE_CHARACTER,
      DEFAULT_SKIP_LINES,
      CSVParser.DEFAULT_STRICT_QUOTES,
      CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE
    );
  }

  /**
   * Constructs CSVReader with supplied separator and quote char.
   *
   * @param csvFile the file to an underlying CSV source.
   * @param separator the delimiter to use for separating entries
   * @param quotechar the character to use for quoted elements
   * @param escape the character to use for escaping a separator or quote
   * @param line the line number to skip for start reading
   * @param strictQuotes sets if characters outside the quotes are ignored
   * @param ignoreLeadingWhiteSpace it true, parser should ignore
   *  white space before a quote in a field
   */
  public CsvStreamReader(File csvFile, char separator, char quotechar, char escape, int line,
                         boolean strictQuotes, boolean ignoreLeadingWhiteSpace) {
    contentQueue = new ArrayDeque<String>();
    TailerListener listener = new CSVContentListener(contentQueue);
    tailer = Tailer.create(csvFile, listener, DEFAULT_MONITOR_DELAY, false, true, 4096);
    this.parser = new CSVParser(
      separator,
      quotechar,
      escape,
      strictQuotes,
      ignoreLeadingWhiteSpace
    );
    this.skipLines = line;
    try {
      //wait for tailer to capture data
      Thread.sleep(DEFAULT_MONITOR_DELAY);
    } catch (InterruptedException e) {
      //ignore the interruption
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
      while (nextLine == null) {
        try {
          Thread.sleep(DEFAULT_MONITOR_DELAY);
          nextLine = getNextLine();
        } catch (InterruptedException e) {
          return null; // should throw if still pending?
        }
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
   * @throws IOException
   *             if bad things happen during the read
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

  /** csv file content watcher*/
  class CSVContentListener extends TailerListenerAdapter {
    Queue<String> contentQueue;

    CSVContentListener(Queue<String> contentQueue) {
      this.contentQueue = contentQueue;
    }

    @Override public void handle(String line) {
      this.contentQueue.add(line);
    }
  }
}

// End CsvStreamReader.java
