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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerator;

import java.io.File;
import java.io.IOException;

/**
 * Csv Streaming enumerator
 * @param <E> Row type
 */
public class CsvStreamEnumerator<E> implements Enumerator<E> {
  protected CsvStreamReader streamReader;
  protected String[] filterValues;
  protected CsvEnumerator.RowConverter<E> rowConverter;
  protected E current;

  public CsvStreamEnumerator(File file, String[] filterValues,
    CsvEnumerator.RowConverter<E> rowConverter) {
    this.rowConverter = rowConverter;
    this.filterValues = filterValues;
    try {
      this.streamReader = new CsvStreamReader(file);
      this.streamReader.readNext(); // skip header row
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void setDataContext(DataContext root) {
    this.streamReader.setDataContext(root);
  }

  public boolean moveNext() {
    return true;
  }

  public E readNext() {
    try {
    outer:
      for (;;) {
        final String[] strings = streamReader.readNext();
        if (strings == null) {
          streamReader.close();
          return current;
        } else {
          if (filterValues != null) {
            for (int i = 0; i < strings.length; i++) {
              String filterValue = filterValues[i];
              if (filterValue != null) {
                if (!filterValue.equals(strings[i])) {
                  continue outer;
                }
              }
            }
          }
          current = rowConverter.convertRow(strings);
          return current;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public E current() {
    return readNext();
  }

  @Override public void close() {
    try {
      streamReader.close();
    } catch (IOException e) {
      throw new RuntimeException("Error closing Csv Stream reader", e);
    }
  }

  @Override public void reset() {
    throw new UnsupportedOperationException();
  }
}

// End CsvStreamEnumerator.java
