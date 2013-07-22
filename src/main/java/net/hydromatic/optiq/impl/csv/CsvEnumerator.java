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
package net.hydromatic.optiq.impl.csv;

import net.hydromatic.linq4j.Enumerator;

import au.com.bytecode.opencsv.CSVReader;

import java.io.*;

/** Enumerator that reads from a CSV file. */
class CsvEnumerator implements Enumerator<Object[]> {
  private final CSVReader reader;
  private final CsvFieldType[] fieldTypes;
  private final int[] fields;
  private Object[] current;

  public CsvEnumerator(File file, CsvFieldType[] fieldTypes) {
    this(file, fieldTypes, identityList(fieldTypes.length));
  }

  public CsvEnumerator(File file, CsvFieldType[] fieldTypes, int[] fields) {
    this.fieldTypes = fieldTypes;
    this.fields = fields;
    try {
      this.reader = new CSVReader(new FileReader(file));
      this.reader.readNext(); // skip header row
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Object[] current() {
    return current;
  }

  public boolean moveNext() {
    try {
      final String[] strings = reader.readNext();
      if (strings == null) {
        current = null;
        reader.close();
        return false;
      }
      current = convertRow(strings);
      return true;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Object[] convertRow(String[] strings) {
    final Object[] objects = new Object[fields.length];
    for (int i = 0; i < fields.length; i++) {
      int field = fields[i];
      objects[i] = convert(fieldTypes[field], strings[field]);
    }
    return objects;
  }

  private Object convert(CsvFieldType fieldType, String string) {
    if (fieldType == null) {
      return string;
    }
    switch (fieldType) {
    default:
    case STRING:
      return string;
    case BOOLEAN:
      if (string.length() == 0) {
        return null;
      }
      return Boolean.parseBoolean(string);
    case BYTE:
      if (string.length() == 0) {
        return null;
      }
      return Byte.parseByte(string);
    case SHORT:
      if (string.length() == 0) {
        return null;
      }
      return Short.parseShort(string);
    case INT:
      if (string.length() == 0) {
        return null;
      }
      return Integer.parseInt(string);
    case LONG:
      if (string.length() == 0) {
        return null;
      }
      return Long.parseLong(string);
    case FLOAT:
      if (string.length() == 0) {
        return null;
      }
      return Float.parseFloat(string);
    case DOUBLE:
      if (string.length() == 0) {
        return null;
      }
      return Double.parseDouble(string);
    }
  }

  public void reset() {
    throw new UnsupportedOperationException();
  }

  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException("Error closing CSV reader", e);
    }
  }

  /** Returns an array of integers {0, ..., n - 1}. */
  static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }
}

// End CsvEnumerator.java
