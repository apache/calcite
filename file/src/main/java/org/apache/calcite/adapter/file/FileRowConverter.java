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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * FileRowConverter.
 */
class FileRowConverter {

  // cache for lazy initialization
  private final FileReader fileReader;
  private final List<Map<String, Object>> fieldConfigs;
  private boolean initialized = false;

  // row parser configuration
  private final List<FieldDef> fields = new ArrayList<>();

  /** Format for parsing numbers. Not thread-safe, but we assume that only
   * one thread uses this converter at a time. */
  private final NumberFormat numberFormat =
      NumberFormat.getInstance(Locale.ROOT);

  /** Format for parsing integers. Not thread-safe, but we assume that only
   * one thread uses this converter at a time. */
  private final NumberFormat integerFormat =
      NumberFormat.getIntegerInstance(Locale.ROOT);

  /** Creates a FileRowConverter. */
  FileRowConverter(FileReader fileReader,
      List<Map<String, Object>> fieldConfigs) {
    this.fileReader = fileReader;
    this.fieldConfigs = fieldConfigs;
  }

  // initialize() - combine HTML table header information with field definitions
  //      to initialize the table reader
  // NB:  object initialization is deferred to avoid unnecessary URL reads
  private void initialize() {
    if (this.initialized) {
      return;
    }
    try {
      final Elements headerElements = this.fileReader.getHeadings();

      // create a name to index map for HTML table elements
      final Map<String, Integer> headerMap = new LinkedHashMap<>();
      int i = 0;
      for (Element th : headerElements) {
        String heading = th.text();
        if (headerMap.containsKey(heading)) {
          throw new Exception("duplicate heading: '" + heading + "'");
        }
        headerMap.put(heading, i++);
      }

      // instantiate the field definitions
      final Set<String> colNames = new HashSet<>();
      final Set<String> sources = new HashSet<>();
      if (this.fieldConfigs != null) {
        try {
          for (Map<String, Object> fieldConfig : this.fieldConfigs) {

            String thName = (String) fieldConfig.get("th");
            String name = thName;
            String newName;
            FileFieldType type = null;
            boolean skip = false;

            if (!headerMap.containsKey(thName)) {
              throw new Exception("bad source column name: '" + thName + "'");
            }
            if ((newName = (String) fieldConfig.get("name")) != null) {
              name = newName;
            }
            if (colNames.contains(name)) {
              throw new Exception("duplicate column name: '" + name + "'");
            }

            String typeString = (String) fieldConfig.get("type");
            if (typeString != null) {
              type = FileFieldType.of(typeString);
            }

            String sSkip = (String) fieldConfig.get("skip");
            if (sSkip != null) {
              skip = Boolean.parseBoolean(sSkip);
            }

            Integer sourceIx = headerMap.get(thName);
            colNames.add(name);
            sources.add(thName);
            if (!skip) {
              addFieldDef(name, type, fieldConfig, sourceIx);
            }
          }
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      // pick up any data elements not explicitly defined
      for (Map.Entry<String, Integer> e : headerMap.entrySet()) {
        final String name = e.getKey();
        if (!sources.contains(name) && !colNames.contains(name)) {
          addFieldDef(name, null, null, e.getValue());
        }
      }

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    this.initialized = true;
  }

  // add another field definition to the FileRowConverter during initialization
  private void addFieldDef(String name, FileFieldType type,
      Map<String, Object> config, int sourceCol) {
    this.fields.add(new FieldDef(name, type, config, sourceCol));
  }

  /** Converts a row of JSoup Elements to an array of java objects. */
  Object toRow(Elements rowElements, int[] projection) {
    initialize();
    final Object[] objects = new Object[projection.length];

    for (int i = 0; i < projection.length; i++) {
      int field = projection[i];
      objects[i] = this.fields.get(field).convert(rowElements);
    }
    return objects;
  }

  int width() {
    initialize();
    return this.fields.size();
  }

  RelDataType getRowType(JavaTypeFactory typeFactory) {
    initialize();
    List<String> names = new ArrayList<>();
    List<RelDataType> types = new ArrayList<>();

    // iterate through FieldDefs, populating names and types
    for (FieldDef f : this.fields) {
      names.add(f.getName());

      FileFieldType fieldType = f.getType();
      RelDataType type;

      if (fieldType == null) {
        type = typeFactory.createJavaType(String.class);
      } else {
        type = fieldType.toType(typeFactory);
      }

      types.add(type);
    }

    if (names.isEmpty()) {
      names.add("line");
      types.add(typeFactory.createJavaType(String.class));
    }

    return typeFactory.createStructType(Pair.zip(names, types));
  }

  /** Parses an an HTML table cell. */
  private class CellReader {
    private String type;
    private String selector;
    private Integer selectedElement;
    private String replaceText;
    private Pattern replacePattern;
    private String replaceWith;
    private String matchText;
    private Pattern matchPattern;
    private Integer matchSeq;

    CellReader(Map<String, Object> config) {
      if (config != null) {
        this.type = (String) config.get("type");
        this.selector = (String) config.get("selector");
        this.selectedElement = (Integer) config.get("selectedElement");
        this.replaceText = (String) config.get("replace");
        this.replaceWith = (String) config.get("replaceWith");
        this.matchText = (String) config.get("match");
        this.matchSeq = (Integer) config.get("matchSeq");
      }

      if (this.selector == null) {
        this.selector = "*";
      }

      if (this.replaceText != null) {
        this.replacePattern = Pattern.compile(this.replaceText);
      }

      if (this.replaceWith == null) {
        this.replaceWith = "";
      }

      if (this.matchText != null) {
        this.matchPattern = Pattern.compile(this.matchText);
      }

      if (this.matchSeq == null) {
        this.matchSeq = 0;
      }

    }

    String read(Element cell) {
      ArrayList<String> cellText = new ArrayList<>();

      if (this.selectedElement != null) {
        cellText.add(cell.select(this.selector)
            .get(this.selectedElement).ownText());
      } else {
        for (Element child : cell.select(this.selector)) {
          //String tagName = child.tag().getName();
          cellText.add(child.ownText());
        }
      }

      String cellString = String.join(" ", cellText).trim();

      // replace
      if (this.replacePattern != null) {
        Matcher m = this.replacePattern.matcher(cellString);
        cellString = m.replaceAll(this.replaceWith);
      }

      // match
      if (this.matchPattern == null) {
        return cellString;
      } else {
        List<String> allMatches = new ArrayList<>();
        Matcher m = this.matchPattern.matcher(cellString);
        while (m.find()) {
          allMatches.add(m.group());
        }
        if (allMatches.size() != 0) {
          return allMatches.get(this.matchSeq);
        } else {
          return null;
        }
      }
    }
  }

  /** Responsible for managing field (column) definition,
   * and for converting an Element to a java data type. */
  private class FieldDef {
    String name;
    FileFieldType type;
    Map<String, Object> config;
    CellReader cellReader;
    int cellSeq;

    FieldDef(String name, FileFieldType type, Map<String, Object> config,
        int cellSeq) {
      this.name = name;
      this.type = type;
      this.config = config;
      this.cellReader = new CellReader(config);
      this.cellSeq = cellSeq;
    }

    Object convert(Elements row) {
      return toObject(this.type, this.cellReader.read(row.get(this.cellSeq)));
    }

    public String getName() {
      return this.name;
    }

    FileFieldType getType() {
      return this.type;
    }

    private java.util.Date parseDate(String string) {
      Parser parser = new Parser(DateTimeUtils.UTC_ZONE);
      List<DateGroup> groups = parser.parse(string);
      DateGroup group = groups.get(0);
      return group.getDates().get(0);
    }

    private Object toObject(FileFieldType fieldType, String string) {
      if ((string == null) || (string.length() == 0)) {
        return null;
      }

      if (fieldType == null) {
        return string;
      }

      switch (fieldType) {
      default:
      case STRING:
        return string;

      case BOOLEAN:
        return Boolean.parseBoolean(string);

      case BYTE:
        return Byte.parseByte(string);

      case SHORT:
        try {
          return integerFormat.parse(string).shortValue();
        } catch (ParseException e) {
          return null;
        }

      case INT:
        try {
          return integerFormat.parse(string).intValue();
        } catch (ParseException e) {
          return null;
        }

      case LONG:
        try {
          return numberFormat.parse(string).longValue();
        } catch (ParseException e) {
          return null;
        }

      case FLOAT:
        try {
          return numberFormat.parse(string).floatValue();
        } catch (ParseException e) {
          return null;
        }

      case DOUBLE:
        try {
          return numberFormat.parse(string).doubleValue();
        } catch (ParseException e) {
          return null;
        }

      case DATE:
        return new java.sql.Date(parseDate(string).getTime());

      case TIME:
        return new java.sql.Time(parseDate(string).getTime());

      case TIMESTAMP:
        return new java.sql.Timestamp(parseDate(string).getTime());
      }
    }
  }
}

// End FileRowConverter.java
