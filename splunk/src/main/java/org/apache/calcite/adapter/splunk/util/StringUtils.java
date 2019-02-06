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
package org.apache.calcite.adapter.splunk.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Utility methods for encoding and decoding strings for Splunk REST calls.
 */
public class StringUtils {
  private StringUtils() {}

  public static StringBuilder encodeList(
      List<? extends CharSequence> list, char delim) {
    StringBuilder result = new StringBuilder();
    boolean first = true;
    for (CharSequence cs : list) {
      if (!first) {
        result.append(delim);
      }
      int len = cs.length();
      for (int i = 0; i < len; ++i) {
        char c = cs.charAt(i);
        if (c == delim) {
          result.append('\\');
        }
        result.append(c);
      }
      first = false;
    }
    return result;
  }

  public static List<String> decodeList(CharSequence encoded, char delim) {
    List<String> list = new LinkedList<>();
    int len = encoded.length();
    int start = 0;
    int end = 0;
    boolean hasEscapedDelim = false;
    char p = '\0';
    char c = '\0';
    for (int i = 0; i < len; i++, ++end) {
      p = c;
      c = encoded.charAt(i);
      if (c == delim) {
        if (p == '\\') {
          hasEscapedDelim = true;
        } else {
          if (!hasEscapedDelim) {
            list.add(encoded.subSequence(start, end).toString());
          } else {
            StringBuilder sb = new StringBuilder(end - start);
            char a = '\0';
            char b = '\0';
            for (int j = start; j < end; ++j) {
              b = a;
              a = encoded.charAt(j);
              if (b == '\\' && a != delim) {
                sb.append(b);
              }
              if (a != '\\') {
                sb.append(a);
              }
            }
            list.add(sb.toString());
          }
          start = end + 1;
          hasEscapedDelim = false;
        }
      }
    }

    if (!hasEscapedDelim) {
      list.add(encoded.subSequence(start, end).toString());
    } else {
      StringBuilder sb = new StringBuilder(end - start);
      char a = '\0';
      char b = '\0';
      for (int j = start; j < end; ++j) {
        b = a;
        a = encoded.charAt(j);
        if (b == '\\' && a != delim) {
          sb.append(b);
        }
        if (a != '\\') {
          sb.append(a);
        }
      }
      list.add(sb.toString());
    }

    return list;
  }

  public static boolean parseBoolean(
      String str, boolean defaultVal, boolean missingVal) {
    if (str == null || str.isEmpty()) {
      return missingVal;
    }
    if (str.equalsIgnoreCase("t")
        || str.equalsIgnoreCase("true")
        || str.equalsIgnoreCase("yes")
        || str.equals("1")) {
      return true;
    }
    if (str.equalsIgnoreCase("f")
        || str.equalsIgnoreCase("false")
        || str.equalsIgnoreCase("no")
        || str.equals("0")) {
      return false;
    }
    return defaultVal;
  }


  public static void main(String[] args) {
    List<String> list = new LinkedList<>();
    list.add("test");
    list.add("test,with,comma");
    list.add("");
    list.add(",");

    System.out.println("=============");

    StringBuilder sb = encodeList(list, ',');
    System.out.println(sb);

    list.clear();
    list = decodeList(sb, ',');
    for (String s : list) {
      System.out.println(s);
    }
    System.out.println("=============");
  }

  public static Logger getClassTracer(Class clazz) {
    return LoggerFactory.getLogger(clazz);
  }
}

// End StringUtils.java
