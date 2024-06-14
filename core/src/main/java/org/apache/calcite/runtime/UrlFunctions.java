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
package org.apache.calcite.runtime;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A collection of functions used in Url processing.
 */
public class UrlFunctions {

  private UrlFunctions() {
  }

  private static final Charset UTF_8 = StandardCharsets.UTF_8;

  /** The "URL_DECODE(string)" function for Hive and Spark,
   * which returns original value when decoded error. */
  public static String urlDecode(String value) {
    try {
      return URLDecoder.decode(value, UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw RESOURCE.charsetEncoding(value, UTF_8.name()).ex();
    } catch (RuntimeException e) {
      return value;
    }
  }

  /** The "URL_ENCODE(string)" function for Hive and Spark. */
  public static String urlEncode(String url) {
    String value;
    try {
      value = URLEncoder.encode(url, UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw RESOURCE.charsetEncoding(url, UTF_8.name()).ex();
    }
    return value;
  }
}
