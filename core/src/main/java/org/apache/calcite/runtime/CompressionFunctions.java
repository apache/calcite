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

import org.apache.calcite.avatica.util.ByteString;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * A collection of functions used in compression and decompression.
 */
public class CompressionFunctions {

  private CompressionFunctions() {
  }

  /**
   * MySql Compression is based on zlib.
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/zip/Deflater.html">Deflater</a>
   * is used to implement compression.
   */
  public static ByteString compress(String data) {
    try {
      if (data == null) {
        return null;
      }
      if (StringUtils.isEmpty(data)) {
        return new ByteString(new byte[0]);
      }
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      ByteBuffer dataLength = ByteBuffer.allocate(4);
      dataLength.order(ByteOrder.LITTLE_ENDIAN);
      dataLength.putInt(data.length());
      outputStream.write(dataLength.array());
      DeflaterOutputStream inflaterStream = new DeflaterOutputStream(outputStream);
      inflaterStream.write(data.getBytes(Charset.defaultCharset()));
      inflaterStream.close();
      return new ByteString(outputStream.toByteArray());
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * MySql Decompression is based on zlib.
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/zip/Inflater.html">Inflater</a>
   * is used to implement decompression.
   */
  public static String uncompress(ByteString data) {
    try {
      if (data == null) {
        return null;
      } else if (data.length() == 0) {
        return "";
      }
      InflaterInputStream inflaterStream = new InflaterInputStream(
          new ByteArrayInputStream(data.getBytes(), 4, data.length()));
      return IOUtils.toString(inflaterStream);
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * MySQl Supports uncompress function for string which returns NULL value.
   */
  public static String uncompress(String data) {
    return null;
  }
}
