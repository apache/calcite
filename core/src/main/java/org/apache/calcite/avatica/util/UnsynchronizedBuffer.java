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
package org.apache.calcite.avatica.util;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A utility class for reading and writing bytes to byte buffers without synchronization. A
 * reduced variant taken from Apache Accumulo. This class is <b>not</b> thread-safe by design.
 * It is up to the caller to guarantee mutual exclusion as necessary.
 */
public class UnsynchronizedBuffer extends OutputStream {
  // Anything larger than 64K, reap the backing buffer
  private static final int LARGE_BUFFER_SIZE = 1024 * 64;

  final int initialCapacity;
  int offset = 0;
  byte[] data;

  /**
   * Creates a new writer.
   */
  public UnsynchronizedBuffer() {
    this(4096);
  }

  /**
   * Creates a new writer.
   *
   * @param initialCapacity initial byte capacity
   */
  public UnsynchronizedBuffer(int initialCapacity) {
    this.initialCapacity = initialCapacity;
    data = new byte[initialCapacity];
  }

  private void reserve(int l) {
    if (offset + l > data.length) {
      int newSize = UnsynchronizedBuffer.nextArraySize(offset + l);

      byte[] newData = new byte[newSize];
      System.arraycopy(data, 0, newData, 0, offset);
      data = newData;
    }

  }

  /**
   * Adds bytes to this writer's buffer.
   *
   * @param bytes byte array
   * @param off offset into array to start copying bytes
   * @param length number of bytes to add
   * @throws IndexOutOfBoundsException if off or length are invalid
   */
  public void write(byte[] bytes, int off, int length) {
    reserve(length);
    System.arraycopy(bytes, off, data, offset, length);
    offset += length;
  }

  @Override public void write(int b) throws IOException {
    reserve(1);
    data[offset] = (byte) b;
    offset++;
  }

  /**
   * Gets (a copy of) the contents of this writer's buffer.
   *
   * @return byte buffer contents
   */
  public byte[] toArray() {
    byte[] ret = new byte[offset];
    System.arraycopy(data, 0, ret, 0, offset);
    return ret;
  }

  /**
   * Resets the internal pointer into the buffer.
   */
  public void reset() {
    offset = 0;
    if (data.length >= LARGE_BUFFER_SIZE) {
      data = new byte[this.initialCapacity];
    }
  }

  /**
   * @return The current offset into the backing array.
   */
  public int getOffset() {
    return offset;
  }

  /**
   * @return The current length of the backing array.
   */
  public long getSize() {
    return data.length;
  }

  /**
   * Determines what next array size should be by rounding up to next power of two.
   *
   * @param i current array size
   * @return next array size
   * @throws IllegalArgumentException if i is negative
   */
  public static int nextArraySize(int i) {
    if (i < 0) {
      throw new IllegalArgumentException();
    }

    if (i > (1 << 30)) {
      return Integer.MAX_VALUE; // this is the next power of 2 minus one... a special case
    }

    if (i == 0) {
      return 1;
    }

    // round up to next power of two
    int ret = i;
    ret--;
    ret |= ret >> 1;
    ret |= ret >> 2;
    ret |= ret >> 4;
    ret |= ret >> 8;
    ret |= ret >> 16;
    ret++;

    return ret;
  }
}

// End UnsynchronizedBuffer.java
