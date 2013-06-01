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
package net.hydromatic.optiq.runtime;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Collection of bytes.
 *
 * <p>ByteString is to bytes what {@link String} is to chars: It is immutable,
 * implements equality (hashCode and equals), comparison (compareTo) and
 * serialization correctly.</p>
 */
public class ByteString implements Comparable<ByteString>, Serializable {
  private final byte[] bytes;

  private static final char[] digits = {
      '0', '1', '2', '3', '4', '5', '6', '7',
      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };

  /**
   * Creates a ByteString.
   *
   * @param bytes Bytes
   */
  public ByteString(byte[] bytes) {
    this.bytes = bytes.clone();
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof ByteString
        && Arrays.equals(bytes, ((ByteString) obj).bytes);
  }

  public int compareTo(ByteString that) {
    final byte[] v1 = bytes;
    final byte[] v2 = that.bytes;
    final int n = Math.min(v1.length, v2.length);
    for (int i = 0; i < n; i++) {
      byte c1 = v1[i];
      byte c2 = v2[i];
      if (c1 != c2) {
        return c1 - c2;
      }
    }
    return v1.length - v2.length;
  }

  /**
   * Returns this byte string in hexadecimal format.
   *
   * @return Hexadecimal string
   */
  @Override
  public String toString() {
    return toString(bytes);
  }

  /**
   * Returns the given byte array in hexadecimal format.
   *
   * <p>For example, <tt>toString(new byte[] {0xDE, 0xAD})</tt>
   * returns {@code "DEAD"}.</p>
   *
   * @param bytes Array of bytes
   * @return String
   */
  public static String toString(byte[] bytes) {
    char[] chars = new char[bytes.length * 2];
    for (int i = 0, j = 0; i < bytes.length; i++) {
      byte b = bytes[i];
      chars[j++] = digits[(b & 0xF0) >> 4];
      chars[j++] = digits[b & 0x0F];
    }
    return new String(chars);
  }

  @SuppressWarnings({
      "CloneDoesntCallSuperClone",
      "CloneDoesntDeclareCloneNotSupportedException"
  })
  @Override
  public Object clone() {
    return this;
  }

  /**
   * Returns the number of bytes in this byte string.
   *
   * @return Length of this byte string
   */
  public int length() {
    return bytes.length;
  }

  /**
   * Returns the byte at a given position in the byte string.
   *
   * @param i Index
   * @throws  IndexOutOfBoundsException
   *          if the <tt>index</tt> argument is negative or not less than
   *          <tt>length()</tt>
   * @return Byte at given position
   */
  public byte byteAt(int i) {
    return bytes[i];
  }
}

// End ByteString.java
