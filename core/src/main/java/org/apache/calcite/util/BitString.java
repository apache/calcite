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
package org.apache.calcite.util;

import java.math.BigInteger;
import java.util.List;

/**
 * String of bits.
 *
 * <p>A bit string logically consists of a set of '0' and '1' values, of a
 * specified length. The length is preserved even if this means that the bit
 * string has leading '0's.
 *
 * <p>You can create a bit string from a string of 0s and 1s
 * ({@link #BitString(String, int)} or {@link #createFromBitString}), or from a
 * string of hex digits ({@link #createFromHexString}). You can convert it to a
 * byte array ({@link #getAsByteArray}), to a bit string ({@link #toBitString}),
 * or to a hex string ({@link #toHexString}). A utility method
 * {@link #toByteArrayFromBitString} converts a bit string directly to a byte
 * array.
 *
 * <p>This class is immutable: once created, none of the methods modify the
 * value.
 */
public class BitString {
  //~ Instance fields --------------------------------------------------------

  private final String bits;
  private final int bitCount;

  //~ Constructors -----------------------------------------------------------

  protected BitString(
      String bits,
      int bitCount) {
    assert bits.replaceAll("1", "").replaceAll("0", "").length() == 0
        : "bit string '" + bits + "' contains digits other than {0, 1}";
    this.bits = bits;
    this.bitCount = bitCount;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a BitString representation out of a Hex String. Initial zeros are
   * be preserved. Hex String is defined in the SQL standard to be a string
   * with odd number of hex digits. An even number of hex digits is in the
   * standard a Binary String.
   *
   * @param s a string, in hex notation
   * @throws NumberFormatException if <code>s</code> is invalid.
   */
  public static BitString createFromHexString(String s) {
    int bitCount = s.length() * 4;
    String bits = (bitCount == 0) ? "" : new BigInteger(s, 16).toString(2);
    return new BitString(bits, bitCount);
  }

  /**
   * Creates a BitString representation out of a Bit String. Initial zeros are
   * be preserved.
   *
   * @param s a string of 0s and 1s.
   * @throws NumberFormatException if <code>s</code> is invalid.
   */
  public static BitString createFromBitString(String s) {
    int n = s.length();
    if (n > 0) { // check that S is valid
      Util.discard(new BigInteger(s, 2));
    }
    return new BitString(s, n);
  }

  public String toString() {
    return toBitString();
  }

  public int getBitCount() {
    return bitCount;
  }

  public byte[] getAsByteArray() {
    return toByteArrayFromBitString(bits, bitCount);
  }

  /**
   * Returns this bit string as a bit string, such as "10110".
   */
  public String toBitString() {
    return bits;
  }

  /**
   * Converts this bit string to a hex string, such as "7AB".
   */
  public String toHexString() {
    byte[] bytes = getAsByteArray();
    String s = ConversionUtil.toStringFromByteArray(bytes, 16);
    switch (bitCount % 8) {
    case 1: // B'1' -> X'1'
    case 2: // B'10' -> X'2'
    case 3: // B'100' -> X'4'
    case 4: // B'1000' -> X'8'
      return s.substring(1);
    case 5: // B'10000' -> X'10'
    case 6: // B'100000' -> X'20'
    case 7: // B'1000000' -> X'40'
    case 0: // B'10000000' -> X'80', and B'' -> X''
      return s;
    }
    if ((bitCount % 8) == 4) {
      return s.substring(1);
    } else {
      return s;
    }
  }

  /**
   * Converts a bit string to an array of bytes.
   */
  public static byte[] toByteArrayFromBitString(
      String bits,
      int bitCount) {
    if (bitCount < 0) {
      return new byte[0];
    }
    int byteCount = (bitCount + 7) / 8;
    byte[] srcBytes;
    if (bits.length() > 0) {
      BigInteger bigInt = new BigInteger(bits, 2);
      srcBytes = bigInt.toByteArray();
    } else {
      srcBytes = new byte[0];
    }
    byte[] dest = new byte[byteCount];

    // If the number started with 0s, the array won't be very long. Assume
    // that ret is already initialized to 0s, and just copy into the
    // RHS of it.
    int bytesToCopy = Math.min(byteCount, srcBytes.length);
    System.arraycopy(
        srcBytes,
        srcBytes.length - bytesToCopy,
        dest,
        dest.length - bytesToCopy,
        bytesToCopy);
    return dest;
  }

  /**
   * Concatenates some BitStrings. Concatenates all at once, not pairwise, to
   * avoid string copies.
   *
   * @param args BitString[]
   */
  public static BitString concat(List<BitString> args) {
    if (args.size() < 2) {
      return args.get(0);
    }
    int length = 0;
    for (BitString arg : args) {
      length += arg.bitCount;
    }
    StringBuilder sb = new StringBuilder(length);
    for (BitString arg1 : args) {
      sb.append(arg1.bits);
    }
    return new BitString(
        sb.toString(),
        length);
  }

  /**
   * Creates a BitString from an array of bytes.
   *
   * @param bytes Bytes
   * @return BitString
   */
  public static BitString createFromBytes(byte[] bytes) {
    assert bytes != null;
    int bitCount = bytes.length * 8;
    StringBuilder sb = new StringBuilder(bitCount);
    for (byte b : bytes) {
      for (int i = 7; i >= 0; --i) {
        sb.append(((b & 1) == 0) ? '0' : '1');
        b >>= 1;
      }
    }
    return new BitString(sb.toString(), bitCount);
  }
}

// End BitString.java
