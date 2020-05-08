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

import java.util.function.BinaryOperator;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A collection of functions used in bitwise processing.
 */
public class BitwiseFunctions {

  private BitwiseFunctions(){}

  // &
  /** Helper function for implementing <code>BIT_AND</code> applied to integer values */
  public static long bitAnd(long b0, long b1) {
    return b0 & b1;
  }

  /** Helper function for implementing <code>BIT_AND</code> applied to binary values */
  public static ByteString bitAnd(ByteString b0, ByteString b1) {
    return binaryOperator(b0, b1, (x, y) -> (byte) (x & y));
  }

  // |
  /** Helper function for implementing <code>BIT_OR</code> applied to integer values */
  public static long bitOr(long b0, long b1) {
    return b0 | b1;
  }

  /** Helper function for implementing <code>BIT_OR</code> applied to binary values */
  public static ByteString bitOr(ByteString b0, ByteString b1) {
    return binaryOperator(b0, b1, (x, y) -> (byte) (x | y));
  }

  // ^
  /** Helper function for implementing <code>BIT_XOR</code> applied to integer values */
  public static long bitXor(long b0, long b1) {
    return b0 ^ b1;
  }

  /** Helper function for implementing <code>BIT_XOR</code> applied to binary values */
  public static ByteString bitXor(ByteString b0, ByteString b1) {
    return binaryOperator(b0, b1, (x, y) -> (byte) (x ^ y));
  }

  /**
   * Utility for bitwise function applied to two byteString values.
   *
   * @param b0 The first byteString value operand of bitwise function.
   * @param b1 The second byteString value operand of bitwise function.
   * @param bitOp BitWise binary operator.
   * @return ByteString after bitwise operation.
   */
  private static ByteString binaryOperator(
      ByteString b0, ByteString b1, BinaryOperator<Byte> bitOp) {
    if (b0.length() == 0) {
      return b1;
    }
    if (b1.length() == 0) {
      return b0;
    }

    if (b0.length() != b1.length()) {
      throw RESOURCE.differentLengthForBitwiseOperands(
          b0.length(), b1.length()).ex();
    }

    final byte[] result = new byte[b0.length()];
    for (int i = 0; i < b0.length(); i++) {
      result[i] = bitOp.apply(b0.byteAt(i), b1.byteAt(i));
    }

    return new ByteString(result);
  }
}
