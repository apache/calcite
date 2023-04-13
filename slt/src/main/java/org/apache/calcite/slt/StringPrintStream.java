/*
 * Copyright 2023 VMware, Inc.
 * SPDX-License-Identifier: MIT
 * SPDX-License-Identifier: Apache-2.0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.apache.calcite.slt;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

/**
 * A PrintStream that writes do a String.
 */
public class StringPrintStream {
  PrintStream stream;
  ByteArrayOutputStream byteStream;
  boolean closed = false;

  public StringPrintStream() throws UnsupportedEncodingException {
    this.byteStream = new ByteArrayOutputStream();
    this.stream = new PrintStream(this.byteStream, true, StandardCharsets.UTF_8.name());
  }

  public PrintStream getPrintStream() {
    return this.stream;
  }

  /**
   * Get the data written so far.  Once this is done the stream is closed and can't be used anymore.
   */
  @Override
  public String toString() {
    if (!this.closed)
      this.stream.close();
    this.closed = true;
    return this.byteStream.toString();
  }
}
