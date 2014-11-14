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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;

/**
 * Serializable wrapper around a {@link Charset}.
 *
 * <p>It serializes itself by writing out the name of the character set, for
 * example "ISO-8859-1". On the other side, it deserializes itself by looking
 * for a charset with the same name.
 *
 * <p>A SerializableCharset is immutable.
 */
public class SerializableCharset implements Serializable {
  //~ Instance fields --------------------------------------------------------

  private Charset charset;
  private String charsetName;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SerializableCharset. External users should call
   * {@link #forCharset(Charset)}.
   *
   * @param charset Character set; must not be null
   */
  private SerializableCharset(Charset charset) {
    assert charset != null;
    this.charset = charset;
    this.charsetName = charset.name();
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Per {@link Serializable}.
   */
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeObject(charset.name());
  }

  /**
   * Per {@link Serializable}.
   */
  private void readObject(ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    charsetName = (String) in.readObject();
    charset = Charset.availableCharsets().get(this.charsetName);
  }

  /**
   * Returns the wrapped {@link Charset}.
   *
   * @return the wrapped Charset
   */
  public Charset getCharset() {
    return charset;
  }

  /**
   * Returns a SerializableCharset wrapping the given Charset, or null if the
   * {@code charset} is null.
   *
   * @param charset Character set to wrap, or null
   * @return Wrapped charset
   */
  public static SerializableCharset forCharset(Charset charset) {
    if (charset == null) {
      return null;
    }
    return new SerializableCharset(charset);
  }
}

// End SerializableCharset.java
