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

import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlUtil;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A string, optionally with {@link Charset character set} and
 * {@link SqlCollation}. It is immutable.
 */
public class NlsString implements Comparable<NlsString>, Cloneable {
  //~ Instance fields --------------------------------------------------------

  private final LoadingCache<ByteString, String> decodeMap =
      CacheBuilder.newBuilder()
          .softValues()
          .build(
              new CacheLoader<ByteString, String>() {
                public String load(ByteString key) {
                  CharsetDecoder decoder = key.charset.newDecoder();
                  ByteBuffer buffer = ByteBuffer.wrap(key.value);
                  try {
                    return decoder.decode(buffer).toString();
                  } catch (CharacterCodingException ex) {
                    throw RESOURCE.charsetEncoding(
                        new String(key.value, Charset.defaultCharset()),
                        charset.name()).ex();
                  }
                }
              }
          );

  private final String charsetName;
  private ByteString valueBytes;
  private String valueString;
  private final Charset charset;
  private final SqlCollation collation;

  /**
   * Internal class for cache loader.
   */
  static class ByteString implements Comparable<ByteString> {
    private final byte[] value;
    private final Charset charset;

    ByteString(byte[] value, Charset charset) {
      assert value != null;
      this.value = value.clone();
      this.charset = Util.first(charset, Charset.defaultCharset());
    }

    @Override public int hashCode() {
      return Objects.hash(value, charset);
    }

    @Override public boolean equals(Object obj) {
      return obj instanceof ByteString
          && this.compareTo((ByteString) obj) == 0;
    }

    @Override public int compareTo(ByteString that) {
      if (this == that) {
        return 0;
      }
      final byte[] v1 = value;
      final byte[] v2 = that.value;
      final int min = Math.min(v1.length, v2.length);
      for (int i=0; i<min; i++) {
        int d= v1[i] - v2[i];
        if (d != 0) {
          return d;
        }
      }
      int d = v1.length - v2.length;
      if (d != 0) {
        return d;
      }
      return charset.compareTo(that.charset);
    }

    public byte[] getValue() {
      return value.clone();
    }
  }

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a string in a specified character set.
   *
   * @param value       String constant in byte array, must not be null
   * @param charsetName Name of the character set, may be null
   * @param collation   Collation, may be null
   * @throws IllegalCharsetNameException If the given charset name is illegal
   * @throws UnsupportedCharsetException If no support for the named charset
   *     is available in this instance of the Java virtual machine
   * @throws RuntimeException If the given value cannot be represented in the
   *     given charset
   */
  public NlsString(
      byte[] value,
      String charsetName,
      SqlCollation collation) {
    assert value != null;
    if (charsetName != null) {
      this.charsetName = charsetName.toUpperCase(Locale.ROOT);
      charset = SqlUtil.getCharset(charsetName);
      SqlUtil.validateCharset(value, charset);
    } else {
      this.charsetName = null;
      charset = null;
    }
    this.collation = collation;
    // Validate charset
    this.valueBytes = new ByteString(value, charset);
  }

  /**
   * Easy constructor for java string.
   *
   * @param value String constant, must not be null
   * @param charsetName Name of the character set, may be null
   * @param collation Collation, may be null
   * @throws IllegalCharsetNameException If the given charset name is illegal
   * @throws UnsupportedCharsetException If no support for the named charset
   *     is available in this instance of the Java virtual machine
   * @throws RuntimeException If the given value cannot be represented in the
   *     given charset
   */
  public NlsString(
      String value,
      String charsetName,
      SqlCollation collation) {
    assert value != null;
    if (charsetName != null) {
      this.charsetName = charsetName.toUpperCase(Locale.ROOT);
      this.charset = SqlUtil.getCharset(charsetName);
      // Java string can be malformed if LATIN1 is required.
      if (this.charsetName.equals("LATIN1")
          || this.charsetName.equals("ISO-8859-1")) {
        if (!charset.newEncoder().canEncode(value)) {
          throw RESOURCE.charsetEncoding(value, charset.name()).ex();
        }
      }
    } else {
      this.charsetName = null;
      this.charset = null;
    }
    this.collation = collation;
    this.valueString = value;
  }

  //~ Methods ----------------------------------------------------------------

  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      throw new AssertionError();
    }
  }

  public int hashCode() {
    return Objects.hash(valueBytes, valueString, charsetName, collation);
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof NlsString)) {
      return false;
    }
    NlsString that = (NlsString) obj;
    return Objects.equals(valueBytes, that.valueBytes)
        && Objects.equals(valueString, that.valueString)
        && Objects.equals(charsetName, that.charsetName)
        && Objects.equals(collation, that.collation);
  }

  @Override public int compareTo(NlsString other) {
    // TODO jvs 18-Jan-2006:  Actual collation support.  This just uses
    // the default collation.
    return getValue().compareTo(other.getValue());
  }

  public String getCharsetName() {
    return charsetName;
  }

  public Charset getCharset() {
    return charset;
  }

  public SqlCollation getCollation() {
    return collation;
  }

  public String getValue() {
    if (valueString == null) {
      assert valueBytes != null;
      return decodeMap.getUnchecked(valueBytes);
    }
    return valueString;
  }

  /**
   * Returns a string the same as this but with spaces trimmed from the
   * right.
   */
  public NlsString rtrim() {
    String trimmed = SqlFunctions.rtrim(getValue());
    if (!trimmed.equals(getValue())) {
      return new NlsString(trimmed, charsetName, collation);
    }
    return this;
  }

  /**
   * Returns the string quoted for SQL, for example <code>_ISO-8859-1'is it a
   * plane? no it''s superman!'</code>.
   *
   * @param prefix if true, prefix the character set name
   * @param suffix if true, suffix the collation clause
   * @return the quoted string
   */
  public String asSql(
      boolean prefix,
      boolean suffix) {
    StringBuilder ret = new StringBuilder();
    if (prefix && (null != charsetName)) {
      ret.append("_");
      ret.append(charsetName);
    }
    ret.append("'");
    ret.append(Util.replace(getValue(), "'", "''"));
    ret.append("'");

    // NOTE jvs 3-Feb-2005:  see FRG-78 for why this should go away
    if (false) {
      if (suffix && (null != collation)) {
        ret.append(" ");
        ret.append(collation.toString());
      }
    }
    return ret.toString();
  }

  /**
   * Returns the string quoted for SQL, for example <code>_ISO-8859-1'is it a
   * plane? no it''s superman!'</code>.
   */
  public String toString() {
    return asSql(true, true);
  }

  /**
   * Concatenates some {@link NlsString} objects. The result has the charset
   * and collation of the first element. The other elements must have matching
   * (or null) charset and collation. Concatenates all at once, not pairwise,
   * to avoid string copies.
   *
   * @param args array of {@link NlsString} to be concatenated
   */
  public static NlsString concat(List<NlsString> args) {
    if (args.size() < 2) {
      return args.get(0);
    }
    String charSetName = args.get(0).charsetName;
    SqlCollation collation = args.get(0).collation;
    int length = args.get(0).getValue().length();

    // sum string lengths and validate
    for (int i = 1; i < args.size(); i++) {
      final NlsString arg = args.get(i);
      length += arg.getValue().length();
      if (!((arg.charsetName == null)
          || arg.charsetName.equals(charSetName))) {
        throw new IllegalArgumentException("mismatched charsets");
      }
      if (!((arg.collation == null)
          || arg.collation.equals(collation))) {
        throw new IllegalArgumentException("mismatched collations");
      }
    }

    StringBuilder sb = new StringBuilder(length);
    for (NlsString arg : args) {
      sb.append(arg.getValue());
    }
    return new NlsString(
        sb.toString(),
        charSetName,
        collation);
  }

  /** Creates a copy of this {@code NlsString} with different content but same
   * charset and collation. */
  public NlsString copy(String value) {
    return new NlsString(value, charsetName, collation);
  }

  /**
   * Get value in raw bytes.
   *
   * @return value bytes.
   */
  public byte[] getValueBytes() {
    return valueBytes.getValue();
  }
}

// End NlsString.java
