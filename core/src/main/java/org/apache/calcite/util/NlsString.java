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

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

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

import static java.util.Objects.requireNonNull;

/**
 * A string, optionally with {@link Charset character set} and
 * {@link SqlCollation}. It is immutable.
 */
public class NlsString implements Comparable<NlsString>, Cloneable {
  //~ Instance fields --------------------------------------------------------

  private static final LoadingCache<Pair<ByteString, Charset>, String>
      DECODE_MAP =
      CacheBuilder.newBuilder()
          .softValues()
          .build(
              new CacheLoader<Pair<ByteString, Charset>, String>() {
                @Override public String load(Pair<ByteString, Charset> key) {
                  final Charset charset = key.right;
                  final CharsetDecoder decoder = charset.newDecoder();
                  final byte[] bytes = key.left.getBytes();
                  final ByteBuffer buffer = ByteBuffer.wrap(bytes);
                  try {
                    return decoder.decode(buffer).toString();
                  } catch (CharacterCodingException ex) {
                    throw RESOURCE.charsetEncoding(
                        //CHECKSTYLE: IGNORE 1
                        new String(bytes, Charset.defaultCharset()),
                        charset.name()).ex();
                  }
                }
              });

  private final @Nullable String stringValue;
  @JsonProperty("valueBytes")
  private final @Nullable ByteString bytesValue;
  private final @Nullable String charsetName;
  private final @Nullable Charset charset;
  private final @Nullable SqlCollation collation;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a string in a specified character set.
   *
   * @param bytesValue  Byte array constant, must not be null
   * @param charsetName Name of the character set, must not be null
   * @param collation   Collation, may be null
   *
   * @throws IllegalCharsetNameException If the given charset name is illegal
   * @throws UnsupportedCharsetException If no support for the named charset
   *     is available in this instance of the Java virtual machine
   * @throws RuntimeException If the given value cannot be represented in the
   *     given charset
   */
  public NlsString(ByteString bytesValue, String charsetName,
      @Nullable SqlCollation collation) {
    this(null, requireNonNull(bytesValue, "bytesValue"),
        requireNonNull(charsetName, "charsetName"), collation);
  }

  /**
   * Easy constructor for Java string.
   *
   * @param stringValue String constant, must not be null
   * @param charsetName Name of the character set, may be null
   * @param collation Collation, may be null
   *
   * @throws IllegalCharsetNameException If the given charset name is illegal
   * @throws UnsupportedCharsetException If no support for the named charset
   *     is available in this instance of the Java virtual machine
   * @throws RuntimeException If the given value cannot be represented in the
   *     given charset
   */
  @JsonCreator
  public NlsString(@JsonProperty("value") String stringValue,
      @JsonProperty("charsetName") @Nullable String charsetName,
      @JsonProperty("collation") @Nullable SqlCollation collation) {
    this(requireNonNull(stringValue, "stringValue"), null, charsetName,
        collation);
  }

  /** Internal constructor; other constructors must call it. */
  private NlsString(@Nullable String stringValue, @Nullable ByteString bytesValue,
      @Nullable String charsetName, @Nullable SqlCollation collation) {
    if (charsetName != null) {
      this.charsetName = charsetName.toUpperCase(Locale.ROOT);
      this.charset = SqlUtil.getCharset(charsetName);
    } else {
      this.charsetName = null;
      this.charset = null;
    }
    if ((stringValue != null) == (bytesValue != null)) {
      throw new IllegalArgumentException("Specify stringValue or bytesValue");
    }
    if (bytesValue != null) {
      if (charset == null) {
        throw new IllegalArgumentException("Bytes value requires charset");
      }
      SqlUtil.validateCharset(bytesValue, charset);
    } else {
      //noinspection ConstantConditions
      requireNonNull(stringValue, "stringValue");
      // Java string can be malformed if LATIN1 is required.
      if (this.charsetName != null
          && (this.charsetName.equals("LATIN1")
          || this.charsetName.equals("ISO-8859-1"))) {
        //noinspection ConstantConditions
        requireNonNull(charset, "charset");
        if (!charset.newEncoder().canEncode(stringValue)) {
          throw RESOURCE.charsetEncoding(stringValue, charset.name()).ex();
        }
      }
    }
    this.collation = collation;
    this.stringValue = stringValue;
    this.bytesValue = bytesValue;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      throw new AssertionError();
    }
  }

  @Override public int hashCode() {
    return Objects.hash(stringValue, bytesValue, charsetName, collation);
  }

  @Override public boolean equals(@Nullable Object obj) {
    return this == obj
        || obj instanceof NlsString
        && Objects.equals(stringValue, ((NlsString) obj).stringValue)
        && Objects.equals(bytesValue, ((NlsString) obj).bytesValue)
        && Objects.equals(charsetName, ((NlsString) obj).charsetName)
        && Objects.equals(collation, ((NlsString) obj).collation);
  }

  @Override public int compareTo(NlsString other) {
    if (collation != null && collation.getCollator() != null) {
      return collation.getCollator().compare(getValue(), other.getValue());
    }
    return getValue().compareTo(other.getValue());
  }

  @Pure
  public @Nullable String getCharsetName() {
    return charsetName;
  }

  @Pure
  public @Nullable Charset getCharset() {
    return charset;
  }

  @Pure
  public @Nullable SqlCollation getCollation() {
    return collation;
  }

  public String getValue() {
    if (stringValue == null) {
      requireNonNull(bytesValue, "bytesValue");
      requireNonNull(charset, "charset");
      return DECODE_MAP.getUnchecked(Pair.of(bytesValue, charset));
    }
    return stringValue;
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
   * Returns a string the same as this but with spaces trimmed from the
   * left and right.
   */
  public NlsString trim(String trimed) {
    String trimmed = SqlFunctions.trim(true, true, trimed, getValue());
    if (!trimmed.equals(getValue())) {
      return new NlsString(trimmed, charsetName, collation);
    }
    return this;
  }

  /**
   * Returns a string the same as this but with spaces trimmed from the
   * left.
   */
  public NlsString ltrim(String trimed) {
    String trimmed = SqlFunctions.trim(true, false, trimed, getValue());
    if (!trimmed.equals(getValue())) {
      return new NlsString(trimmed, charsetName, collation);
    }
    return this;
  }

  /**
   * Returns a string the same as this but with spaces trimmed from the
   * right.
   */
  public NlsString rtrim(String trimed) {
    String trimmed = SqlFunctions.trim(false, true, trimed, getValue());
    if (!trimmed.equals(getValue())) {
      return new NlsString(trimmed, charsetName, collation);
    }
    return this;
  }

  /**
   * Returns a string the same as this but with spaces trimmed from the
   * right.  The result is never shorter than minResultSize.
   *
   * @param minResultSize Expected size for result string.  If negative, it indicates
   *                   that no trimming should be done.
   */
  public NlsString rtrim(int minResultSize) {
    String value = getValue();
    if (value.length() <= minResultSize || minResultSize < 0) {
      return this;
    }
    String left = value.substring(0, minResultSize);
    String right = value.substring(minResultSize);
    String trimmed = left + SqlFunctions.rtrim(right);
    if (!trimmed.equals(value)) {
      return new NlsString(trimmed, charsetName, collation);
    }
    return this;
  }

  /** As {@link #asSql(boolean, boolean, SqlDialect)} but with SQL standard
   * dialect. */
  public String asSql(boolean prefix, boolean suffix) {
    return asSql(prefix, suffix, AnsiSqlDialect.DEFAULT);
  }

  /**
   * Returns the string quoted for SQL, for example <code>_ISO-8859-1'is it a
   * plane? no it''s superman!'</code>.
   *
   * @param prefix if true, prefix the character set name
   * @param suffix if true, suffix the collation clause
   * @param dialect Dialect
   * @return the quoted string
   */
  public String asSql(
      boolean prefix,
      boolean suffix,
      SqlDialect dialect) {
    StringBuilder ret = new StringBuilder();
    dialect.quoteStringLiteral(ret, prefix ? charsetName : null, getValue());

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
  @Override public String toString() {
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

  /** Returns the value as a {@link ByteString}. */
  @Pure
  public @Nullable ByteString getValueBytes() {
    return bytesValue;
  }
}
