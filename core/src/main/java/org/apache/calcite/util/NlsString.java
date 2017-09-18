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

import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
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

  private final String charsetName;
  private final String value;
  private final Charset charset;
  private final SqlCollation collation;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a string in a specified character set.
   *
   * @param value       String constant, must not be null
   * @param charsetName Name of the character set, may be null
   * @param collation   Collation, may be null
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
    if (null != charsetName) {
      charsetName = charsetName.toUpperCase(Locale.ROOT);
      this.charsetName = charsetName;
      String javaCharsetName =
          SqlUtil.translateCharacterSetName(charsetName);
      if (javaCharsetName == null) {
        throw new UnsupportedCharsetException(charsetName);
      }
      this.charset = Charset.forName(javaCharsetName);
      CharsetEncoder encoder = charset.newEncoder();

      // dry run to see if encoding hits any problems
      try {
        encoder.encode(CharBuffer.wrap(value));
      } catch (CharacterCodingException ex) {
        throw RESOURCE.charsetEncoding(value, javaCharsetName).ex();
      }
    } else {
      this.charsetName = null;
      this.charset = null;
    }
    this.collation = collation;
    this.value = value;
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
    return Objects.hash(value, charsetName, collation);
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof NlsString)) {
      return false;
    }
    NlsString that = (NlsString) obj;
    return Objects.equals(value, that.value)
        && Objects.equals(charsetName, that.charsetName)
        && Objects.equals(collation, that.collation);
  }

  // implement Comparable
  public int compareTo(NlsString other) {
    // TODO jvs 18-Jan-2006:  Actual collation support.  This just uses
    // the default collation.

    return value.compareTo(other.value);
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
    return value;
  }

  /**
   * Returns a string the same as this but with spaces trimmed from the
   * right.
   */
  public NlsString rtrim() {
    String trimmed = SqlFunctions.rtrim(value);
    if (!trimmed.equals(value)) {
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
    ret.append(Util.replace(value, "'", "''"));
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
    int length = args.get(0).value.length();

    // sum string lengths and validate
    for (int i = 1; i < args.size(); i++) {
      final NlsString arg = args.get(i);
      length += arg.value.length();
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
      sb.append(arg.value);
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
}

// End NlsString.java
