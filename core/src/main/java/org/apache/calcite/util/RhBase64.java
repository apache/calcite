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
package org.eigenbase.util;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Encodes and decodes to and from Base64 notation.
 *
 * <p>Change Log:</p>
 *
 * <ul>
 * <li>v2.1 - Cleaned up javadoc comments and unused variables and methods.
 * Added some convenience methods for reading and writing to and from
 * files.</li>
 * <li>v2.0.2 - Now specifies UTF-8 encoding in places where the code fails on
 * systems with other encodings (like EBCDIC).</li>
 * <li>v2.0.1 - Fixed an error when decoding a single byte, that is, when the
 * encoded data was a single byte.</li>
 * <li>v2.0 - I got rid of methods that used booleans to set options. Now
 * everything is more consolidated and cleaner. The code now detects when data
 * that's being decoded is gzip-compressed and will decompress it automatically.
 * Generally things are cleaner. You'll probably have to change some method
 * calls that you were making to support the new options format (<tt>int</tt>s
 * that you "OR" together).</li>
 * <li>v1.5.1 - Fixed bug when decompressing and decoding to a byte[] using <tt>
 * decode( String s, boolean gzipCompressed )</tt>. Added the ability to
 * "suspend" encoding in the Output Stream so you can turn on and off the
 * encoding if you need to embed base64 data in an otherwise "normal" stream
 * (like an XML file).</li>
 * <li>v1.5 - Output stream pases on flush() command but doesn't do anything
 * itself. This helps when using GZIP streams. Added the ability to
 * GZip-compress objects before encoding them.</li>
 * <li>v1.4 - Added helper methods to read/write files.</li>
 * <li>v1.3.6 - Fixed OutputStream.flush() so that 'position' is reset.</li>
 * <li>v1.3.5 - Added flag to turn on and off line breaks. Fixed bug in input
 * stream where last buffer being read, if not completely full, was not
 * returned.</li>
 * <li>v1.3.4 - Fixed when "improperly padded stream" error was thrown at the
 * wrong time.</li>
 * <li>v1.3.3 - Fixed I/O streams which were totally messed up.</li>
 * </ul>
 *
 * <p>I am placing this code in the Public Domain. Do with it as you will. This
 * software comes with no guarantees or warranties but with plenty of
 * well-wishing instead! Please visit <a href="http://iharder.net/base64">
 * http://iharder.net/base64</a> periodically to check for updates or to
 * contribute improvements.</p>
 *
 * <p>Author: Robert Harder, rob@iharder.net</p>
 *
 * @version 2.1
 */
public class RhBase64 {
  // ********  P U B L I C   F I E L D S  ********

  //~ Static fields/initializers ---------------------------------------------

  /**
   * No options specified. Value is zero.
   */
  public static final int NO_OPTIONS = 0;

  /**
   * Specify encoding.
   */
  public static final int ENCODE = 1;

  /**
   * Specify decoding.
   */
  public static final int DECODE = 0;

  /**
   * Specify that data should be gzip-compressed.
   */
  public static final int GZIP = 2;

  /**
   * Don't break lines when encoding (violates strict Base64 specification)
   */
  public static final int DONT_BREAK_LINES = 8;

  // ********  P R I V A T E   F I E L D S  ********

  /**
   * Maximum line length (76) of Base64 output.
   */
  private static final int MAX_LINE_LENGTH = 76;

  /**
   * The equals sign (=) as a byte.
   */
  private static final byte EQUALS_SIGN = (byte) '=';

  /**
   * The new line character (\n) as a byte.
   */
  private static final byte NEW_LINE = (byte) '\n';

  /**
   * Preferred encoding.
   */
  private static final String PREFERRED_ENCODING = "UTF-8";

  /**
   * The 64 valid Base64 values.
   */
  private static final byte[] ALPHABET;
  private static final byte[] NATIVE_ALPHABET = {
    // May be something funny like EBCDIC
    (byte) 'A', (byte) 'B', (byte) 'C', (byte) 'D', (byte) 'E', (byte) 'F',
    (byte) 'G', (byte) 'H', (byte) 'I', (byte) 'J', (byte) 'K', (byte) 'L',
    (byte) 'M', (byte) 'N', (byte) 'O', (byte) 'P', (byte) 'Q', (byte) 'R',
    (byte) 'S', (byte) 'T', (byte) 'U', (byte) 'V', (byte) 'W', (byte) 'X',
    (byte) 'Y', (byte) 'Z', (byte) 'a', (byte) 'b', (byte) 'c', (byte) 'd',
    (byte) 'e', (byte) 'f', (byte) 'g', (byte) 'h', (byte) 'i', (byte) 'j',
    (byte) 'k', (byte) 'l', (byte) 'm', (byte) 'n', (byte) 'o', (byte) 'p',
    (byte) 'q', (byte) 'r', (byte) 's', (byte) 't', (byte) 'u', (byte) 'v',
    (byte) 'w', (byte) 'x', (byte) 'y', (byte) 'z', (byte) '0', (byte) '1',
    (byte) '2', (byte) '3', (byte) '4', (byte) '5', (byte) '6', (byte) '7',
    (byte) '8', (byte) '9', (byte) '+', (byte) '/'
  };

  /**
   * Determine which ALPHABET to use.
   */
  static {
    byte[] bytes;
    try {
      bytes =
          "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
              .getBytes(PREFERRED_ENCODING);
    } catch (UnsupportedEncodingException use) {
      bytes = NATIVE_ALPHABET; // Fall back to native encoding
    }
    ALPHABET = bytes;
  }

  /**
   * Translates a Base64 value to either its 6-bit reconstruction value or a
   * negative number indicating some other meaning.
   */
  private static final byte[] DECODABET = {
    -9, -9, -9, -9, -9, -9, -9, -9, -9, // Decimal  0 -  8
    -5, -5, // Whitespace: Tab and Linefeed
    -9, -9, // Decimal 11 - 12
    -5, // Whitespace: Carriage Return
    -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, // Decimal 14 - 26
    -9, -9, -9, -9, -9, // Decimal 27 - 31
    -5, // Whitespace: Space
    -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, // Decimal 33 - 42
    62, // Plus sign at decimal 43
    -9, -9, -9, // Decimal 44 - 46
    63, // Slash at decimal 47
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, // Numbers zero through nine
    -9, -9, -9, // Decimal 58 - 60
    -1, // Equals sign at decimal 61
    -9, -9, -9, // Decimal 62 - 64
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, // Letters 'A' through 'N'
    14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, // Letters 'O' through 'Z'
    -9, -9, -9, -9, -9, -9, // Decimal 91 - 96
    26, 27, 28, 29, 30, 31, 32, // Letters 'a' through 'f'
    33, 34, 35, 36, 37, 38,     // Letters 'g' through 'm'
    39, 40, 41, 42, 43, 44, 45, // Letters 'n' through 't'
    46, 47, 48, 49, 50, 51,     // Letters 'u' through 'z'
    -9, -9, -9, -9 // Decimal 123 - 126

/*
        ,
        -9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,     // Decimal 127 - 139
        -9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,     // Decimal 140 - 152
         -9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,     // Decimal 153 - 165
         -9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,     // Decimal 166 - 178
         -9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,     // Decimal 179 - 191
         -9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,     // Decimal 192 - 204
         -9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,     // Decimal 205 - 217
         -9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,     // Decimal 218 - 230
         -9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,     // Decimal 231 -
         243-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9,-9         // Decimal 244 - 255
*/
  };

  // I think I end up not using the BAD_ENCODING indicator. private final
  // static byte BAD_ENCODING    = -9; // Indicates error in encoding
  private static final byte WHITE_SPACE_ENC = -5; // Indicates white space in
  // encoding
  private static final byte EQUALS_SIGN_ENC = -1; // Indicates equals sign in
  // encoding

  //~ Constructors -----------------------------------------------------------

  /**
   * Defeats instantiation.
   */
  private RhBase64() {
  }

  //~ Methods ----------------------------------------------------------------

  // ********  E N C O D I N G   M E T H O D S  ********

  /**
   * Encodes up to the first three bytes of array {@code threeBytes} and
   * returns a four-byte array in Base64 notation. The actual number of
   * significant bytes in your array is given by {@code numSigBytes}. The
   * array {@code threeBytes} needs only be as big as {@code numSigBytes}.
   * Code can reuse a byte array by passing a four-byte
   * array as {@code b4}.
   *
   * @param b4          A reusable byte array to reduce array instantiation
   * @param threeBytes  the array to convert
   * @param numSigBytes the number of significant bytes in your array
   * @return four byte array in Base64 notation.
   * @since 1.5.1
   */
  private static byte[] encode3to4(
      byte[] b4,
      byte[] threeBytes,
      int numSigBytes) {
    encode3to4(threeBytes, 0, numSigBytes, b4, 0);
    return b4;
  }

  /**
   * Encodes up to three bytes of the array {@code source} and writes the
   * resulting four Base64 bytes to {@code destination}. The source and
   * destination arrays can be manipulated anywhere along their length by
   * specifying {@code srcOffset} and {@code destOffset}. This method
   * does not check to make sure your arrays are large enough to accomodate
   * {@code srcOffset} + 3 for the {@code source} array or
   * {@code destOffset} + 4 for the {@code destination} array. The actual
   * number of significant bytes in your array is given by {@code numSigBytes}.
   *
   * @param source      the array to convert
   * @param srcOffset   the index where conversion begins
   * @param numSigBytes the number of significant bytes in your array
   * @param destination the array to hold the conversion
   * @param destOffset  the index where output will be put
   * @return the {@code destination} array
   * @since 1.3
   */
  private static byte[] encode3to4(
      byte[] source,
      int srcOffset,
      int numSigBytes,
      byte[] destination,
      int destOffset) {
    //           1         2         3
    // 01234567890123456789012345678901 Bit position
    // --------000000001111111122222222 Array position from threeBytes
    // --------|    ||    ||    ||    | Six bit groups to index ALPHABET
    //          >>18  >>12  >> 6  >> 0  Right shift necessary
    //                0x3f  0x3f  0x3f  Additional AND

    // Create buffer with zero-padding if there are only one or two
    // significant bytes passed in the array. We have to shift left 24 in
    // order to flush out the 1's that appear when Java treats a value as
    // negative that is cast from a byte to an int.
    int inBuff =
        ((numSigBytes > 0) ? ((source[srcOffset] << 24) >>> 8) : 0)
            | ((numSigBytes > 1) ? ((source[srcOffset + 1] << 24) >>> 16) : 0)
            | ((numSigBytes > 2) ? ((source[srcOffset + 2] << 24) >>> 24) : 0);

    switch (numSigBytes) {
    case 3:
      destination[destOffset] = ALPHABET[inBuff >>> 18];
      destination[destOffset + 1] = ALPHABET[(inBuff >>> 12) & 0x3f];
      destination[destOffset + 2] = ALPHABET[(inBuff >>> 6) & 0x3f];
      destination[destOffset + 3] = ALPHABET[inBuff & 0x3f];
      return destination;

    case 2:
      destination[destOffset] = ALPHABET[inBuff >>> 18];
      destination[destOffset + 1] = ALPHABET[(inBuff >>> 12) & 0x3f];
      destination[destOffset + 2] = ALPHABET[(inBuff >>> 6) & 0x3f];
      destination[destOffset + 3] = EQUALS_SIGN;
      return destination;

    case 1:
      destination[destOffset] = ALPHABET[inBuff >>> 18];
      destination[destOffset + 1] = ALPHABET[(inBuff >>> 12) & 0x3f];
      destination[destOffset + 2] = EQUALS_SIGN;
      destination[destOffset + 3] = EQUALS_SIGN;
      return destination;

    default:
      return destination;
    }
  }

  /**
   * Serializes an object and returns the Base64-encoded version of that
   * serialized object. If the object cannot be serialized or there is another
   * error, the method will return <tt>null</tt>. The object is not
   * GZip-compressed before being encoded.
   *
   * @param serializableObject The object to encode
   * @return The Base64-encoded object
   * @since 1.4
   */
  public static String encodeObject(Serializable serializableObject) {
    return encodeObject(serializableObject, NO_OPTIONS);
  }

  /**
   * Serializes an object and returns the Base64-encoded version of that
   * serialized object. If the object cannot be serialized or there is another
   * error, the method will return <tt>null</tt>.
   *
   * <p>Valid options:
   *
   * <pre>
   *   GZIP: gzip-compresses object before encoding it.
   *   DONT_BREAK_LINES: don't break lines at 76 characters
   *     <i>Note: Technically, this makes your encoding non-compliant.</i>
   * </pre>
   *
   * <p>Example: <code>encodeObject( myObj, RhBase64.GZIP )</code> or
   *
   * <p>Example: <code>encodeObject( myObj, RhBase64.GZIP |
   * RhBase64.DONT_BREAK_LINES )</code>
   *
   * @param serializableObject The object to encode
   * @param options            Specified options
   * @return The Base64-encoded object
   * @see RhBase64#GZIP
   * @see RhBase64#DONT_BREAK_LINES
   * @since 2.0
   */
  public static String encodeObject(
      Serializable serializableObject,
      int options) {
    // Streams
    ByteArrayOutputStream baos = null;
    OutputStream b64os = null;
    ObjectOutputStream oos = null;
    GZIPOutputStream gzos = null;

    // Isolate options
    int gzip = options & GZIP;
    int dontBreakLines = options & DONT_BREAK_LINES;

    try {
      // ObjectOutputStream -> (GZIP) -> Base64 -> ByteArrayOutputStream
      baos = new ByteArrayOutputStream();
      b64os = new RhBase64.OutputStream(baos, ENCODE | dontBreakLines);

      // GZip?
      if (gzip == GZIP) {
        gzos = new GZIPOutputStream(b64os);
        oos = new ObjectOutputStream(gzos);
      } else {
        oos = new ObjectOutputStream(b64os);
      }

      oos.writeObject(serializableObject);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    } finally {
      try {
        oos.close();
      } catch (Exception e) {
          // ignore
      }
      try {
        gzos.close();
      } catch (Exception e) {
          // ignore
      }
      try {
        b64os.close();
      } catch (Exception e) {
          // ignore
      }
      try {
        baos.close();
      } catch (Exception e) {
          // ignore
      }
    }

    // Return value according to relevant encoding.
    try {
      return new String(
          baos.toByteArray(),
          PREFERRED_ENCODING);
    } catch (UnsupportedEncodingException uue) {
      return new String(baos.toByteArray());
    }
  }

  /**
   * Encodes a byte array into Base64 notation. Does not GZip-compress data.
   *
   * @param source The data to convert
   * @since 1.4
   */
  public static String encodeBytes(byte[] source) {
    return encodeBytes(source, 0, source.length, NO_OPTIONS);
  }

  /**
   * Encodes a byte array into Base64 notation.
   *
   * <p>Valid options:
   *
   * <pre>
   *   GZIP: gzip-compresses object before encoding it.
   *   DONT_BREAK_LINES: don't break lines at 76 characters
   *     <i>Note: Technically, this makes your encoding non-compliant.</i>
   * </pre>
   *
   * <p>Example: <code>encodeBytes( myData, RhBase64.GZIP )</code> or
   *
   * <p>Example: <code>encodeBytes( myData, RhBase64.GZIP |
   * RhBase64.DONT_BREAK_LINES )</code>
   *
   * @param source  The data to convert
   * @param options Specified options
   * @see RhBase64#GZIP
   * @see RhBase64#DONT_BREAK_LINES
   * @since 2.0
   */
  public static String encodeBytes(byte[] source, int options) {
    return encodeBytes(source, 0, source.length, options);
  }

  /**
   * Encodes a byte array into Base64 notation. Does not GZip-compress data.
   *
   * @param source The data to convert
   * @param off    Offset in array where conversion should begin
   * @param len    Length of data to convert
   * @since 1.4
   */
  public static String encodeBytes(byte[] source, int off, int len) {
    return encodeBytes(source, off, len, NO_OPTIONS);
  }

  /**
   * Encodes a byte array into Base64 notation.
   *
   * <p>Valid options:
   *
   * <pre>
   *   GZIP: gzip-compresses object before encoding it.
   *   DONT_BREAK_LINES: don't break lines at 76 characters
   *     <i>Note: Technically, this makes your encoding non-compliant.</i>
   * </pre>
   *
   * <p>Example: <code>encodeBytes( myData, RhBase64.GZIP )</code> or
   *
   * <p>Example: <code>encodeBytes( myData, RhBase64.GZIP |
   * RhBase64.DONT_BREAK_LINES )</code>
   *
   * @param source  The data to convert
   * @param off     Offset in array where conversion should begin
   * @param len     Length of data to convert
   * @param options Specified options
   * @see RhBase64#GZIP
   * @see RhBase64#DONT_BREAK_LINES
   * @since 2.0
   */
  public static String encodeBytes(
      byte[] source,
      int off,
      int len,
      int options) {
    // Isolate options
    int dontBreakLines = options & DONT_BREAK_LINES;
    int gzip = options & GZIP;

    // Compress?
    if (gzip == GZIP) {
      ByteArrayOutputStream baos = null;
      GZIPOutputStream gzos = null;
      RhBase64.OutputStream b64os = null;

      try {
        // GZip -> Base64 -> ByteArray
        baos = new ByteArrayOutputStream();
        b64os =
            new RhBase64.OutputStream(baos, ENCODE | dontBreakLines);
        gzos = new GZIPOutputStream(b64os);

        gzos.write(source, off, len);
        gzos.close();
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      } finally {
        try {
          gzos.close();
        } catch (Exception e) {
          // ignore
        }
        try {
          b64os.close();
        } catch (Exception e) {
          // ignore
        }
        try {
          baos.close();
        } catch (Exception e) {
          // ignore
        }
      }

      // Return value according to relevant encoding.
      try {
        return new String(
            baos.toByteArray(),
            PREFERRED_ENCODING);
      } catch (UnsupportedEncodingException uue) {
        return new String(baos.toByteArray());
      }
    } else {
      // Else, don't compress. Better not to use streams at all then.
      // Convert option to boolean in way that code likes it.
      boolean breakLines = dontBreakLines == 0;

      int len43 = len * 4 / 3;
      byte[] outBuff =
          new byte[
              len43 // Main 4:3
                  + (((len % 3) > 0) ? 4 : 0) // Account for padding
                  + (breakLines
                  ? (len43 / MAX_LINE_LENGTH)
                  : 0)]; // New lines
      int d = 0;
      int e = 0;
      int len2 = len - 2;
      int lineLength = 0;
      for (; d < len2; d += 3, e += 4) {
        encode3to4(source, d + off, 3, outBuff, e);

        lineLength += 4;
        if (breakLines && (lineLength == MAX_LINE_LENGTH)) {
          outBuff[e + 4] = NEW_LINE;
          e++;
          lineLength = 0;
        }
      } // en dfor: each piece of array

      if (d < len) {
        encode3to4(source, d + off, len - d, outBuff, e);
        e += 4;
      }

      // Return value according to relevant encoding.
      try {
        return new String(outBuff, 0, e, PREFERRED_ENCODING);
      } catch (UnsupportedEncodingException uue) {
        return new String(outBuff, 0, e);
      }
    }
  }

  // ********  D E C O D I N G   M E T H O D S  ********

  /**
   * Decodes four bytes from array {@code source} and writes the resulting
   * bytes (up to three of them) to {@code destination}. The source and
   * destination arrays can be manipulated anywhere along their length by
   * specifying {@code srcOffset} and {@code destOffset}. This method
   * does not check to make sure your arrays are large enough to accomodate
   * {@code srcOffset} + 4 for the {@code source} array or
   * {@code destOffset} + 3 for the {@code destination} array. This method
   * returns the actual number of bytes that were converted from the Base64
   * encoding.
   *
   * @param source      the array to convert
   * @param srcOffset   the index where conversion begins
   * @param destination the array to hold the conversion
   * @param destOffset  the index where output will be put
   * @return the number of decoded bytes converted
   * @since 1.3
   */
  private static int decode4to3(
      byte[] source,
      int srcOffset,
      byte[] destination,
      int destOffset) {
    // Example: Dk==
    if (source[srcOffset + 2] == EQUALS_SIGN) {
      // Two ways to do the same thing. Don't know which way I like best.
      // int outBuff =   ( ( DECODABET[ source[ srcOffset    ] ] << 24 )
      // >>>  6 )              | ( ( DECODABET[ source[ srcOffset + 1] ]
      // << 24 ) >>> 12 );
      int outBuff =
          ((DECODABET[source[srcOffset]] & 0xFF) << 18)
              | ((DECODABET[source[srcOffset + 1]] & 0xFF) << 12);

      destination[destOffset] = (byte) (outBuff >>> 16);
      return 1;
    } else if (source[srcOffset + 3] == EQUALS_SIGN) {
      // Example: DkL=
      // Two ways to do the same thing. Don't know which way I like best.
      // int outBuff =   ( ( DECODABET[ source[ srcOffset     ] ] << 24 )
      // >>>  6 )              | ( ( DECODABET[ source[ srcOffset + 1 ] ]
      // << 24 ) >>> 12 )              | ( ( DECODABET[ source[ srcOffset
      // + 2 ] ] << 24 ) >>> 18 );
      int outBuff =
          ((DECODABET[source[srcOffset]] & 0xFF) << 18)
              | ((DECODABET[source[srcOffset + 1]] & 0xFF) << 12)
              | ((DECODABET[source[srcOffset + 2]] & 0xFF) << 6);

      destination[destOffset] = (byte) (outBuff >>> 16);
      destination[destOffset + 1] = (byte) (outBuff >>> 8);
      return 2;
    } else {
      // Example: DkLE
      try {
        // Two ways to do the same thing. Don't know which way I like
        // best. int outBuff =   ( ( DECODABET[ source[ srcOffset     ]
        // ] << 24 ) >>>  6 )              | ( ( DECODABET[ source[
        // srcOffset + 1 ] ] << 24 ) >>> 12 )              | ( (
        // DECODABET[ source[ srcOffset + 2 ] ] << 24 ) >>> 18 )      |
        // ( ( DECODABET[ source[ srcOffset + 3 ] ] << 24 ) >>> 24 );
        int outBuff =
            ((DECODABET[source[srcOffset]] & 0xFF) << 18)
                | ((DECODABET[source[srcOffset + 1]] & 0xFF) << 12)
                | ((DECODABET[source[srcOffset + 2]] & 0xFF) << 6)
                | (DECODABET[source[srcOffset + 3]] & 0xFF);

        destination[destOffset] = (byte) (outBuff >> 16);
        destination[destOffset + 1] = (byte) (outBuff >> 8);
        destination[destOffset + 2] = (byte) outBuff;

        return 3;
      } catch (Exception e) {
        System.out.println(
            "" + source[srcOffset] + ": "
            + (DECODABET[source[srcOffset]]));
        System.out.println(
            "" + source[srcOffset + 1] + ": "
            + (DECODABET[source[srcOffset + 1]]));
        System.out.println(
            "" + source[srcOffset + 2] + ": "
            + (DECODABET[source[srcOffset + 2]]));
        System.out.println(
            "" + source[srcOffset + 3] + ": "
            + (DECODABET[source[srcOffset + 3]]));
        return -1;
      }
    }
  }

  /**
   * Very low-level access to decoding ASCII characters in the form of a byte
   * array. Does not support automatically gunzipping or any other "fancy"
   * features.
   *
   * @param source The Base64 encoded data
   * @param off    The offset of where to begin decoding
   * @param len    The length of characters to decode
   * @return decoded data
   * @since 1.3
   */
  public static byte[] decode(byte[] source, int off, int len) {
    int len34 = len * 3 / 4;
    byte[] outBuff = new byte[len34]; // Upper limit on size of output
    int outBuffPosn = 0;

    byte[] b4 = new byte[4];
    int b4Posn = 0;
    int i = 0;
    byte sbiCrop = 0;
    byte sbiDecode = 0;
    for (i = off; i < (off + len); i++) {
      sbiCrop = (byte) (source[i] & 0x7f); // Only the low seven bits
      sbiDecode = DECODABET[sbiCrop];

      if (sbiDecode >= WHITE_SPACE_ENC) {
        // White space, Equals sign or better
        if (sbiDecode >= EQUALS_SIGN_ENC) {
          b4[b4Posn++] = sbiCrop;
          if (b4Posn > 3) {
            outBuffPosn += decode4to3(b4, 0, outBuff, outBuffPosn);
            b4Posn = 0;

            // If that was the equals sign, break out of 'for' loop
            if (sbiCrop == EQUALS_SIGN) {
              break;
            }
          }
        }
      } else {
        System.err.println(
            "Bad Base64 input character at " + i + ": " + source[i]
            + "(decimal)");
        return null;
      }
    }

    byte[] out = new byte[outBuffPosn];
    System.arraycopy(outBuff, 0, out, 0, outBuffPosn);
    return out;
  }

  /**
   * Decodes data from Base64 notation, automatically detecting
   * gzip-compressed data and decompressing it.
   *
   * @param s the string to decode
   * @return the decoded data
   * @since 1.4
   */
  public static byte[] decode(String s) {
    byte[] bytes;
    try {
      bytes = s.getBytes(PREFERRED_ENCODING);
    } catch (UnsupportedEncodingException uee) {
      bytes = s.getBytes();
    }

    //</change>

    // Decode
    bytes = decode(bytes, 0, bytes.length);

    // Check to see if it's gzip-compressed
    // GZIP Magic Two-Byte Number: 0x8b1f (35615)
    if ((bytes != null) && (bytes.length >= 4)) {
      int head = ((int) bytes[0] & 0xff) | ((bytes[1] << 8) & 0xff00);
      if (GZIPInputStream.GZIP_MAGIC == head) {
        ByteArrayInputStream bais = null;
        GZIPInputStream gzis = null;
        ByteArrayOutputStream baos = null;
        byte[] buffer = new byte[2048];
        int length = 0;

        try {
          baos = new ByteArrayOutputStream();
          bais = new ByteArrayInputStream(bytes);
          gzis = new GZIPInputStream(bais);

          while ((length = gzis.read(buffer)) >= 0) {
            baos.write(buffer, 0, length);
          }

          // No error? Get new bytes.
          bytes = baos.toByteArray();
        } catch (IOException e) {
          // Just return originally-decoded bytes
        } finally {
          try {
            baos.close();
          } catch (Exception e) {
            // ignore
          }
          try {
            gzis.close();
          } catch (Exception e) {
            // ignore
          }
          try {
            bais.close();
          } catch (Exception e) {
            // ignore
          }
        }
      }
    }

    return bytes;
  }

  /**
   * Attempts to decode Base64 data and deserialize a Java Object within.
   * Returns <tt>null</tt> if there was an error.
   *
   * @param encodedObject The Base64 data to decode
   * @return The decoded and deserialized object
   * @since 1.5
   */
  public static Object decodeToObject(String encodedObject) {
    // Decode and gunzip if necessary
    byte[] objBytes = decode(encodedObject);

    ByteArrayInputStream bais = null;
    ObjectInputStream ois = null;
    Object obj = null;

    try {
      bais = new ByteArrayInputStream(objBytes);
      ois = new ObjectInputStream(bais);

      obj = ois.readObject();
    } catch (IOException e) {
      e.printStackTrace();
      obj = null;
    } catch (java.lang.ClassNotFoundException e) {
      e.printStackTrace();
      obj = null;
    } finally {
      try {
        bais.close();
      } catch (Exception e) {
          // ignore
      }
      try {
        ois.close();
      } catch (Exception e) {
          // ignore
      }
    }

    return obj;
  }

  /**
   * Convenience method for encoding data to a file.
   *
   * @param dataToEncode byte array of data to encode in base64 form
   * @param filename     Filename for saving encoded data
   * @return <tt>true</tt> if successful, <tt>false</tt> otherwise
   * @since 2.1
   */
  public static boolean encodeToFile(byte[] dataToEncode, String filename) {
    boolean success = false;
    RhBase64.OutputStream bos = null;
    try {
      bos =
          new RhBase64.OutputStream(
              new FileOutputStream(filename),
              RhBase64.ENCODE);
      bos.write(dataToEncode);
      success = true;
    } catch (IOException e) {
      success = false;
    } finally {
      try {
        bos.close();
      } catch (Exception e) {
          // ignore
      }
    }

    return success;
  }

  /**
   * Convenience method for decoding data to a file.
   *
   * @param dataToDecode Base64-encoded data as a string
   * @param filename     Filename for saving decoded data
   * @return <tt>true</tt> if successful, <tt>false</tt> otherwise
   * @since 2.1
   */
  public static boolean decodeToFile(String dataToDecode, String filename) {
    boolean success = false;
    RhBase64.OutputStream bos = null;
    try {
      bos =
          new RhBase64.OutputStream(
              new FileOutputStream(filename),
              RhBase64.DECODE);
      bos.write(dataToDecode.getBytes(PREFERRED_ENCODING));
      success = true;
    } catch (IOException e) {
      success = false;
    } finally {
      try {
        bos.close();
      } catch (Exception e) {
          // ignore
      }
    }

    return success;
  }

  /**
   * Convenience method for reading a base64-encoded file and decoding it.
   *
   * @param filename Filename for reading encoded data
   * @return decoded byte array or null if unsuccessful
   * @since 2.1
   */
  public static byte[] decodeFromFile(String filename) {
    byte[] decodedData = null;
    RhBase64.InputStream bis = null;
    try {
      // Set up some useful variables
      File file = new File(filename);
      byte[] buffer = null;
      int length = 0;
      int numBytes = 0;

      // Check for size of file
      if (file.length() > Integer.MAX_VALUE) {
        System.err.println(
            "File is too big for this convenience method ("
            + file.length() + " bytes).");
        return null;
      }
      buffer = new byte[(int) file.length()];

      // Open a stream
      bis =
          new RhBase64.InputStream(
              new BufferedInputStream(
                  new FileInputStream(file)),
              RhBase64.DECODE);

      // Read until done
      while ((numBytes = bis.read(buffer, length, 4096)) >= 0) {
        length += numBytes;
      }

      // Save in a variable to return
      decodedData = new byte[length];
      System.arraycopy(buffer, 0, decodedData, 0, length);
    } catch (IOException e) {
      System.err.println("Error decoding from file " + filename);
    } finally {
      try {
        bis.close();
      } catch (Exception e) {
          // ignore
      }
    }

    return decodedData;
  }

  /**
   * Convenience method for reading a binary file and base64-encoding it.
   *
   * @param filename Filename for reading binary data
   * @return base64-encoded string or null if unsuccessful
   * @since 2.1
   */
  public static String encodeFromFile(String filename) {
    String encodedData = null;
    RhBase64.InputStream bis = null;
    try {
      // Set up some useful variables
      File file = new File(filename);
      byte[] buffer = new byte[(int) (file.length() * 1.4)];
      int length = 0;
      int numBytes = 0;

      // Open a stream
      bis =
          new RhBase64.InputStream(
              new BufferedInputStream(
                  new FileInputStream(file)),
              RhBase64.ENCODE);

      // Read until done
      while ((numBytes = bis.read(buffer, length, 4096)) >= 0) {
        length += numBytes;
      }

      // Save in a variable to return
      encodedData =
          new String(buffer, 0, length, RhBase64.PREFERRED_ENCODING);
    } catch (IOException e) {
      System.err.println("Error encoding from file " + filename);
    } finally {
      try {
        bis.close();
      } catch (Exception e) {
          // ignore
      }
    }

    return encodedData;
  }

  //~ Inner Classes ----------------------------------------------------------

  // ********  I N N E R   C L A S S   I N P U T S T R E A M  ********

  /**
   * A {@link RhBase64.InputStream} will read data from another <tt>
   * InputStream</tt>, given in the constructor, and encode/decode
   * to/from Base64 notation on the fly.
   *
   * @see RhBase64
   */
  public static class InputStream extends FilterInputStream {
    private boolean encode; // Encoding or decoding
    private int position; // Current position in the buffer
    private byte[] buffer; // Small buffer holding converted data
    private int bufferLength; // Length of buffer (3 or 4)
    private int numSigBytes; // Number of meaningful bytes in the buffer
    private int lineLength;
    private boolean breakLines; // Break lines at less than 80 characters

    /**
     * Constructs a {@link RhBase64.InputStream} in DECODE mode.
     *
     * @param in the <tt>InputStream</tt> from which to read data.
     * @since 1.3
     */
    public InputStream(InputStream in) {
      this(in, DECODE);
    }

    /**
     * Constructs a {@link RhBase64.InputStream} in either ENCODE or DECODE
     * mode.
     *
     * <p>Valid options:
     *
     * <pre>
     *   ENCODE or DECODE: Encode or Decode as data is read.
     *   DONT_BREAK_LINES: don't break lines at 76 characters
     *     (only meaningful when encoding)
     *     <i>Note: Technically, this makes your encoding non-compliant.</i>
     * </pre>
     *
     * <p>Example: <code>new RhBase64.InputStream( in, RhBase64.DECODE
     * )</code>
     *
     * @param in      the <tt>InputStream</tt> from which to read data.
     * @param options Specified options
     * @see RhBase64#ENCODE
     * @see RhBase64#DECODE
     * @see RhBase64#DONT_BREAK_LINES
     * @since 2.0
     */
    public InputStream(java.io.InputStream in, int options) {
      super(in);
      this.breakLines = (options & DONT_BREAK_LINES) != DONT_BREAK_LINES;
      this.encode = (options & ENCODE) == ENCODE;
      this.bufferLength = encode ? 4 : 3;
      this.buffer = new byte[bufferLength];
      this.position = -1;
      this.lineLength = 0;
    }

    /**
     * Reads enough of the input stream to convert to/from Base64 and
     * returns the next byte.
     *
     * @return next byte
     * @since 1.3
     */
    public int read() throws IOException {
      // Do we need to get data?
      if (position < 0) {
        if (encode) {
          byte[] b3 = new byte[3];
          int numBinaryBytes = 0;
          for (int i = 0; i < 3; i++) {
            try {
              int b = in.read();

              // If end of stream, b is -1.
              if (b >= 0) {
                b3[i] = (byte) b;
                numBinaryBytes++;
              }
            } catch (IOException e) {
              // Only a problem if we got no data at all.
              if (i == 0) {
                throw e;
              }
            }
          }

          if (numBinaryBytes > 0) {
            encode3to4(b3, 0, numBinaryBytes, buffer, 0);
            position = 0;
            numSigBytes = 4;
          } else {
            return -1;
          }
        } else {
          // Else decoding
          byte[] b4 = new byte[4];
          int i = 0;
          for (i = 0; i < 4; i++) {
            // Read four "meaningful" bytes:
            int b = 0;
            do {
              b = in.read();
            } while ((b >= 0)
                && (DECODABET[b & 0x7f] <= WHITE_SPACE_ENC));

            if (b < 0) {
              break; // Reads a -1 if end of stream
            }

            b4[i] = (byte) b;
          }

          if (i == 4) {
            numSigBytes = decode4to3(b4, 0, buffer, 0);
            position = 0;
          } else if (i == 0) {
            return -1;
          } else {
            // Must have broken out from above.
            throw new IOException(
                "Improperly padded Base64 input.");
          }
        }
      }

      // Got data?
      if (position >= 0) {
        // End of relevant data?
        if (position >= numSigBytes) {
          return -1;
        }

        if (encode && breakLines && (lineLength >= MAX_LINE_LENGTH)) {
          lineLength = 0;
          return '\n';
        } else {
          // This isn't important when decoding
          // but throwing an extra "if" seems
          // just as wasteful.
          lineLength++;

          int b = buffer[position++];

          if (position >= bufferLength) {
            position = -1;
          }

          return b & 0xFF; // This is how you "cast" a byte that's
          // intended to be unsigned.
        }
      } else {
        // Else error
        // When JDK1.4 is more accepted, use an assertion here.
        throw new IOException(
            "Error in Base64 code reading stream.");
      }
    }

    /**
     * Calls {@link #read()} repeatedly until the end of stream is reached
     * or {@code len} bytes are read. Returns number of bytes read into
     * array or -1 if end of stream is encountered.
     *
     * @param dest array to hold values
     * @param off  offset for array
     * @param len  max number of bytes to read into array
     * @return bytes read into array or -1 if end of stream is encountered.
     * @since 1.3
     */
    public int read(byte[] dest, int off, int len) throws IOException {
      int i;
      int b;
      for (i = 0; i < len; i++) {
        b = read();

        if (b >= 0) {
          dest[off + i] = (byte) b;
        } else if (i == 0) {
          return -1;
        } else {
          break; // Out of 'for' loop
        }
      }
      return i;
    }
  }

  // ********  I N N E R   C L A S S   O U T P U T S T R E A M  ********

  /**
   * A {@link RhBase64.OutputStream} will write data to another <tt>
   * OutputStream</tt>, given in the constructor, and encode/decode
   * to/from Base64 notation on the fly.
   *
   * @see RhBase64
   * @since 1.3
   */
  public static class OutputStream extends FilterOutputStream {
    private boolean encode;
    private int position;
    private byte[] buffer;
    private int bufferLength;
    private int lineLength;
    private boolean breakLines;
    private byte[] b4; // Scratch used in a few places
    private boolean suspendEncoding;

    /**
     * Constructs a {@link RhBase64.OutputStream} in ENCODE mode.
     *
     * @param out the <tt>OutputStream</tt> to which data will be
     *            written.
     * @since 1.3
     */
    public OutputStream(OutputStream out) {
      this(out, ENCODE);
    }

    /**
     * Constructs a {@link RhBase64.OutputStream} in either ENCODE or DECODE
     * mode.
     *
     * <p>Valid options:
     *
     * <pre>
     *   ENCODE or DECODE: Encode or Decode as data is read.
     *   DONT_BREAK_LINES: don't break lines at 76 characters
     *     (only meaningful when encoding)
     *     <i>Note: Technically, this makes your encoding non-compliant.</i>
     * </pre>
     *
     * <p>Example: <code>new RhBase64.OutputStream( out, RhBase64.ENCODE
     * )</code>
     *
     * @param out     the <tt>OutputStream</tt> to which data will be
     *                written.
     * @param options Specified options.
     * @see RhBase64#ENCODE
     * @see RhBase64#DECODE
     * @see RhBase64#DONT_BREAK_LINES
     * @since 1.3
     */
    public OutputStream(java.io.OutputStream out, int options) {
      super(out);
      this.breakLines = (options & DONT_BREAK_LINES) != DONT_BREAK_LINES;
      this.encode = (options & ENCODE) == ENCODE;
      this.bufferLength = encode ? 3 : 4;
      this.buffer = new byte[bufferLength];
      this.position = 0;
      this.lineLength = 0;
      this.suspendEncoding = false;
      this.b4 = new byte[4];
    }

    /**
     * Writes the byte to the output stream after converting to/from Base64
     * notation. When encoding, bytes are buffered three at a time before
     * the output stream actually gets a write() call. When decoding, bytes
     * are buffered four at a time.
     *
     * @param theByte the byte to write
     * @since 1.3
     */
    public void write(int theByte) throws IOException {
      // Encoding suspended?
      if (suspendEncoding) {
        super.out.write(theByte);
        return;
      }

      // Encode?
      if (encode) {
        buffer[position++] = (byte) theByte;
        if (position >= bufferLength) {
          // Enough to encode.
          out.write(encode3to4(b4, buffer, bufferLength));

          lineLength += 4;
          if (breakLines && (lineLength >= MAX_LINE_LENGTH)) {
            out.write(NEW_LINE);
            lineLength = 0;
          }

          position = 0;
        }
      } else {
        // Else, Decoding
        // Meaningful Base64 character?
        if (DECODABET[theByte & 0x7f] > WHITE_SPACE_ENC) {
          buffer[position++] = (byte) theByte;
          if (position >= bufferLength) {
            // Enough to output.
            int len = RhBase64.decode4to3(buffer, 0, b4, 0);
            out.write(b4, 0, len);

            //out.write( RhBase64.decode4to3( buffer ) );
            position = 0;
          }
        } else if (DECODABET[theByte & 0x7f] != WHITE_SPACE_ENC) {
          throw new IOException(
              "Invalid character in Base64 data.");
        }
      }
    }

    /**
     * Calls {@link #write(int)} repeatedly until {@code len} bytes are
     * written.
     *
     * @param theBytes array from which to read bytes
     * @param off      offset for array
     * @param len      max number of bytes to read into array
     * @since 1.3
     */
    public void write(byte[] theBytes, int off, int len) throws IOException {
      // Encoding suspended?
      if (suspendEncoding) {
        super.out.write(theBytes, off, len);
        return;
      }

      for (int i = 0; i < len; i++) {
        write(theBytes[off + i]);
      }
    }

    /**
     * Method added by PHIL. [Thanks, PHIL. -Rob] This pads the buffer
     * without closing the stream.
     */
    public void flushBase64() throws IOException {
      if (position > 0) {
        if (encode) {
          out.write(encode3to4(b4, buffer, position));
          position = 0;
        } else {
          throw new IOException(
              "Base64 input not properly padded.");
        }
      }
    }

    /**
     * Flushes and closes (I think, in the superclass) the stream.
     *
     * @since 1.3
     */
    public void close() throws IOException {
      // 1. Ensure that pending characters are written
      flushBase64();

      // 2. Actually close the stream
      // Base class both flushes and closes.
      super.close();

      buffer = null;
      out = null;
    }

    /**
     * Suspends encoding of the stream. May be helpful if you need to embed
     * a piece of base640-encoded data in a stream.
     *
     * @since 1.5.1
     */
    public void suspendEncoding() throws IOException {
      flushBase64();
      this.suspendEncoding = true;
    }

    /**
     * Resumes encoding of the stream. May be helpful if you need to embed a
     * piece of base640-encoded data in a stream.
     *
     * @since 1.5.1
     */
    public void resumeEncoding() {
      this.suspendEncoding = false;
    }
  }
}

// End RhBase64.java
