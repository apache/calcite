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
package org.eigenbase.util;

import java.sql.*;
import java.util.logging.*;

/**
 * Utility class for breaking huge strings into smaller &quot;chunks&quot; for
 * applications that require it (such as fitting into transmission buffers).
 * The class contains methods that allow the user to specify the maximum chunk
 * length, or by default uses a maximum chunk length of 32,000 characters.
 */
public abstract class StringChunker {

  private static final int DEFAULT_CHUNK_SIZE = 32000;
  private static final Logger tracer =
      Logger.getLogger("org.eigenbase.util.StringChunker");

  /**
   * Breaks a large string into &quot;chunks&quot; of no more than a specified
   * length for easier transmission or storage. The array of resulting chunks
   * are kept in order so they can be reassembled easily.
   *
   * @param input  String to break into chunks
   * @param maxLen Maximum size (in characters) of a chunk
   * @return String array containing the chunks of the input string, in order
   * If the input string is empty, the return is a single-element array
   * containing an empty string.
   * @throws NullPointerException if the input string is null
   */
  public static String[] slice(String input, int maxLen) {
    if ("".equals(input)) { // handle easy case of empty input string
      return new String[]{input};
    }
    String[] results = new String[countChunks(input.length(), maxLen)];
    int offset = 0;
    int rowNum = 0;
    String temp;
    while (offset < input.length()) {
      temp = input.substring(
          offset,
          Math.min(offset + maxLen, input.length()));
      results[rowNum] = temp;
      offset += maxLen;
      rowNum++;
    }
    return results;
  }

  /**
   * Breaks a large string into &quot;chunks&quot; of up to 32,000 characters
   * for easier transmission or storage. The array of resulting chunks are
   * kept in order so they can be reassembled easily.
   *
   * @param input String to break into chunks
   * @return String array containing the chunks of the input string, in order
   * If the input string is empty, the return is a single-element array
   * containing an empty string.
   * @throws NullPointerException if the input string is null
   */
  public static String[] slice(String input) {
    return slice(input, DEFAULT_CHUNK_SIZE);
  }

  /**
   * Calculates the number of &quot;chunks&quot; created when dividing a
   * corpus of a certain size into chunks of a given size.
   *
   * @param wholeSize int representing the size of the corpus
   * @param chunkSize int representing the maximum size of each chunk
   * @return int representing the total number of chunks
   */
  public static final int countChunks(int wholeSize, int chunkSize) {
    if (wholeSize == 0) {
      wholeSize = 1;
    }
    int result = wholeSize / chunkSize;
    if (wholeSize % chunkSize != 0) {
      result++;
    }
    return result;
  }

  /**
   * Writes a chunked string into a PreparedStatement sink. Each chunk is
   * written with a sequential index in its row, so code reading the chunks
   * can reconstruct the original string by ordering the chunks on read with
   * an ORDER BY clause.<P>
   * The prepared statement must contain two columns: the first being an
   * INTEGER to hold the index value, and the second a VARCHAR of sufficient
   * size to hold the largest chunk value.
   *
   * @param chunks   Array of String objects representing the chunks of a larger
   *                 original String
   * @param ps       PreparedStatement that will write each indexed chunk
   * @param startIdx int representing the initial chunk index, which will be
   *                 incremented for subsequent chunks
   * @return the number of chunks written
   * @throws SQLException if the input data does not match the prepared
   *                      statement or if the statement execution fails
   */
  public static int writeChunks(
      String[] chunks,
      PreparedStatement ps,
      int startIdx)
      throws SQLException {
    int idx = startIdx;
    for (String chunk : chunks) {
      ps.setInt(1, idx);
      ps.setString(2, chunk);
      ps.executeUpdate();
      idx++;
    }
    return (idx - startIdx);
  }

  /**
   * Writes a chunked string into a PreparedStatement sink. Each chunk is
   * written with a sequential index in its row, so code reading the chunks
   * can reconstruct the original string by ordering the chunks on read with
   * an ORDER BY clause. The chunk index starts at zero.<P>
   * The prepared statement must contain two columns: the first being an
   * INTEGER to hold the index value, and the second a VARCHAR of sufficient
   * size to hold the largest chunk value.
   *
   * @param chunks Array of String objects representing the chunks of a larger
   *               original String
   * @param ps     PreparedStatement that will write each indexed chunk
   * @return the number of chunks written
   * @throws SQLException if the input data does not match the prepared
   *                      statement or if the statement execution fails
   */
  public static int writeChunks(String[] chunks, PreparedStatement ps)
      throws SQLException {
    return writeChunks(chunks, ps, 0);
  }

  /**
   * Reads a set of string &quot;chunks&quot; from a ResultSet and
   * concatenates them in the order read to produce a large, single string.
   * One column from each row is designated as the one containing the string
   * chunks.
   * This is the complement to the <code>writeChunks()</code> methods.
   *
   * @param rs          ResultSet to read chunks from
   * @param columnIndex index indicating which column in the result set to
   *                    use as the chunk for each row
   * @return String containing all concatenated chunks
   * @throws SQLException if there is a database access problem or if the
   *                      designated column is not a string
   */
  public static String readChunks(ResultSet rs, int columnIndex)
      throws SQLException {
    int chunkCount = 0;
    StringBuilder sb = new StringBuilder(DEFAULT_CHUNK_SIZE);
    String chunk = null;
    while (rs.next()) {
      chunk = rs.getString(columnIndex);
      sb.append(chunk);
      chunkCount++;
    }
    return sb.toString();
  }

  /**
   * Reads a set of strings &quot;chunks&quot; from a ResultSet and
   * concatenates them in the order read to produce a large, single string.
   * The chunks are taken from the first column of each row in the result
   * set.
   *
   * @param rs ResultSet to read chunks from
   * @return String containing all concatenated chunks
   * @throws SQLException if there is a database access problem or if the
   *                      designated column is not a string
   */
  public static String readChunks(ResultSet rs) throws SQLException {
    return readChunks(rs, 1);
  }
}

// End StringChunker.java
