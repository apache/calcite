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
package org.apache.calcite.adapter.os;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Table function that executes the OS "find" command to find files under a
 * particular path.
 */
public class FilesTableFunction {

  private static final BigDecimal THOUSAND = BigDecimal.valueOf(1000L);

  private FilesTableFunction() {
  }

  /**
   * Evaluates the function.
   *
   * @param path Directory in which to start the search. Typically '.'
   * @return Table that can be inspected, planned, and evaluated
   */
  public static ScannableTable eval(final String path) {
    return new AbstractBaseScannableTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("access_time", SqlTypeName.TIMESTAMP) // %A@ sec since epoch
            .add("block_count", SqlTypeName.INTEGER) // %b in 512B blocks
            .add("change_time", SqlTypeName.TIMESTAMP) // %C@ sec since epoch
            .add("depth", SqlTypeName.INTEGER) // %d depth in directory tree
            .add("device", SqlTypeName.INTEGER) // %D device number
            .add("file_name", SqlTypeName.VARCHAR) // %f file name, sans dirs
            .add("fstype", SqlTypeName.VARCHAR) // %F file system type
            .add("gname", SqlTypeName.VARCHAR) // %g group name
            .add("gid", SqlTypeName.INTEGER) // %G numeric group id
            .add("dir_name", SqlTypeName.VARCHAR) // %h leading dirs
            .add("inode", SqlTypeName.BIGINT) // %i inode number
            .add("link", SqlTypeName.VARCHAR) // %l object of sym link
            .add("perm", SqlTypeName.CHAR, 4) // %#m permission octal
            .add("hard", SqlTypeName.INTEGER) // %n number of hard links
            .add("path", SqlTypeName.VARCHAR) // %P file's name
            .add("size", SqlTypeName.BIGINT) // %s file's size in bytes
            .add("mod_time", SqlTypeName.TIMESTAMP) // %T@ seconds since epoch
            .add("user", SqlTypeName.VARCHAR) // %u user name
            .add("uid", SqlTypeName.INTEGER) // %U numeric user id
            .add("type", SqlTypeName.CHAR, 1) // %Y file type
            .build();

        // Fields in Linux find that are currently ignored:
        // %y file type (not following sym links)
        // %k block count in 1KB blocks
        // %p file name (including argument)
      }

      private Enumerable<String> sourceLinux() {
        final String[] args = {
            "find", path, "-printf", ""
              + "%A@\\0" // access_time
              + "%b\\0" // block_count
              + "%C@\\0" // change_time
              + "%d\\0" // depth
              + "%D\\0" // device
              + "%f\\0" // file_name
              + "%F\\0" // fstype
              + "%g\\0" // gname
              + "%G\\0" // gid
              + "%h\\0" // dir_name
              + "%i\\0" // inode
              + "%l\\0" // link
              + "%#m\\0" // perm
              + "%n\\0" // hard
              + "%P\\0" // path
              + "%s\\0" // size
              + "%T@\\0" // mod_time
              + "%u\\0" // user
              + "%U\\0" // uid
              + "%Y\\0" // type
        };
        return Processes.processLines('\0', args);
      }

      private Enumerable<String> sourceMacOs() {
        if (path.contains("'")) {
          // no injection monkey business
          throw new IllegalArgumentException();
        }
        final String[] args = {"/bin/sh", "-c", "find '" + path
              + "' | xargs stat -f "
              + "%a%n" // access_time
              + "%b%n" // block_count
              + "%c%n" // change_time
              + "0%n" // depth: not supported by macOS stat
              + "%Hd%n" // device: we only use the high part of "H,L" device
              + "filename%n" // filename: not supported by macOS stat
              + "fstype%n" // fstype: not supported by macOS stat
              + "%Sg%n" // gname
              + "%g%n" // gid
              + "dir_name%n" // dir_name: not supported by macOS stat
              + "%i%n" // inode
              + "%Y%n" // link
              + "%Lp%n" // perm
              + "%l%n" // hard
              + "%SN%n" // path
              + "%z%n" // size
              + "%m%n" // mod_time
              + "%Su%n" // user
              + "%u%n" // uid
              + "%LT%n" // type
        };
        return Processes.processLines('\n', args);
      }

      @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
        JavaTypeFactory typeFactory = root.getTypeFactory();
        final RelDataType rowType = getRowType(typeFactory);
        final List<String> fieldNames =
            ImmutableList.copyOf(rowType.getFieldNames());
        final String osName = System.getProperty("os.name");
        final String osVersion = System.getProperty("os.version");
        Util.discard(osVersion);
        final Enumerable<String> enumerable;
        switch (osName) {
        case "Mac OS X": // tested on version 10.12.5
          enumerable = sourceMacOs();
          break;
        default:
          enumerable = sourceLinux();
        }
        return new AbstractEnumerable<@Nullable Object[]>() {
          @Override public Enumerator<@Nullable Object[]> enumerator() {
            final Enumerator<String> e = enumerable.enumerator();
            return new Enumerator<@Nullable Object[]>() {
              @Nullable Object @Nullable [] current;

              @Override public Object[] current() {
                return requireNonNull(current, "current");
              }

              @Override public boolean moveNext() {
                current = new Object[fieldNames.size()];
                for (int i = 0; i < current.length; i++) {
                  if (!e.moveNext()) {
                    return false;
                  }
                  final String v = e.current();
                  try {
                    current[i] = field(fieldNames.get(i), v);
                  } catch (RuntimeException e) {
                    throw new RuntimeException("while parsing value ["
                        + v + "] of field [" + fieldNames.get(i)
                        + "] in line [" + Arrays.toString(current) + "]", e);
                  }
                }
                switch (osName) {
                case "Mac OS X":
                  // Strip leading "./"
                  String path = requireNonNull((String) current[14]);
                  if (".".equals(path)) {
                    current[14] = path = "";
                    current[3] = 0; // depth
                  } else if (path.startsWith("./")) {
                    current[14] = path = path.substring(2);
                    current[3] = count(path, '/') + 1; // depth
                  } else {
                    current[3] = count(path, '/'); // depth
                  }
                  final int slash = path.lastIndexOf('/');
                  if (slash >= 0) {
                    current[5] = path.substring(slash + 1); // filename
                    current[9] = path.substring(0, slash); // dir_name
                  } else {
                    current[5] = path; // filename
                    current[9] = ""; // dir_name
                  }

                  // Make type values more like those on Linux
                  final String type = (String) current[19];
                  current[19] = "/".equals(type) ? "d"
                      : "".equals(type) || "*".equals(type) ? "f"
                      : "@".equals(type) ? "l"
                      : type;
                  break;
                default:
                  break;
                }
                return true;
              }

              private int count(String s, char c) {
                int n = 0;
                for (int i = 0, len = s.length(); i < len; i++) {
                  if (s.charAt(i) == c) {
                    ++n;
                  }
                }
                return n;
              }

              @Override public void reset() {
                throw new UnsupportedOperationException();
              }

              @Override public void close() {
                e.close();
              }

              private Object field(String field, String value) {
                switch (field) {
                case "block_count":
                case "depth":
                case "device":
                case "gid":
                case "uid":
                case "hard":
                  return Integer.valueOf(value);
                case "inode":
                case "size":
                  return Long.valueOf(value);
                case "access_time":
                case "change_time":
                case "mod_time":
                  return new BigDecimal(value).multiply(THOUSAND).longValue();
                default:
                  return value;
                }
              }
            };
          }
        };
      }
    };
  }
}
