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
package org.apache.calcite.sql.test;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlOverlapsOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.apache.calcite.test.DiffTestCase;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Various automated checks on the documentation. */
public class DocumentationTest {
  /** Generates a copy of {@code reference.md} with the current set of key
   * words. Fails if the copy is different from the original. */
  @Test public void testGenerateKeyWords() throws IOException {
    final FileFixture f = new FileFixture();
    f.outFile.getParentFile().mkdirs();
    try (BufferedReader r = Util.reader(f.inFile);
         FileOutputStream fos = new FileOutputStream(f.outFile);
         PrintWriter w = Util.printWriter(f.outFile)) {
      String line;
      int stage = 0;
      while ((line = r.readLine()) != null) {
        if (line.equals("{% comment %} end {% endcomment %}")) {
          ++stage;
        }
        if (stage != 1) {
          w.println(line);
        }
        if (line.equals("{% comment %} start {% endcomment %}")) {
          ++stage;
          SqlAbstractParserImpl.Metadata metadata =
              new SqlParserTest().getSqlParser("").getMetadata();
          int z = 0;
          for (String s : metadata.getTokens()) {
            if (z++ > 0) {
              w.println(",");
            }
            if (metadata.isKeyword(s)) {
              w.print(metadata.isReservedWord(s) ? ("**" + s + "**") : s);
            }
          }
          w.println(".");
        }
      }
      w.flush();
      fos.flush();
      fos.getFD().sync();
    }
    String diff = DiffTestCase.diff(f.outFile, f.inFile);
    if (!diff.isEmpty()) {
      throw new AssertionError("Mismatch between " + f.outFile
          + " and " + f.inFile + ":\n"
          + diff);
    }
  }

  /** Tests that every function in {@link SqlStdOperatorTable} is documented in
   * reference.md. */
  @Test public void testAllFunctionsAreDocumented() throws IOException {
    final FileFixture f = new FileFixture();
    final Map<String, PatternOp> map = new TreeMap<>();
    addOperators(map, "", SqlStdOperatorTable.instance().getOperatorList());
    for (SqlLibrary library : SqlLibrary.values()) {
      switch (library) {
      case STANDARD:
      case SPATIAL:
        continue;
      }
      addOperators(map, "\\| [^|]*" + library.abbrev + "[^|]* ",
          SqlLibraryOperatorTableFactory.INSTANCE
              .getOperatorTable(EnumSet.of(library)).getOperatorList());
    }
    final Set<String> regexSeen = new HashSet<>();
    try (LineNumberReader r = new LineNumberReader(Util.reader(f.inFile))) {
      for (;;) {
        final String line = r.readLine();
        if (line == null) {
          break;
        }
        for (Map.Entry<String, PatternOp> entry : map.entrySet()) {
          if (entry.getValue().pattern.matcher(line).matches()) {
            regexSeen.add(entry.getKey()); // function is documented
          }
        }
      }
    }
    final Set<String> regexNotSeen = new TreeSet<>(map.keySet());
    regexNotSeen.removeAll(regexSeen);
    assertThat("some functions are not documented: " + map.entrySet().stream()
            .filter(e -> regexNotSeen.contains(e.getKey()))
            .map(e -> e.getValue().opName + "(" + e.getKey() + ")")
            .collect(Collectors.joining(", ")),
        regexNotSeen.isEmpty(), is(true));
  }

  private void addOperators(Map<String, PatternOp> map, String prefix,
      List<SqlOperator> operatorList) {
    for (SqlOperator op : operatorList) {
      final String name = op.getName().equals("TRANSLATE3") ? "TRANSLATE"
          : op.getName();
      if (op instanceof SqlSpecialOperator
          || !name.matches("^[a-zA-Z][a-zA-Z0-9_]*$")) {
        continue;
      }
      final String regex;
      if (op instanceof SqlOverlapsOperator) {
        regex = "[ ]*<td>period1 " + name + " period2</td>";
      } else if (op instanceof SqlFunction
          && (op.getOperandTypeChecker() == null
              || op.getOperandTypeChecker().getOperandCountRange().getMin()
                  != 0)) {
        regex = prefix + "\\| .*" + name + "\\(.*";
      } else {
        regex = prefix + "\\| .*" + name + ".*";
      }
      map.put(regex, new PatternOp(Pattern.compile(regex), name));
    }
  }

  /** A compiled regex and an operator name. An item to be found in the
   * documentation. */
  private static class PatternOp {
    final Pattern pattern;
    final String opName;

    private PatternOp(Pattern pattern, String opName) {
      this.pattern = pattern;
      this.opName = opName;
    }
  }

  /** Defines paths needed by a couple of tests. */
  private static class FileFixture {
    final File base;
    final File inFile;
    final File outFile;

    FileFixture() {
      // inUrl =
      // "file:/home/x/calcite/core/target/test-classes/hsqldb-model.json"
      String path = "hsqldb-model.json";
      File hsqlDbModel =
          Sources.of(SqlParserTest.class.getResource("/" + path)).file();
      assert hsqlDbModel.getAbsolutePath().endsWith(
          Paths.get("core", "target", "test-classes", "hsqldb-model.json")
              .toString())
          : hsqlDbModel.getAbsolutePath()
          + " should end with core/target/test-classes/hsqldb-model.json";
      // skip hsqldb-model.json, test-classes, target, core
      // The assertion above protects us from walking over unrelated paths
      base = hsqlDbModel.getAbsoluteFile()
          .getParentFile().getParentFile().getParentFile().getParentFile();
      inFile = new File(base, "site/_docs/reference.md");
      outFile = new File(base, "core/target/surefire/reference.md");
    }
  }
}

// End DocumentationTest.java
