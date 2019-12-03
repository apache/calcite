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
package org.apache.calcite.adapter.file;

import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;

import org.jsoup.select.Elements;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;

/**
 * Unit tests for FileReader.
 */
public class FileReaderTest {

  private static final Source CITIES_SOURCE =
      Sources.url("http://en.wikipedia.org/wiki/List_of_United_States_cities_by_population");

  private static final Source STATES_SOURCE =
      Sources.url(
          "http://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States");

  /** Converts a path that is relative to the module into a path that is
   * relative to where the test is running. */
  public static String file(String s) {
    if (new File("file").exists()) {
      return "file/" + s;
    } else {
      return s;
    }
  }

  private static String resourcePath(String path) throws Exception {
    return Sources.of(FileReaderTest.class.getResource("/" + path)).file().getAbsolutePath();
  }

  /** Tests {@link FileReader} URL instantiation - no path. */
  @Test public void testFileReaderUrlNoPath() throws FileReaderException {
    Assume.assumeTrue(FileSuite.hazNetwork());

    // Under OpenJDK, test fails with the following, so skip test:
    //   javax.net.ssl.SSLHandshakeException:
    //   sun.security.validator.ValidatorException: PKIX path building failed:
    //   sun.security.provider.certpath.SunCertPathBuilderException:
    //   unable to find valid certification path to requested target
    final String r = System.getProperty("java.runtime.name");
    // http://openjdk.java.net/jeps/319 => root certificates are bundled with JEP 10
    Assume.assumeTrue("Java 10+ should have root certificates (JEP 319). Runtime is "
            + r + ", Jave major version is " + TestUtil.getJavaMajorVersion(),
        !r.equals("OpenJDK Runtime Environment")
            || TestUtil.getJavaMajorVersion() > 10);

    FileReader t = new FileReader(STATES_SOURCE);
    t.refresh();
  }

  /** Tests {@link FileReader} URL instantiation - with path. */
  @Ignore("[CALCITE-1789] Wikipedia format change breaks file adapter test")
  @Test public void testFileReaderUrlWithPath() throws FileReaderException {
    Assume.assumeTrue(FileSuite.hazNetwork());
    FileReader t =
        new FileReader(CITIES_SOURCE,
            "#mw-content-text > table.wikitable.sortable", 0);
    t.refresh();
  }

  /** Tests {@link FileReader} URL fetch. */
  @Ignore("[CALCITE-1789] Wikipedia format change breaks file adapter test")
  @Test public void testFileReaderUrlFetch() throws FileReaderException {
    Assume.assumeTrue(FileSuite.hazNetwork());
    FileReader t =
        new FileReader(STATES_SOURCE,
            "#mw-content-text > table.wikitable.sortable", 0);
    int i = 0;
    for (Elements row : t) {
      i++;
    }
    assertThat(i, is(51));
  }

  /** Tests failed {@link FileReader} instantiation - malformed URL. */
  @Test public void testFileReaderMalUrl() throws FileReaderException {
    try {
      final Source badSource = Sources.url("bad" + CITIES_SOURCE.url());
      fail("expected exception, got " + badSource);
    } catch (RuntimeException e) {
      assertThat(e.getCause(), instanceOf(MalformedURLException.class));
      assertThat(e.getCause().getMessage(), is("unknown protocol: badhttp"));
    }
  }

  /** Tests failed {@link FileReader} instantiation - bad URL. */
  @Test(expected = FileReaderException.class)
  public void testFileReaderBadUrl() throws FileReaderException {
    final String uri =
        "http://ex.wikipedia.org/wiki/List_of_United_States_cities_by_population";
    FileReader t = new FileReader(Sources.url(uri), "table:eq(4)");
    t.refresh();
  }

  /** Tests failed {@link FileReader} instantiation - bad selector. */
  @Test(expected = FileReaderException.class)
  public void testFileReaderBadSelector() throws FileReaderException {
    final Source source =
        Sources.file(null, file("target/test-classes/tableOK.html"));
    FileReader t = new FileReader(source, "table:eq(1)");
    t.refresh();
  }

  /** Test {@link FileReader} with static file - headings. */
  @Test public void testFileReaderHeadings() throws FileReaderException {
    final Source source =
        Sources.file(null, file("target/test-classes/tableOK.html"));
    FileReader t = new FileReader(source);
    Elements headings = t.getHeadings();
    assertTrue(headings.get(1).text().equals("H1"));
  }

  /** Test {@link FileReader} with static file - data. */
  @Test public void testFileReaderData() throws FileReaderException {
    final Source source =
        Sources.file(null, file("target/test-classes/tableOK.html"));
    FileReader t = new FileReader(source);
    Iterator<Elements> i = t.iterator();
    Elements row = i.next();
    assertTrue(row.get(2).text().equals("R0C2"));
    row = i.next();
    assertTrue(row.get(0).text().equals("R1C0"));
  }

  /** Tests {@link FileReader} with bad static file - headings. */
  @Test public void testFileReaderHeadingsBadFile() throws FileReaderException {
    final Source source =
        Sources.file(null, file("target/test-classes/tableNoTheadTbody.html"));
    FileReader t = new FileReader(source);
    Elements headings = t.getHeadings();
    assertTrue(headings.get(1).text().equals("H1"));
  }

  /** Tests {@link FileReader} with bad static file - data. */
  @Test public void testFileReaderDataBadFile() throws FileReaderException {
    final Source source =
        Sources.file(null, file("target/test-classes/tableNoTheadTbody.html"));
    FileReader t = new FileReader(source);
    Iterator<Elements> i = t.iterator();
    Elements row = i.next();
    assertTrue(row.get(2).text().equals("R0C2"));
    row = i.next();
    assertTrue(row.get(0).text().equals("R1C0"));
  }

  /** Tests {@link FileReader} with no headings static file - data. */
  @Test public void testFileReaderDataNoTh() throws FileReaderException {
    final Source source =
        Sources.file(null, file("target/test-classes/tableNoTH.html"));
    FileReader t = new FileReader(source);
    Iterator<Elements> i = t.iterator();
    Elements row = i.next();
    assertTrue(row.get(2).text().equals("R0C2"));
  }

  /** Tests {@link FileReader} iterator with static file, */
  @Test public void testFileReaderIterator() throws FileReaderException {
    final Source source =
        Sources.file(null, file("target/test-classes/tableOK.html"));
    FileReader t = new FileReader(source);
    Elements row = null;
    for (Elements aT : t) {
      row = aT;
    }
    assertFalse(row == null);
    assertTrue(row.get(1).text().equals("R2C1"));
  }

  /** Tests reading a CSV file via the file adapter. Based on the test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1952">[CALCITE-1952]
   * NPE in planner</a>. */
  @Test public void testCsvFile() throws Exception {
    Properties info = new Properties();
    final String path = resourcePath("sales-csv");
    final String model = "inline:"
        + "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"XXX\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"FILES\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": " + TestUtil.escapeString(path) + "\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    info.put("model", model);
    info.put("lex", "JAVA");

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = connection.createStatement()) {
      final String sql = "select * from FILES.DEPTS";
      final ResultSet rs = stmt.executeQuery(sql);
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("10"));
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("20"));
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("30"));
      assertThat(rs.next(), is(false));
      rs.close();
    }
  }

  /**
   * Tests reading a JSON file via the file adapter.
   */
  @Test public void testJsonFile() throws Exception {
    Properties info = new Properties();
    final String path = resourcePath("sales-json");
    final String model = "inline:"
        + "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"XXX\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"FILES\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": " + TestUtil.escapeString(path) + "\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    info.put("model", model);
    info.put("lex", "JAVA");

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = connection.createStatement()) {
      final String sql = "select * from FILES.DEPTS";
      final ResultSet rs = stmt.executeQuery(sql);
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("10"));
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("20"));
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("30"));
      assertThat(rs.next(), is(false));
      rs.close();
    }
  }

  /**
   * Tests reading two JSON file with join via the file adapter.
   */
  @Test public void testJsonFileWithJoin() throws Exception {
    Properties info = new Properties();
    final String path = resourcePath("sales-json");
    final String model = "inline:"
        + "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"XXX\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"FILES\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": " + TestUtil.escapeString(path) + "\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    info.put("model", model);
    info.put("lex", "JAVA");

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = connection.createStatement()) {
      final String sql = "select a.EMPNO,a.NAME,a.CITY,b.DEPTNO "
          + "from FILES.EMPS a, FILES.DEPTS b where a.DEPTNO = b.DEPTNO";
      final ResultSet rs = stmt.executeQuery(sql);
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("100"));
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("110"));
      assertThat(rs.next(), is(true));
      assertThat(rs.getString(1), is("120"));
      assertThat(rs.next(), is(false));
      rs.close();
    }
  }
}

// End FileReaderTest.java
