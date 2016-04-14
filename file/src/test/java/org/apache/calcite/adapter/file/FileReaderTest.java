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

import org.jsoup.select.Elements;

import org.junit.Assume;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

/**
 * Unit tests for FileReader.
 */

public class FileReaderTest {

  static final String CITIES_URI =
      "http://en.wikipedia.org/wiki/List_of_United_States_cities_by_population";

  static final String STATES_URI =
      "http://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States";

  /**
   * Test FileReader URL instantiation - no path
   */
  @Test
  public void testFileReaderURLNoPath() throws FileReaderException {
    Assume.assumeTrue(FileSuite.hazNetwork());
    FileReader t = new FileReader(STATES_URI);
    t.refresh();
  }

  /**
   * Test FileReader URL instantiation - with path
   */
  @Test
  public void testFileReaderURLWithPath() throws FileReaderException {
    Assume.assumeTrue(FileSuite.hazNetwork());
    FileReader t =
        new FileReader(CITIES_URI,
            "#mw-content-text > table.wikitable.sortable", 0);
    t.refresh();
  }

  /**
   * Test FileReader URL fetch
   */
  @Test
  public void testFileReaderURLFetch() throws FileReaderException {
    Assume.assumeTrue(FileSuite.hazNetwork());
    FileReader t =
        new FileReader(STATES_URI,
            "#mw-content-text > table.wikitable.sortable", 0);
    int i = 0;
    for (Elements row : t) {
      i++;
    }
    assertTrue(i == 50);
  }

  /**
   * Test failed FileReader instantiation - malformed URL
   */
  @Test(expected = FileReaderException.class)
  public void testFileReaderMalURL() throws FileReaderException {
    FileReader t = new FileReader("bad" + CITIES_URI, "table:eq(4)");
    t.refresh();
  }

  /**
   * Test failed FileReader instantiation - bad URL
   */
  @Test(expected = FileReaderException.class)
  public void testFileReaderBadURL() throws FileReaderException {
    final String uri =
        "http://ex.wikipedia.org/wiki/List_of_United_States_cities_by_population";
    FileReader t = new FileReader(uri, "table:eq(4)");
    t.refresh();
  }

  /**
   * Test failed FileReader instantiation - bad selector
   */
  @Test(expected = FileReaderException.class)
  public void testFileReaderBadSelector() throws FileReaderException {
    FileReader t =
        new FileReader("file:target/test-classes/tableOK.html", "table:eq(1)");
    t.refresh();
  }

  /**
   * Test FileReader with static file - headings
   */
  @Test
  public void testFileReaderHeadings() throws FileReaderException {
    FileReader t = new FileReader("file:target/test-classes/tableOK.html");
    Elements headings = t.getHeadings();
    assertTrue(headings.get(1).text().equals("H1"));
  }

  /**
   * Test FileReader with static file - data
   */
  @Test
  public void testFileReaderData() throws FileReaderException {
    FileReader t = new FileReader("file:target/test-classes/tableOK.html");
    Iterator<Elements> i = t.iterator();
    Elements row = i.next();
    assertTrue(row.get(2).text().equals("R0C2"));
    row = i.next();
    assertTrue(row.get(0).text().equals("R1C0"));
  }

  /**
   * Test FileReader with bad static file - headings
   */
  @Test
  public void testFileReaderHeadingsBadFile() throws FileReaderException {
    FileReader t =
        new FileReader("file:target/test-classes/tableNoTheadTbody.html");
    Elements headings = t.getHeadings();
    assertTrue(headings.get(1).text().equals("H1"));
  }

  /**
   * Test FileReader with bad static file - data
   */
  @Test
  public void testFileReaderDataBadFile() throws FileReaderException {
    final FileReader t =
        new FileReader("file:target/test-classes/tableNoTheadTbody.html");
    Iterator<Elements> i = t.iterator();
    Elements row = i.next();
    assertTrue(row.get(2).text().equals("R0C2"));
    row = i.next();
    assertTrue(row.get(0).text().equals("R1C0"));
  }

  /**
   * Test FileReader with no headings static file - data
   */
  @Test
  public void testFileReaderDataNoTH() throws FileReaderException {
    FileReader t = new FileReader("file:target/test-classes/tableNoTH.html");
    Iterator<Elements> i =
        new FileReader("file:target/test-classes/tableNoTH.html").iterator();
    Elements row = i.next();
    assertTrue(row.get(2).text().equals("R0C2"));
  }

  /**
   * Test FileReader iterator with static file
   */
  @Test
  public void testFileReaderIterator() throws FileReaderException {
    FileReader t = new FileReader("file:target/test-classes/tableOK.html");
    Elements row = null;
    for (Elements aT : t) {
      row = aT;
    }
    assertFalse(row == null);
    assertTrue(row.get(1).text().equals("R2C1"));
  }

}

// End FileReaderTest.java
