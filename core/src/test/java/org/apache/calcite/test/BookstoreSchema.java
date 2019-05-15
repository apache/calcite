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
package org.apache.calcite.test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A Schema representing a bookstore.
 *
 * <p>It contains a single table with various levels/types of nesting,
 * and is used mainly for testing parts of code that rely on nested
 * structures.
 *
 * <p>New authors can be added but attention should be made to update
 * appropriately tests that might fail.
 *
 * <p>The Schema is meant to be used with
 * {@link org.apache.calcite.adapter.java.ReflectiveSchema} thus all
 * fields, and methods, should be public.
 */
public final class BookstoreSchema {

  public final Author[] authors = {
      new Author(1,
          "Victor Hugo",
          new Place(
              new Coordinate(BigDecimal.valueOf(47.24), BigDecimal.valueOf(6.02)),
              "Besançon",
              "France"),
          Collections.singletonList(
              new Book("Les Misérables",
                  1862,
                  Collections.singletonList(new Page(1, "Contents"))))),
      new Author(2,
          "Nikos Kazantzakis",
          new Place(
              new Coordinate(BigDecimal.valueOf(35.3387), BigDecimal.valueOf(25.1442)),
              "Heraklion",
              "Greece"),
          Arrays.asList(
              new Book("Zorba the Greek",
                  1946,
                  Arrays.asList(new Page(1, "Contents"),
                      new Page(2, "Acknowledgements"))),
              new Book("The Last Temptation of Christ",
                  1955,
                  Collections.singletonList(new Page(1, "Contents"))))),
      new Author(3,
          "Homer",
          new Place(null,
              "Ionia",
              "Greece"),
          Collections.emptyList())
  };

  /**
   */
  public static class Author {
    public final int aid;
    public final String name;
    public final Place birthPlace;
    @org.apache.calcite.adapter.java.Array(component = Book.class)
    public final List<Book> books;

    public Author(int aid, String name, Place birthPlace, List<Book> books) {
      this.aid = aid;
      this.name = name;
      this.birthPlace = birthPlace;
      this.books = books;
    }
  }

  /**
   */
  public static class Place {
    public final Coordinate coords;
    public final String city;
    public final String country;

    public Place(Coordinate coords, String city, String country) {
      this.coords = coords;
      this.city = city;
      this.country = country;
    }

  }

  /**
   */
  public static class Coordinate {
    public final BigDecimal latitude;
    public final BigDecimal longtitude;

    public Coordinate(BigDecimal latitude, BigDecimal longtitude) {
      this.latitude = latitude;
      this.longtitude = longtitude;
    }
  }

  /**
   */
  public static class Book {
    public final String title;
    public final int publishYear;
    @org.apache.calcite.adapter.java.Array(component = Page.class)
    public final List<Page> pages;

    public Book(String title, int publishYear, List<Page> pages) {
      this.title = title;
      this.publishYear = publishYear;
      this.pages = pages;
    }
  }

  /**
   */
  public static class Page {
    public final int pageNo;
    public final String contentType;

    public Page(int pageNo, String contentType) {
      this.pageNo = pageNo;
      this.contentType = contentType;
    }
  }
}

// End BookstoreSchema.java
