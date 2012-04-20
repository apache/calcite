linq4j
======

A port of LINQ (Language-Integrated Query) to Java.

Download
========

    $ git clone git://github.com/julianhyde/linq4j.git linq4j

Build and test
==============

    $ mvn compile
    $ mvn test

Backlog
=======

If you would like to contribute, here are some of the tasks we have planned.
Please let us know if you are starting one.

* Implement and test the methods allowing queries on Enumerables. The methods
  are specified in ExtendedEnumerable, DefaultEnumerable calls the
  implementations in Extensions. We'll do these in tranches. Each time you
  implement a method, add a test similar to Linq4jTest.testWhere.
  Try to refactor out some helper (named inner) classes, rather than creating
  2 or 3 anonymous classes per method.
  First tranche: implement take, takeWhile, skip, skipWhile for Enumerable.

* Second tranche: implement remaining select, selectMany, where methods for
  Enumerable.

* Third tranche: implement groupBy, toMap, toLookup for Enumerable.

* Fourth tranche: implement any, all, aggregate, sum, min, max, average, count,
  longCount for Enumerable.

* Fifth tranche: implement groupJoin and join methods for Enumerable.

* Sixth tranche: implement union, intersect, except, distinct methods for
  Enumerable.

* Seventh tranche: first, last, defaultIfEmpty, elementAtOrDefault,
  firstOrDefault, lastOrDefault for Enumerable. May need to add a class
  parameter so that we can generate the right default value.

* Eight tranche: implement orderBy, reverse for Enumerable.

* Last tranche: all remaining methods for Enumerable.

* Hand-write some examples of calls to Queryable methods, including expression
  trees.

* Parser support. Either modify a Java parser (e.g. OpenJDK), or write a
  pre-processor. Generate Java code that includes expression trees.

* Port Enumerable and Queryable to Scala. Change classes (in particular,
  collections and function types) so that user code is looks like concise,
  native Scala. Share as much of the back-end as possible with linq4j, but
  don't compromise the Scala look-and-feel of the front-end. Use adapters
  (and sacrifice a bit of performance) if it helps.

* Write a simple LINQ-to-SQL provider. This would generate SQL and get data
  from JDBC. It's a prototype, demonstrating that we can connect the dots.
  Plan to throw it away.

* In the prototype LINQ-to-SQL provider, write a simple rule to recognize a
  select list and where clause and push them down to SQL.

* Test Scala front-end against LINQ-to-SQL provider.

* A better provider using a planner framework.

* JDBC driver on top of linq4j (not necessarily on top of the
  Queryable/Expression object model; more likely on the query model that this
  translates to).

* Use planner framework to build back-ends to non-SQL data sources (e.g.
  MongoDB, Hadoop, text files).

More information
================

* License: Apache License, Version 2.0.
* Author: Julian Hyde
* Blog: http://julianhyde.blogspot.com

