---
layout: docs
title: Spatial
permalink: /docs/spatial.html
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

Calcite is [aiming](https://issues.apache.org/jira/browse/CALCITE-1968) to implement
OpenGIS Simple Features Implementation Specification for SQL,
[version 1.2.1](https://www.opengeospatial.org/standards/sfs),
a standard implemented by spatial databases such as
[PostGIS](https://postgis.net/)
and [H2GIS](https://www.h2gis.org/).

We also aim to add optimizer support for
[spatial indexes](https://issues.apache.org/jira/browse/CALCITE-1861)
and other forms of query optimization.

* TOC
{:toc}

## Introduction

A spatial database is a database that is optimized for storing and query data
that represents objects defined in a geometric space.

Calcite's support for spatial data includes:

* A [GEOMETRY](reference.html#data-types) data type and
  [sub-types](reference.html#spatial-types) including `POINT`, `LINESTRING`
  and `POLYGON`
* [Spatial functions](reference.html#spatial-functions) (prefixed `ST_`;
  we have implemented about 35 of the 150 in the OpenGIS specification)

and will at some point also include query rewrites to use spatial indexes.

## Enabling spatial support

Though the `GEOMETRY` data type is built-in, the functions are not enabled by
default. You need to add `fun=spatial` to the JDBC connect string to enable
the functions. For example, `sqlline`:

{% highlight sql %}
$ ./sqlline
> !connect jdbc:calcite:fun=spatial "sa" ""
SELECT ST_PointFromText('POINT(-71.064544 42.28787)');
+-------------------------------+
| EXPR$0                        |
+-------------------------------+
| {"x":-71.064544,"y":42.28787} |
+-------------------------------+
1 row selected (0.323 seconds)
{% endhighlight %}

## Query rewrites

One class of rewrites uses
[Hilbert space-filling curves](https://en.wikipedia.org/wiki/Hilbert_curve).
Suppose that a table
has columns `x` and `y` denoting the position of a point and also a column `h`
denoting the distance of that point along a curve. Then a predicate involving
distance of (x, y) from a fixed point can be translated into a predicate
involving ranges of h.

Suppose we have a table with the locations of restaurants:

{% highlight sql %}
CREATE TABLE Restaurants (
  INT id NOT NULL PRIMARY KEY,
  VARCHAR(30) name,
  VARCHAR(20) cuisine,
  INT x NOT NULL,
  INT y NOT NULL,
  INT h  NOT NULL DERIVED (ST_Hilbert(x, y)))
SORT KEY (h);
{% endhighlight %}

The optimizer requires that `h` is the position on the Hilbert curve of
point (`x`, `y`), and also requires that the table is sorted on `h`.
The `DERIVED` and `SORT KEY` clauses in the DDL syntax are invented for the
purposes of this example, but a clustered table with a `CHECK` constraint
would work just as well.

The query

{% highlight sql %}
SELECT *
FROM Restaurants
WHERE ST_DWithin(ST_Point(x, y), ST_Point(10.0, 20.0), 6)
{% endhighlight %}

can be rewritten to

{% highlight sql %}
SELECT *
FROM Restaurants
WHERE (h BETWEEN 36496 AND 36520
    OR h BETWEEN 36456 AND 36464
    OR h BETWEEN 33252 AND 33254
    OR h BETWEEN 33236 AND 33244
    OR h BETWEEN 33164 AND 33176
    OR h BETWEEN 33092 AND 33100
    OR h BETWEEN 33055 AND 33080
    OR h BETWEEN 33050 AND 33053
    OR h BETWEEN 33033 AND 33035)
AND ST_DWithin(ST_Point(x, y), ST_Point(10.0, 20.0), 6)
{% endhighlight %}

The rewritten query contains a collection of ranges on `h` followed by the
original `ST_DWithin` predicate. The range predicates are evaluated first and
are very fast because the table is sorted on `h`.

Here is the full set of transformations:

| Description | Expression
|:----------- |: ------
| Test whether a constant rectangle (X, X2, Y, Y2) contains a point (a, b)<br/><br/>Rewrite to use Hilbert index | ST_Contains(&#8203;ST_Rectangle(&#8203;X, X2, Y, Y2), ST_Point(a, b)))<br/><br/>h BETWEEN C1 AND C2<br/>OR ...<br/>OR h BETWEEN C<sub>2k</sub> AND C<sub>2k+1</sub>
| Test whether a constant geometry G contains a point (a, b)<br/><br/>Rewrite to use bounding box of constant geometry, which is also constant, then rewrite to Hilbert range(s) as above | ST_Contains(&#8203;ST_Envelope(&#8203;G), ST_Point(a, b))<br/><br/>ST_Contains(&#8203;ST_Rectangle(&#8203;X, X2, Y, Y2), ST_Point(a, b)))
| Test whether a point (a, b) is within a buffer around a constant point (X, Y)<br/><br/>Special case of previous, because buffer is a constant geometry | ST_Contains(&#8203;ST_Buffer(&#8203;ST_Point(a, b), D), ST_Point(X, Y))
| Test whether a point (a, b) is within a constant distance D of a constant point (X, Y)<br/><br/>First, convert to buffer, then use previous rewrite for constant geometry | ST_DWithin(&#8203;ST_Point(a, b), ST_Point(X, Y), D))<br/><br/>ST_Contains(&#8203;ST_Buffer(&#8203;ST_Point(&#8203;X, Y), D), ST_Point(a, b))
| Test whether a constant point (X, Y) is within a constant distance D of a point (a, b)<br/><br/>Reverse arguments of call to <code>ST_DWithin</code>, then use previous rewrite | ST_DWithin(&#8203;ST_Point(X, Y), ST_Point(a, b), D))<br/><br/>ST_Contains(&#8203;ST_Buffer(&#8203;ST_Point(&#8203;X, Y), D), ST_Point(a, b))

In the above, `a` and `b` are variables, `X`, `X2`, `Y`, `Y2`, `D` and `G` are
constants.

Many rewrites are inexact: there are some points where the predicate would
return false but the rewritten predicate returns true.
For example, a rewrite might convert a test whether a point is in a circle to a
test for whether the point is in the circle's bounding square.
These rewrites are worth performing because they are much quicker to apply,
and often allow range scans on the Hilbert index.
But for safety, Calcite applies the original predicate, to remove false positives.

## Acknowledgements

Calcite's OpenGIS implementation uses the
[JTS Topology Suite](https://github.com/locationtech/jts). Thanks for the
help we received from their community.

While developing this feature, we made extensive use of the
PostGIS documentation and tests,
and the H2GIS documentation, and consulted both as reference implementations
when the specification wasn't clear. Thank you to these awesome projects.
