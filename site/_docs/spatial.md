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
[version 1.2.1](http://www.opengeospatial.org/standards/sfs),
a standard implemented by spatial databases such as
[PostGIS](https://postgis.net/)
and [H2GIS](http://www.h2gis.org/).

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

## Acknowledgements

Calcite's OpenGIS implementation uses the
[Esri geometry API](https://github.com/Esri/geometry-api-java). Thanks for the
help we received from their community.

While developing this feature, we made extensive use of the
PostGIS documentation and tests,
and the H2GIS documentation, and consulted both as reference implementations
when the specification wasn't clear. Thank you to these awesome projects.
