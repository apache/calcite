<!--
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
-->
# Lattice

A lattice is a framework for creating and populating materialized views,
and for recognizing that a materialized view can be used to solve a
particular query.

A lattice represents a star (or snowflake) schema, not a general
schema. In particular, all relationships must be many-to-one, heading
from a fact table at the center of the star.

The name derives from the mathematics: a
<a href="http://en.wikipedia.org/wiki/Lattice_(order)">lattice</a>
is a
<a href="http://en.wikipedia.org/wiki/Partially_ordered_set">partially
ordered set</a> where any two elements have a unique greatest lower
bound and least upper bound.

[<a href="#user-content-ref-hru96">HRU96</a>] observed that the set of possible
materializations of a data cube forms a lattice, and presented an
algorithm to choose a good set of materializations. Calcite's
recommendation algorithm is derived from this.

The lattice definition uses a SQL statement to represent the star. SQL
is a useful short-hand to represent several tables joined together,
and assigning aliases to the column names (it more convenient than
inventing a new language to represent relationships, join conditions
and cardinalities).

Unlike regular SQL, order is important. If you put A before B in the
FROM clause, and make a join between A and B, you are saying that
there is a many-to-one foreign key relationship from A to B. (E.g. in
the example lattice, the Sales fact table occurs before the Time
dimension table, and before the Product dimension table. The Product
dimension table occurs before the ProductClass outer dimension table,
further down an arm of a snowflake.)

A lattice implies constraints. In the A to B relationship, there is a
foreign key on A (i.e. every value of A's foreign key has a
corresponding value in B's key), and a unique key on B (i.e. no key
value occurs more than once). These constraints are really important,
because it allows the planner to remove joins to tables whose columns
are not being used, and know that the query results will not change.

Calcite does not check these constraints. If they are violated,
Calcite will return wrong results.

A lattice is a big, virtual join view. It is not materialized (it
would be several times larger than the star schema, because of
denormalization) and you probably wouldn't want to query it (far too
many columns). So what is it useful for? As we said above, (a) the
lattice declares some very useful primary and foreign key constraints,
(b) it helps the query planner map user queries onto
filter-join-aggregate materialized views (the most useful kind of
materialized view for DW queries), (c) gives Calcite a framework
within which to gather stats about data volumes and user queries, (d)
allows Calcite to automatically design and populate materialized
views.

Most star schema models force you to choose whether a column is a
dimension or a measure. In a lattice, every column is a dimension
column. (That is, it can become one of the columns in the GROUP BY clause
to query the star schema at a particular dimensionality). Any column
can also be used in a measure; you define measures by giving the
column and an aggregate function.

If "unit_sales" tends to be used much more often as a measure rather
than a dimension, that's fine. Calcite's algorithm should notice that
it is rarely aggregated, and not be inclined to create tiles that
aggregate on it. (By "should" I mean "could and one day will". The
algorithm does not currently take query history into account when
designing tiles.)

But someone might want to know whether orders with fewer than 5 items
were more or less profitable than orders with more than 100. All of a
sudden, "unit_sales" has become a dimension. If there's virtually zero
cost to declaring a column a dimension column, I figured let's make
them all dimension columns.

The model allows for a particular table to be used more than once,
with a different table alias. You could use this to model say
OrderDate and ShipDate, with two uses to the Time dimension table.

Most SQL systems require that the column names in a view are unique.
This is hard to achieve in a lattice, because you often include
primary and foreign key columns in a join. So Calcite lets you refer
to columns in two ways. If the column is unique, you can use its name,
["unit_sales"]. Whether or not it is unique in the lattice, it will be
unique in its table, so you can use it qualified by its table alias.
Examples:
* ["sales", "unit_sales"]
* ["ship_date", "time_id"]
* ["order_date", "time_id"]

A "tile" is a materialized table in a lattice, with a particular
dimensionality. (What Kylin calls a "cuboid".) The "tiles" attribute
of the <a href="model.json#lattice">lattice JSON element</a>
defines an initial set of tiles to materialize.

If you run the algorithm, you can omit the tiles attribute. Calcite
will choose an initial set. If you include the tiles attribute, the
algorithm will start with that list and then start finding other tiles
that are complementary (i.e. "fill in the gaps" left by the initial
tiles).

### References

* <a name="ref-hru96">[HRU96]</a> V. Harinarayan, A. Rajaraman and J. Ullman.
  <a href="http://web.eecs.umich.edu/~jag/eecs584/papers/implementing_data_cube.pdf">Implementing
  data cubes efficiently</a>. In _Proc. ACM SIGMOD Conf._, Montreal, 1996.
