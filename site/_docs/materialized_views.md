---
layout: docs
title: Materialized Views
permalink: /docs/materialized_views.html
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

There are several different ways to exploit materialized views in Calcite.

* TOC
{:toc}

## Materialized views maintained by Calcite

For details, see the [lattices documentation]({{ site.baseurl }}/docs/lattice.html).

## Expose materialized views to Calcite

Some Calcite adapters as well as projects that rely on Calcite have their own notion of materialized views.

For example, Apache Cassandra allows the user to define materialized views based on existing tables which are automatically maintained.
The Cassandra adapter automatically exposes these materialized views to Calcite.

Another example is Apache Hive. When a materialized view is created in Hive, the user can specify whether the view may be used in query optimization. If the user chooses to do so, the materialized view will be registered with Calcite.

By registering materialized views in Calcite, the optimizer has the opportunity to automatically rewrite queries to use these views.

### View-based query rewriting

View-based query rewriting aims to take an input query which can be answered using a preexisting view and rewrite the query to make use of the view.
Currently Calcite has two implementations of view-based query rewriting.

#### Substitution via rules transformation

The first approach is based on view substitution.
`SubstitutionVisitor` and its extension `MaterializedViewSubstitutionVisitor` aim to substitute part of the relational algebra tree with an equivalent expression which makes use of a materialized view. The scan over the materialized view and the materialized view definition plan are registered with the planner. Afterwards, transformation rules that try to unify expressions in the plan are triggered. Expressions do not need to be equivalent to be replaced: the visitor might add a residual predicate on top of the expression if needed.

The following example is taken from the documentation of `SubstitutionVisitor`:

 * Query: `SELECT a, c FROM t WHERE x = 5 AND b = 4`
 * Target (materialized view definition): `SELECT a, b, c FROM t WHERE x = 5`
 * Result: `SELECT a, c FROM mv WHERE b = 4`

Note that `result` uses the materialized view table `mv` and a simplified condition `b = 4`.

While this approach can accomplish a large number of rewritings, it has some limitations. Since the rule relies on transformation rules to create the equivalence between expressions in the query and the materialized view, it might need to enumerate exhaustively all possible equivalent rewritings for a given expression to find a materialized view substitution. However, this is not scalable in the presence of complex
views, e.g., views with an arbitrary number of join operators.

#### Rewriting using plan structural information

In turn, an alternative rule that attempts to match queries to views by extracting some structural information about the expression to replace has been proposed.

`AbstractMaterializedViewRule` builds on the ideas presented in [<a href="#ref-gl01">GL01</a>] and introduces some additional extensions.
The rule can rewrite expressions containing arbitrary chains of Join, Filter, and Project operators.
Additionally, the rule can rewrite expressions rooted at an Aggregate operator, rolling aggregations up if necessary. In turn, it can also produce rewritings using Union operators if the query can be partially answered from a view.

To produce a larger number of rewritings, the rule relies on information exposed as constraints defined over the database tables, e.g., *foreign keys*, *primary keys*, *unique keys* or *not null*.

##### Rewriting coverage

Let us illustrate with some examples the coverage of the view rewriting algorithm implemented in `AbstractMaterializedViewRule`. The examples are based on the following database schema.

```
CREATE TABLE depts(
  deptno INT NOT NULL,
  deptname VARCHAR(20),
  PRIMARY KEY (deptno)
);
CREATE TABLE locations(
  locationid INT NOT NULL,
  state CHAR(2),
  PRIMARY KEY (locationid)
);
CREATE TABLE emps(
  empid INT NOT NULL,
  deptno INT NOT NULL,
  locationid INT NOT NULL,
  empname VARCHAR(20) NOT NULL,
  salary DECIMAL (18, 2),
  PRIMARY KEY (empid),
  FOREIGN KEY (deptno) REFERENCES depts(deptno),
  FOREIGN KEY (locationid) REFERENCES locations(locationid)
);
```

###### Join rewriting

The rewriting can handle different join orders in the query and the view definition. In addition, the rule tries to detect when a compensation predicate could be used to produce a rewriting using a view.

* Query:

```
SELECT empid
FROM depts
JOIN (
  SELECT empid, deptno
  FROM emps
  WHERE empid = 1) AS subq
ON depts.deptno = subq.deptno
```

* Materialized view definition:

```
SELECT empid
FROM emps
JOIN depts USING (deptno)
```

* Rewriting:

```
SELECT empid
FROM mv
WHERE empid = 1
```


###### Aggregate rewriting

* Query:

```
SELECT deptno
FROM emps
WHERE deptno > 10
GROUP BY deptno
```

* Materialized view definition:

```
SELECT empid, deptno
FROM emps
WHERE deptno > 5
GROUP BY empid, deptno
```

* Rewriting:

```
SELECT deptno
FROM mv
WHERE deptno > 10
GROUP BY deptno
```


###### Aggregate rewriting (with aggregation rollup)

* Query:

```
SELECT deptno, COUNT(*) AS c, SUM(salary) AS s
FROM emps
GROUP BY deptno
```

* Materialized view definition:

```
SELECT empid, deptno, COUNT(*) AS c, SUM(salary) AS s
FROM emps
GROUP BY empid, deptno
```

* Rewriting:

```
SELECT deptno, SUM(c), SUM(s)
FROM mv
GROUP BY deptno
```


###### Query partial rewriting

Through the declared constraints, the rule can detect joins that only append columns without altering the tuples multiplicity and produce correct rewritings.

* Query:

```
SELECT deptno, COUNT(*)
FROM emps
GROUP BY deptno
```

* Materialized view definition:

```
SELECT empid, depts.deptno, COUNT(*) AS c, SUM(salary) AS s
FROM emps
JOIN depts USING (deptno)
GROUP BY empid, depts.deptno
```

* Rewriting:

```
SELECT deptno, SUM(c)
FROM mv
GROUP BY deptno
```


###### View partial rewriting

* Query:

```
SELECT deptname, state, SUM(salary) AS s
FROM emps
JOIN depts ON emps.deptno = depts.deptno
JOIN locations ON emps.locationid = locations.locationid
GROUP BY deptname, state
```

* Materialized view definition:

```
SELECT empid, deptno, state, SUM(salary) AS s
FROM emps
JOIN locations ON emps.locationid = locations.locationid
GROUP BY empid, deptno, state
```

* Rewriting:

```
SELECT deptname, state, SUM(s)
FROM mv
JOIN depts ON mv.deptno = depts.deptno
GROUP BY deptname, state
```


###### Union rewriting

* Query:

```
SELECT empid, deptname
FROM emps
JOIN depts ON emps.deptno = depts.deptno
WHERE salary > 10000
```

* Materialized view definition:

```
SELECT empid, deptname
FROM emps
JOIN depts ON emps.deptno = depts.deptno
WHERE salary > 12000
```

* Rewriting:

```
SELECT empid, deptname
FROM mv
UNION ALL
SELECT empid, deptname
FROM emps
JOIN depts ON emps.deptno = depts.deptno
WHERE salary > 10000 AND salary <= 12000
```


###### Union rewriting with aggregate

* Query:

```
SELECT empid, deptname, SUM(salary) AS s
FROM emps
JOIN depts ON emps.deptno = depts.deptno
WHERE salary > 10000
GROUP BY empid, deptname
```

* Materialized view definition:

```
SELECT empid, deptname, SUM(salary) AS s
FROM emps
JOIN depts ON emps.deptno = depts.deptno
WHERE salary > 12000
GROUP BY empid, deptname
```

* Rewriting:

```
SELECT empid, deptname, SUM(s)
FROM (
  SELECT empid, deptname, s
  FROM mv
  UNION ALL
  SELECT empid, deptname, SUM(salary) AS s
  FROM emps
  JOIN depts ON emps.deptno = depts.deptno
  WHERE salary > 10000 AND salary <= 12000
  GROUP BY empid, deptname) AS subq
GROUP BY empid, deptname
```


##### Limitations

This rule still presents some limitations. In particular, the rewriting rule attempts to match all views against each query. We plan to implement more refined filtering techniques such as those described in [<a href="#ref-gl01">GL01</a>].

## References

<ul>
<li>[<a name="ref-gl01">GL01</a>] Jonathan Goldstein and Per-åke Larson.
    <a href="http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.95.113">Optimizing queries using materialized views: A practical, scalable solution</a>.
    In <i>Proc. ACM SIGMOD Conf.</i>, 2001.</li>
</ul>
