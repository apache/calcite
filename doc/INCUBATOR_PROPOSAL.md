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
# Optiq

## Abstract

Optiq is a framework that allows efficient translation of queries
involving heterogeneous and federated data.

## Proposal

Optiq is a highly customizable engine for parsing and planning queries
on data in a wide variety of formats. It allows database-like access,
and in particular a SQL interface and advanced query optimization, for
data not residing in a traditional database.

## Background

Databases were traditionally engineered in a monolithic stack,
providing a data storage format, data processing algorithms, query
parser, query planner, built-in functions, metadata repository and
connectivity layer. They innovate in some areas but rarely in all.

Modern data management systems are decomposing that stack
into separate components, separating data, processing engine,
metadata, and query language support. They are highly heterogeneous,
with data in multiple locations and formats, caching and redundant
data, different workloads, and processing occurring in different
engines.

Query planning (sometimes called query optimization) has always been
a key function of a DBMS, because it allows the implementors to
introduce new query-processing algorithms, and allows data
administrators to re-organize the data without affecting applications
built on that data. In a componentized system, the query planner
integrates the components (data formats, engines, algorithms) without
introducing unncessary coupling or performance tradeoffs.

But building a query planner is hard; many systems muddle along
without a planner, and indeed a SQL interface, until the demand from
their customers is overwhelming.

There is an opportunity to make this process more efficient by
creating a re-usable framework.

## Rationale

Optiq allows database-like access, and in particular a SQL interface
and advanced query optimization, for data not residing in a
traditional database. It is complementary to many current Hadoop and
NoSQL systems, which have innovative and performant storage and
runtime systems but lack a SQL interface and intelligent query
translation.

Optiq is already in use by several projects, including Apache Drill,
Apache Hive and Cascading Lingual, and commercial products.

Optiq's architecture consists of:
* An extensible relational algebra.
* SPIs (service-provider interfaces) for metadata (schemas and
  tables), planner rules, statistics, cost-estimates, user-defined
  functions.
* Built-in sets of rules for logical transformations and common data-sources.
* Two query planning engines driven by rules, statistics, etc. One
  engine is cost-based, the other rule-based.
* Optional SQL parser, validator and translator to relational algebra.
* Optional JDBC driver.

## Initial Goals

The initial goals are be to move the existing codebase to Apache and
integrate with the Apache development process. Once this is
accomplished, we plan for incremental development and releases that
follow the Apache guidelines.

As we move the code into the org.apache namespace, we will restructure
components as necessary to allow clients to use just the components of
Optiq that they need.

A version 1.0 release, including pre-built binaries, will foster wider
adoption.

## Current Status

Optiq has had over a dozen minor releases over the last 18 months. Its
core SQL parser and validator, and its planning engine and core rules,
are mature and robust and are the basis for several production
systems; but other components and SPIs are still undergoing rapid
evolution.

### Meritocracy

We plan to invest in supporting a meritocracy. We will discuss the
requirements in an open forum. We encourage the companies and projects
using Optiq to discuss their requirements in an open forum and to
participate in development.  We will encourage and monitor community
participation so that privileges can be extended to those that
contribute.

Optiq's pluggable architecture encourages developers to contribute
extensions such as adapters for data sources, new planning rules, and
better statistics and cost-estimation functions.  We look forward to
fostering a rich ecosystem of extensions.

### Community

Building a data management system requires a high degree of
technical skill, and correspondingly, the community of developers
directly using Optiq is potentially fairly small, albeit highly
technical and engaged. But we also expect engagement from members of
the communities of projects that use Optiq, such as Drill and
Hive. And we intend to structure Optiq so that it can be used for
lighter weight applications, such as providing a SQL and JDBC
interface to a NoSQL system.

### Core Developers

The developers on the initial committers list are all experienced open
source developers, and are actively using Optiq in their projects.

* Julian Hyde is lead developer of Mondrian, an open source OLAP
  engine, and an Apache Drill committer.
* Chris Wensel is lead developer of Cascading, and of Lingual, the SQL
  interface to Cascading built using Optiq.
* Jacques Nadeau is lead developer of Apache Drill, which uses Optiq.

In addition, there are several regular contributors whom we hope will
graduate to committers during the incubation process.

We realize that additional employer diversity is needed, and we will
work aggressively to recruit developers from additional companies.

### Alignment

Apache, and in particular the ecosystem surrounding Hadoop, contains
several projects for building data management systems that leverage
each other's capabilities. Optiq is a natural fit for that ecosystem,
and will help foster projects meeting new challenges.

Optiq is already used by Apache Hive and Apache Drill; Optiq embeds
Apache Spark as an optional engine; we are in discussion with Apache
Phoenix about integrating JDBC and query planning.

## Known Risks

### Orphaned Products

Optiq is already a key component in three independent projects, each
backed by a different company, so the risk of being orphaned is
relatively low. We plan to mitigate this risk by recruiting additional
committers, and promoting Optiq's adoption as a framework by other
projects.

### Inexperience with Open Source

The initial committers are all Apache members, some of whom have
several years in the Apache Hadoop community. The founder of the
project, Julian Hyde, has been a founder and key developer in open
source projects for over ten years.

### Homogenous Developers

The initial committers are employed by a number of companies,
including Concurrent, Hortonworks, MapR Technologies and Salesforce.com.
We are committed to recruiting additional committers from outside these
companies.

### Reliance on Salaried Developers

Like most open source projects, Optiq receives substantial support
from salaried developers. This is to be expected given that it is a
highly technical framework. However, they are all passionate about the
project, and we are confident that the project will continue even if
no salaried developers contribute to the project. As a framework, the
project encourages the involvement of members of other projects, and
of academic researchers. We are committed to recruiting additional
committers including non-salaried developers.

### Relationships with Other Apache Products

As mentioned in the Alignment section, Optiq is being used by
<a href="http://hive.apache.org">Apache Hive</a> and
<a href="http://incubator.apache.org/drill">Apache Drill</a>,
and has adapters for
<a href="http://phoenix.incubator.apache.org">Apache Phoenix</a>
and
<a href="http://spark.apache.org">Apache Spark</a>.
Optiq often operates on data in a Hadoop environment, so collaboration
with other Hadoop projects is desirable and highly likely.

Unsurprisingly there is some overlap in capabilities between Optiq and
other Apache projects. Several projects that are databases or
database-like have query-planning capabilities. These include Hive,
Drill, Phoenix, Spark,
<a href="http://db.apache.org/derby">Apache Derby</a>,
<a href="http://pig.apache.org">Apache Pig</a>,
<a href="http://jena.apache.org">Apache Jena</a> and
<a href="http://tajo.apache.org">Apache Tajo</a>.
Optiq’s query planner is extensible at run time, and does
not have a preferred runtime engine on which to execute compiled
queries. These capabilities, and the large corpus of pre-built rules,
are what allow Optiq to be embedded in other projects.

Several other Apache projects access third-party data sources,
including Hive, Pig, Drill, Spark and
<a href="http://metamodel.incubator.apache.org">Apache MetaModel</a>.
Optiq allows users to optimize access to third-party data sources by
writing rules to push processing down to the data source, and provide
a cost model to choose the optimal location for processing. That said,
maintaining a library of adapters is burdensome, and so it would make
sense to collaborate with other projects on adapter libraries, and
re-use libraries where possible.

Optiq supports several front ends for submitting queries. The most
popular is SQL, with driver connectivity via JDBC (and ODBC
planned). Other Apache projects with a SQL parser include Hive, Spark,
Phoenix, Derby, Tajo. Drill uses Optiq’s parser and JDBC stack; both
Phoenix and Drill have expressed interest in collaborating on JDBC and
ODBC. Optiq’s Linq4j API is similar to the fluent query-builder APIs
in Spark and MetaModel. Use of a front end is not required; for
instance, Hive integrates with Optiq by directly building a graph of
`RelNode` objects.

### An Excessive Fascination with the Apache Brand

Optiq solves a real problem, as evidenced by its take-up by other
projects. This proposal is not for the purpose of generating
publicity. Rather, the primary benefits to joining Apache are those
outlined in the Rationale section.

## Documentation

Additional documentation for Optiq may be found on its github site:
* Overview - https://github.com/julianhyde/optiq/blob/master/README.md
* Tutorial - https://github.com/julianhyde/optiq-csv/blob/master/TUTORIAL.md
* HOWTO - https://github.com/julianhyde/optiq/blob/master/HOWTO.md
* Reference guide -  https://github.com/julianhyde/optiq/blob/master/REFERENCE.md

Presentation:
* <a href="https://github.com/julianhyde/share/blob/master/slides/optiq-richrelevance-2013.pdf?raw=true">SQL on Big Data using Optiq</a>

## Initial Source

The initial code codebase resides in three projects, all hosted on github:
* https://github.com/julianhyde/optiq
* https://github.com/julianhyde/optiq-csv
* https://github.com/julianhyde/linq4j

### Source and Intellectual Property Submission Plan

The initial codebase is already distributed under the Apache 2.0
License. The owners of the IP have indicated willingness to sign the
SGA.

## External Dependencies

Optiq and Linq4j have the following external dependencies.

* Java 1.6, 1.7 or 1.8
* Apache Maven, Commons
* JavaCC (BSD license)
* Sqlline 1.1.6 (BSD license)
* Junit 4.11 (EPL)
* Janino (BSD license)
* Guava (Apache 2.0 license)
* Eigenbase-resgen, eigenbase-xom, eigenbase-properties (Apache 2.0 license)

Some of Optiq's adapters (optiq-csv, optiq-mongodb, optiq-spark,
optiq-splunk) are currently developed alongside core Optiq, and have
the following additional dependencies:

* Open CSV 2.3 (Apache 2.0 license)
* Apache Incubator Spark
* Mongo Java driver (Apache 2.0 license)

Upon acceptance to the incubator, we would begin a thorough analysis
of all transitive dependencies to verify this information and
introduce license checking into the build and release process by
integrating with Apache Rat.

## Cryptography

Optiq will eventually support encryption on the wire. This is not one
of the initial goals, and we do not expect Optiq to be a controlled
export item due to the use of encryption.

## Required Resources

### Mailing Lists

* private@optiq.incubator.apache.org
* dev@optiq.incubator.apache.org (will be migrated from optiq-dev@googlegroups.com)
* commits@optiq.incubator.apache.org

### Source control

The Optiq team would like to use git for source control, due to our
current use of git/github. We request a writeable git repo
git://git.apache.org/incubator-optiq, and mirroring to be set up to
github through INFRA.

### Issue Tracking

Optiq currently uses the github issue tracking system associated with
its github repo: https://github.com/julianhyde/optiq/issues. We will
migrate to the Apache JIRA:
http://issues.apache.org/jira/browse/OPTIQ.

## Initial Committers

Julian Hyde (jhyde at apache dot org)
Jacques Nadeau (jacques at apache dot org)
James R. Taylor (jamestaylor at apache dot org)
Chris Wensel (cwensel at apache dot org)

## Affiliations

The initial committers are employees of Concurrent, Hortonworks, MapR
and Salesforce.com.

* Julian Hyde (Hortonworks)
* Jacques Nadeau (MapR Technologies)
* James R. Taylor (Salesforce.com)
* Chris Wensel (Concurrent)

## Sponsors

### Champion

* Ashutosh Chauhan (hashutosh at apache dot org)

### Nominated Mentors

* Ted Dunning (tdunning at apache dot org) – Chief Application Architect at
  MapR Technologies; committer for Lucene, Mahout and ZooKeeper.
* Alan Gates (gates at apache dot org) - Architect at Hortonworks;
  committer for Pig, Hive and others.
* Steven Noels (stevenn at apache dot org) - Chief Technical Officer at NGDATA;
  committer for Cocoon and Forrest, mentor for Phoenix.

### Sponsoring Entity

The Apache Incubator.
