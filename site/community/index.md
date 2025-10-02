---
layout: page
title: Community
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

* TOC
{:toc}

# Upcoming talks

None scheduled.

# Project Members

Name (Apache ID) | Github                                                                                                                                                                                   | Org | Role
:--------------- |:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| :-- | :---
{% for c in site.data.contributors %}{% unless c.emeritus %}{% if c.homepage %}<a href="{{ c.homepage }}">{{ c.name }}</a>{% else %}{{ c.name }}{% endif %} (<a href="https://people.apache.org/phonebook.html?uid={{ c.apacheId }}">{{ c.apacheId }}</a>) {{ c.pronouns }} | <a href="https://github.com/{{ c.githubId }}"><img width="64" src="{% unless c.avatar %}{{ site.baseurl }}/img/avatars/{{ c.githubId }}.png{% else %}{{ c.avatar }}{% endunless %}"></a> | {{ c.org }} | {{ c.role }}
{% endunless %}{% endfor %}

Emeritus members

Name (Apache ID) | Github | Org | Role
:--------------- | :----- | :-- | :---
{% for c in site.data.contributors %}{% if c.emeritus %}{% if c.homepage %}<a href="{{ c.homepage }}">{{ c.name }}</a>{% else %}{{ c.name }}{% endif %} (<a href="https://people.apache.org/phonebook.html?uid={{ c.apacheId }}">{{ c.apacheId }}</a>) {{ c.pronouns }} | <a href="https://github.com/{{ c.githubId }}"><img width="64" src="{% unless c.avatar %}{{ site.baseurl }}/img/avatars/{{ c.githubId }}.png{% else %}{{ c.avatar }}{% endunless %}"></a> | {{ c.org }} | {{ c.role }}
{% endif %}{% endfor %}

# Mailing Lists

There are several mailing lists for Calcite:

* [dev@calcite.apache.org](mailto:dev@calcite.apache.org) &mdash; Development discussions
  [[archive](https://mail-archives.apache.org/mod_mbox/calcite-dev/)]
* [issues@calcite.apache.org](mailto:issues@calcite.apache.org) &mdash; Bug tracking
  [[archive](https://mail-archives.apache.org/mod_mbox/calcite-issues/)]
* [commits@calcite.apache.org](mailto:commits@calcite.apache.org) &mdash; Git tracking
  [[archive](https://mail-archives.apache.org/mod_mbox/calcite-commits/)]

You can subscribe to the lists by sending email to
*{list}*-subscribe@calcite.apache.org and unsubscribe by sending email to
*{list}*-unsubscribe@calcite.apache.org (where *{list}* is either "dev", "issues", or "commits").

# Help

Need help with Calcite? Try these resources:

* **Mailing Lists**.
  The best option is to send email to the developers list
  [dev@calcite.apache.org](mailto:dev@calcite.apache.org). All
  of the historic traffic is available in the
  [archive](https://mail-archives.apache.org/mod_mbox/calcite-dev/). To
  subscribe to the user list, please send email to
  [dev-subscribe@calcite.apache.org](mailto:dev-subscribe@calcite.apache.org).
* **Bug Reports**.
  Please file any issues you encounter or fixes you'd like on the
  [Calcite Jira](https://issues.apache.org/jira/browse/CALCITE). We welcome
  patches and pull-requests!
* **StackOverflow**.
  [StackOverflow](https://stackoverflow.com/questions/tagged/calcite) is a wonderful resource for
  any developer. Take a look over there to see if someone has answered
  your question.
* **Browse the code**.
  One of the advantages of open source software is that you can browse the code.
  The code is available on [GitHub](https://github.com/apache/calcite/tree/main).

# Talks

Want to learn more about Calcite?

Watch some presentations and read through some slide decks about
Calcite, or attend one of the [upcoming talks](#upcoming-talks).

## Federated Query Planning w/ Calcite & Substrait

At [Calcite Hybrid Meetup](https://www.meetup.com/apache-calcite/events/305627349), February 2025;
[[video](https://www.youtube.com/watch?v=PHm5vZ1A43I&t=231s)].

## Streaming, Incremental, Finite-Memory Computations in SQL Over Unbounded Streams

At [Calcite Hybrid Meetup](https://www.meetup.com/apache-calcite/events/305627349), February 2025;
[[video](https://www.youtube.com/watch?v=PHm5vZ1A43I&t=2212s)].

## Revolutionizing Data Lakes: A Dive into Coral, the SQL Translation, Analysis, and Rewrite Engine

At [Calcite Hybrid Meetup](https://www.meetup.com/apache-calcite/events/305627349), February 2025;
[[video](https://www.youtube.com/watch?v=PHm5vZ1A43I&t=4226s)].

## Optimizing Common Table Expressions in Apache Hive with Calcite

At [Calcite Hybrid Meetup](https://www.meetup.com/apache-calcite/events/305627349), February 2025;
[[sumary](https://github.com/zabetak/slides/blob/master/2025/calcite-meetup-feb/optimizing-common-table-expressions-in-apache-hive-with-calcite.md)],
[[slides](https://www.slideshare.net/slideshow/optimizing-common-table-expressions-in-apache-hive-with-calcite/276219213)],
[[pdf](https://github.com/zabetak/slides/blob/master/2025/calcite-meetup-feb/optimizing-common-table-expressions-in-apache-hive-with-calcite.pdf)],
[[video](https://www.youtube.com/watch?v=PHm5vZ1A43I&t=6317s)].

## Adding measures to Calcite SQL

At [Calcite Virtual Meetup](https://www.meetup.com/apache-calcite/events/291489488/), March 2023;
[[slides](https://www.slideshare.net/julianhyde/adding-measures-to-calcite-sql)],
[[video](https://youtu.be/COFQrSEX_iI)].

## Building a streaming incremental view maintenance engine with Calcite

At [Calcite Virtual Meetup](https://www.meetup.com/apache-calcite/events/291489488/), March 2023;
[[slides](http://budiu.info/work/dbsp-calcite23.pptx)],
[[video](https://youtu.be/iT4k5DCnvPU)].

## Debugging planning issues using Calcite's built in loggers

At [Calcite Virtual Meetup](https://www.meetup.com/apache-calcite/events/291489488/), March 2023;
[[summary](https://github.com/zabetak/slides/blob/master/2023/calcite-meetup-march/debugging-planning-issues-using-calcites-built-in-loggers.md)],
[[slides](https://www.slideshare.net/StamatisZampetakis/debugging-planning-issues-using-calcites-builtin-loggers)],
[[pdf](https://github.com/zabetak/slides/blob/master/2023/calcite-meetup-march/debugging-planning-issues-using-calcites-built-in-loggers.pdf)],
[[video](https://youtu.be/_phzRNCWJfw)].

## calcite-clj - Use Calcite with Clojure

At [Calcite Virtual Meetup](https://www.meetup.com/Apache-Calcite/events/282836907/), January 2022;
[[slides](https://ieugen.github.io/calcite-clj/)],
[[video](https://www.youtube.com/watch?v=9CUWX8JHA90)],
[[code](https://github.com/ieugen/calcite-clj)].

## Morel, a functional query language (Julian Hyde)

At [Strange Loop 2021](https://thestrangeloop.com/2021/morel-a-functional-query-language.html),
St. Louis, Missouri, September 30, 2021;
[[slides](https://www.slideshare.net/julianhyde/morel-a-functional-query-language)].

## Building modern SQL query optimizers with Apache Calcite

At [ApacheCon 2021](https://www.apachecon.com/acah2021/tracks/bigdatasql.html), September 22, 2021.

## Apache Calcite Tutorial

At [BOSS 2021](https://boss-workshop.github.io/boss-2021/), Copenhagen, Denmark, August 16, 2021;
[[summary](https://github.com/zabetak/slides/blob/master/2021/boss-workshop/apache-calcite-tutorial.md)],
[[slides](https://www.slideshare.net/StamatisZampetakis/apache-calcite-tutorial-boss-21)],
[[pdf](https://github.com/zabetak/slides/blob/master/2021/boss-workshop/apache-calcite-tutorial.pdf)],
[[video](https://youtu.be/meI0W12f_nw)].

## An introduction to query processing & Apache Calcite

At [Calcite Virtual Meetup](https://www.meetup.com/Apache-Calcite/events/275461117/), January 20, 2021;
[[summary](https://github.com/zabetak/slides/blob/master/2021/calcite-meetup-january/an-introduction-to-query-processing-and-apache-calcite.md)],
[[slides](https://github.com/zabetak/slides/blob/master/2021/calcite-meetup-january/an-introduction-to-query-processing-and-apache-calcite.pdf)],
[[video](https://youtu.be/p1O3E33FIs8)].

## Calcite streaming for event-time semantics

At [Calcite Virtual Meetup](https://www.meetup.com/Apache-Calcite/events/275461117/), January 20, 2021;
[[video](https://youtu.be/n4NU8J1DlWI)].

## Efficient spatial queries on vanilla databases

At [Calcite Virtual Meetup](https://www.meetup.com/Apache-Calcite/events/275461117/), January 20, 2021;
[[video](https://youtu.be/6iozdGUL-aw)].

## Apache Calcite integration in Hazelcast In-Memory Data Grid

At [Calcite Virtual Meetup](https://www.meetup.com/Apache-Calcite/events/275461117/), January 20, 2021;
[[video](https://youtu.be/2cKE4HyhIrc)].

## Fast federated SQL with Apache Calcite

At [ApacheCon Europe 2019](https://aceu19.apachecon.com/), Berlin, Germany, October 24, 2019;
[[summary](https://aceu19.apachecon.com/session/fast-federated-sql-apache-calcite)],
[[video](https://youtu.be/4JAOkLKrcYE)].


## One SQL to Rule Them All - an Efficient and Syntactically Idiomatic Approach to Management of Streams and Tables

At [SIGMOD/PODS 2019](https://sigmod2019.org/sigmod_industry_list), Amsterdam, Netherlands, 2019
and [Beam Summit Europe 2019](https://beam-summit.firebaseapp.com/schedule/);
[[paper](https://arxiv.org/abs/1905.12133)],
[[review](https://blog.acolyer.org/2019/07/03/one-sql-to-rule-them-all/)],
[[pdf](https://github.com/julianhyde/share/blob/main/slides/one-sql-to-rule-them-all-beam-summit-2019.pdf?raw=true)],
[[video](https://www.youtube.com/watch?v=9f4igtyNseo)].

## Apache Calcite: A Foundational Framework for Optimized Query Processing Over Heterogeneous Data Sources

At [SIGMOD/PODS 2018](https://sigmod2018.org/index.shtml), Houston, TX, 2018;
[[paper](https://arxiv.org/pdf/1802.10233)],
[[slides](https://www.slideshare.net/julianhyde/apache-calcite-a-foundational-framework-for-optimized-query-processing-over-heterogeneous-data-sources)],
[[pdf](https://github.com/julianhyde/share/blob/main/slides/calcite-sigmod-2018.pdf?raw=true)].

## Spatial query on vanilla databases

At ApacheCon North America, 2018;
[[slides](https://www.slideshare.net/julianhyde/spatial-query-on-vanilla-databases)],
[[pdf](https://github.com/julianhyde/share/blob/main/slides/calcite-spatial-apache-con-2018.pdf?raw=true).

## Apache Calcite: One planner fits all

Voted [Best Lightning Talk at XLDB-2015](https://www.xldb.org/archives/2015/05/best-lightning-talks-selected/);
[[video](https://www.youtube.com/watch?v=5_MyORYjq3w)],
[[slides](https://www.slideshare.net/julianhyde/apache-calcite-one-planner-fits-all)].

## Streaming SQL

At Hadoop Summit, San Jose, CA, 2016
[[video](https://www.youtube.com/watch?v=b7HENkvd1uU)],
[[slides](https://www.slideshare.net/julianhyde/streaming-sql-63554778)],
[[pdf](https://github.com/julianhyde/share/blob/main/slides/calcite-streaming-sql-san-jose-2016.pdf?raw=true)].

## Cost-based Query Optimization in Apache Phoenix using Apache Calcite

At Hadoop Summit, San Jose, CA, 2016
[[video](https://www.youtube.com/watch?v=gz9X7JD8BAU)],
[[slides](https://www.slideshare.net/julianhyde/costbased-query-optimization-in-apache-phoenix-using-apache-calcite)],
[[pdf](https://github.com/julianhyde/share/blob/main/slides/phoenix-on-calcite-hadoop-summit-2016.pdf?raw=true)].

## Planning with Polyalgebra: Bringing together relational, complex and machine learning algebra

As Hadoop Summit, Dublin, 2016
[[video](https://www.youtube.com/watch?v=fHZqbe3iPMc)],
[[slides](https://www.slideshare.net/julianhyde/planning-with-polyalgebra-bringing-together-relational-complex-and-machine-learning-algebra)].

## More talks

* <a href="https://github.com/julianhyde/share/blob/main/slides/calcite-algebra-edw-2015.pdf?raw=true">Why you care about relational algebra (even though you didn't know it)</a> (Washington DC, 2015)
* <a href="https://github.com/julianhyde/share/blob/main/slides/hive-cbo-seattle-2014.pdf?raw=true">Cost-based optimization in Hive 0.14</a> (Seattle, 2014)
* <a href="https://github.com/julianhyde/share/blob/main/slides/dmmq-summit-2014.pdf?raw=true">Discardable, in-memory materialized query for Hadoop</a> (<a href="https://www.youtube.com/watch?v=CziGOa8GXqI">video</a>) (Hadoop Summit, 2014)
* <a href="https://github.com/julianhyde/share/blob/main/slides/optiq-nosql-now-2013.pdf?raw=true">SQL Now!</a> (NoSQL Now! conference, 2013)
* <a href="https://github.com/julianhyde/share/blob/main/slides/optiq-drill-user-group-2013.pdf?raw=true">Drill / SQL / Optiq</a> (2013)
* <a href="https://www.slideshare.net/julianhyde/how-to-integrate-splunk-with-any-data-solution">How to integrate Splunk with any data solution</a> (Splunk User Conference, 2012)

# External resources

A collection of articles, blogs, presentations, and interesting projects related to Apache Calcite.

If you have something interesting to share with the community drop us an email on the dev list or
consider creating a pull request on GitHub. If you just finished a cool project using Calcite
consider writing a short article about it for our [news section]({{ site.baseurl }}/news/index.html).

* <a href="https://www.feldera.com/blog/constant-folding-in-calcite">Constant folding in Calcite</a> (Mihai Budiu, 2025)
* <a href="https://github.com/JiajunBernoulli/calcite-notes">Calcite notes about runnable examples and concise documents</a> (Jiajun Xie, 2024)
* <a href="https://www.feldera.com/blog/calcite-irs/">Calcite program representations</a> (Mihai Budiu, October 2023)
* <a href="https://datalore.jetbrains.com/view/notebook/JYTVfn90xYSmv6U5f2NIQR">Building a new Calcite frontend (GraphQL)</a> (Gavin Ray, 2022)
* <a href="https://github.com/ieugen/calcite-clj">Write Calcite adapters in Clojure</a> (Ioan Eugen Stan, 2022)
* <a href="https://www.querifylabs.com/blog/cross-product-suppression-in-join-order-planning">Cross-Product Suppression in Join Order Planning</a> (Vladimir Ozerov, 2021)
* <a href="https://www.querifylabs.com/blog/metadata-management-in-apache-calcite">Metadata Management in Apache Calcite</a> (Roman Kondakov, 2021)
* <a href="https://www.querifylabs.com/blog/relational-operators-in-apache-calcite">Relational Operators in Apache Calcite</a> (Vladimir Ozerov, 2021)
* <a href="https://www.querifylabs.com/blog/introduction-to-the-join-ordering-problem">Introduction to the Join Ordering Problem</a> (Alexey Goncharuk, 2021)
* <a href="https://www.querifylabs.com/blog/what-is-cost-based-optimization">What is Cost-based Optimization?</a> (Alexey Goncharuk, 2021)
* <a href="https://www.querifylabs.com/blog/memoization-in-cost-based-optimizers">Memoization in Cost-based Optimizers</a> (Vladimir Ozerov, 2021)
* <a href="https://www.querifylabs.com/blog/rule-based-query-optimization">Rule-based Query Optimization</a> (Vladimir Ozerov, 2021)
* <a href="https://www.querifylabs.com/blog/custom-traits-in-apache-calcite">Custom traits in Apache Calcite</a> (Vladimir Ozerov, 2020)
* <a href="https://www.querifylabs.com/blog/assembling-a-query-optimizer-with-apache-calcite">Assembling a query optimizer with Apache Calcite</a> (Vladimir Ozerov, 2020)
* <a href="https://github.com/michaelmior/calcite-notebooks">A series of Jupyter notebooks to demonstrate the functionality of Apache Calcite</a> (Michael Mior)
* <a href="https://github.com/pingcap/awesome-database-learning">A curated collection of resources about databases</a>
