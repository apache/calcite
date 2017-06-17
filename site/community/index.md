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

* 2017/04/04 [Apex Big Data World 2017](http://www.apexbigdata.com/mountain-view.html) (Mountain View, USA)
  * [Streaming SQL](http://www.apexbigdata.com/platform-track-2.html) &mdash; Julian Hyde
* 2017/05/16&ndash;18 [Apache: Big Data North America 2017](http://events.linuxfoundation.org/events/apache-big-data-north-america) (Miami, USA)
  * [Data Profiling in Apache Calcite](https://apachebigdata2017.sched.com/event/A00j) &mdash; Julian Hyde
  * [A Smarter Pig](https://apachebigdata2017.sched.com/event/A02J) &mdash; Eli Levine and Julian Hyde
* 2017/06/13&ndash;15 [DataWorks Summit 2017](https://dataworkssummit.com/san-jose-2017/) (San Jose, USA)
  * [Data Profiling in Apache Calcite](https://dataworkssummit.com/san-jose-2017/agenda/) &mdash; Julian Hyde

# Project Members

Name (Apache ID) | Github | Org | Role
:--------------- | :----- | :-- | :---
{% for c in site.data.contributors %}{% if c.homepage %}<a href="{{ c.homepage }}">{{ c.name }}</a>{% else %}{{ c.name }}{% endif %} (<a href="http://people.apache.org/phonebook.html?uid={{ c.apacheId }}">{{ c.apacheId }}</a>) | <a href="http://github.com/{{ c.githubId }}"><img width="64" src="{% unless c.avatar %}http://github.com/{{ c.githubId }}.png{% else %}{{ c.avatar }}{% endunless %}"></a> | {{ c.org }} | {{ c.role }}
{% endfor %}

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
  [archive](http://mail-archives.apache.org/mod_mbox/calcite-dev/). To
  subscribe to the user list, please send email to
  [dev-subscribe@calcite.apache.org](mailto:dev-subscribe@calcite.apache.org).
* **Bug Reports**.
  Please file any issues you encounter or fixes you'd like on the
  [Calcite Jira](https://issues.apache.org/jira/browse/CALCITE). We welcome
  patches and pull-requests!
* **StackOverflow**.
  [StackOverflow](http://stackoverflow.com/questions/tagged/calcite) is a wonderful resource for
  any developer. Take a look over there to see if someone has answered
  your question.
* **Browse the code**.
  One of the advantages of open source software is that you can browse the code.
  The code is available on [github](https://github.com/apache/calcite/tree/master).

# Talks

Want to learn more about Calcite?

Watch some presentations and read through some slide decks about
Calcite, or attend one of the [upcoming talks](#upcoming-talks).

## Apache Calcite: One planner fits all

Voted [Best Lightning Talk at XLDB-2015](http://www.xldb.org/archives/2015/05/best-lightning-talks-selected/);
[[video](https://www.youtube.com/watch?v=5_MyORYjq3w)],
[[slides](http://www.slideshare.net/julianhyde/apache-calcite-one-planner-fits-all)].

{% oembed https://www.youtube.com/watch?v=5_MyORYjq3w %}

## Streaming SQL

At Hadoop Summit, San Jose, CA, 2016
[[video](https://www.youtube.com/watch?v=b7HENkvd1uU)],
[[slides](http://www.slideshare.net/julianhyde/streaming-sql-63554778)],
[[pdf](https://github.com/julianhyde/share/blob/master/slides/calcite-streaming-sql-san-jose-2016.pdf?raw=true)].

{% oembed https://www.youtube.com/watch?v=b7HENkvd1uU %}

## Cost-based Query Optimization in Apache Phoenix using Apache Calcite

At Hadoop Summit, San Jose, CA, 2016
[[video](https://www.youtube.com/watch?v=gz9X7JD8BAU)],
[[slides](http://www.slideshare.net/julianhyde/costbased-query-optimization-in-apache-phoenix-using-apache-calcite)],
[[pdf](https://github.com/julianhyde/share/blob/master/slides/phoenix-on-calcite-hadoop-summit-2016.pdf?raw=true)].

{% oembed https://www.youtube.com/watch?v=gz9X7JD8BAU %}

## Planning with Polyalgebra: Bringing together relational, complex and machine learning algebra

As Hadoop Summit, Dublin, 2016
[[video](https://www.youtube.com/watch?v=fHZqbe3iPMc)],
[[slides](http://www.slideshare.net/julianhyde/planning-with-polyalgebra-bringing-together-relational-complex-and-machine-learning-algebra)].

{% oembed https://www.youtube.com/watch?v=fHZqbe3iPMc %}

## More talks

* <a href="https://github.com/julianhyde/share/blob/master/slides/calcite-algebra-edw-2015.pdf?raw=true">Why you care about relational algebra (even though you didn't know it)</a> (Washington DC, 2015)
* <a href="https://github.com/julianhyde/share/blob/master/slides/hive-cbo-seattle-2014.pdf?raw=true">Cost-based optimization in Hive 0.14</a> (Seattle, 2014)
* <a href="https://github.com/julianhyde/share/blob/master/slides/dmmq-summit-2014.pdf?raw=true">Discardable, in-memory materialized query for Hadoop</a> (<a href="https://www.youtube.com/watch?v=CziGOa8GXqI">video</a>) (Hadoop Summit, 2014)
* <a href="https://github.com/julianhyde/share/blob/master/slides/optiq-nosql-now-2013.pdf?raw=true">SQL Now!</a> (NoSQL Now! conference, 2013)
* <a href="https://github.com/julianhyde/share/blob/master/slides/optiq-drill-user-group-2013.pdf?raw=true">Drill / SQL / Optiq</a> (2013)
* <a href="http://www.slideshare.net/julianhyde/how-to-integrate-splunk-with-any-data-solution">How to integrate Splunk with any data solution</a> (Splunk User Conference, 2012)
