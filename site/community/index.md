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

* 2016/03/30 <a href="http://conferences.oreilly.com/strata/hadoop-big-data-ca/public/schedule/detail/48180">Strata + Hadoop World</a>, San Jose (developer showcase)
* 2016/04/13 <a href="http://hadoopsummit.org/dublin/agenda/">Hadoop Summit</a>, Dublin
* 2016/04/26 <a href="http://kafka-summit.org/schedule/">Kafka Summit</a>, San Francisco
* 2016/05/10 <a href="http://events.linuxfoundation.org/events/apache-big-data-north-america/program/schedule">ApacheCon Big Data North America</a>, Vancouver
* 2016/05/24 <a href="http://www-conf.slac.stanford.edu/xldb2016/Program.asp">XLDB</a>, Palo Alto

# Project Members

Name (Apache ID) | Github | Org | Role
:--------------- | :----- | :-- | :---
{% for c in site.data.contributors %}  {{ c.name }} (<a href="http://people.apache.org/committer-index#{{ c.apacheId }}">{{ c.apacheId }}</a>) | <a href="http://github.com/{{ c.githubId }}"><img width="64" src="{% unless c.avatar %}http://github.com/{{ c.githubId }}.png{% else %}{{ c.avatar }}{% endunless %}"></a> | {{ c.org }} | {{ c.role }}
{% endfor %}

# Mailing Lists

There are several development mailing lists for Calcite:

* [dev@calcite.apache.org](mailto:dev@calcite.apache.org) - Development discussions
  [[archive](https://mail-archives.apache.org/mod_mbox/calcite-dev/)]
* [issues@calcite.apache.org](mailto:issues@calcite.apache.org) - Bug tracking
  [[archive](https://mail-archives.apache.org/mod_mbox/calcite-issues/)]
* [commits@calcite.apache.org](mailto:commits@calcite.apache.org) - Git tracking
  [[archive](https://mail-archives.apache.org/mod_mbox/calcite-commits/)]

You can subscribe to the lists by sending email to
*list*-subscribe@calcite.apache.org and unsubscribe by sending email to
*list*-unsubscribe@calcite.apache.org.

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
  patches!
* **StackOverflow**.
  [StackOverflow](http://stackoverflow.com) is a wonderful resource for
  any developer. Take a look over there to see if someone has answered
  your question.
* **Browse the code**.
  One of the advantages of open source software is that you can browse the code.
  The code is available on [github](https://github.com/apache/calcite/tree/master).

# Talks

Want to learn more about Calcite?

Watch some presentations and read through some slide decks about Calcite.

## Apache Calcite: One planner fits all

Voted [Best Lightning Talk at XLDB-2015](http://www.xldb.org/archives/2015/05/best-lightning-talks-selected/);
[[video](https://www.youtube.com/watch?v=5_MyORYjq3w)],
[[slides](http://www.slideshare.net/julianhyde/apache-calcite-one-planner-fits-all)].

{% oembed https://www.youtube.com/watch?v=5_MyORYjq3w %}

## Why you care about relational algebra (even though you didn't know it)

Washington DC, April 2015;
[[slides](http://www.slideshare.net/julianhyde/calcite-algebraedw2015)].

<iframe src="//www.slideshare.net/slideshow/embed_code/key/vfVDu6y1mAM5Dl" width="629" height="355" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe>

## Apache Calcite overview

Apache Kylin meetup, 2014;
[[slides](http://www.slideshare.net/julianhyde/apache-calcite-overview)].

<iframe src="//www.slideshare.net/slideshow/embed_code/key/fCGsAedsQiq53V" width="629" height="354" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe>

## Streaming SQL

At Samza meetup, Mountain View, CA, 2016
[[video](http://www.ustream.tv/recorded/83322450#to00:55:48)],
[[slides](http://www.slideshare.net/julianhyde/streaming-sql)].

<iframe src="//www.slideshare.net/slideshow/embed_code/key/rzaptOy3H8K6Gz" width="595" height="485" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe>

## More talks

* <a href="https://github.com/julianhyde/share/blob/master/slides/hive-cbo-seattle-2014.pdf?raw=true">Cost-based optimization in Hive 0.14</a> (Seattle, 2014)
* <a href="https://github.com/julianhyde/share/blob/master/slides/dmmq-summit-2014.pdf?raw=true">Discardable, in-memory materialized query for Hadoop</a> (<a href="https://www.youtube.com/watch?v=CziGOa8GXqI">video</a>) (Hadoop Summit, 2014)
* <a href="https://github.com/julianhyde/share/blob/master/slides/hive-cbo-summit-2014.pdf?raw=true">Cost-based optimization in Hive</a> (<a href="https://www.youtube.com/watch?v=vpG5noIbEFs">video</a>) (Hadoop Summit, 2014)
* <a href="https://github.com/julianhyde/share/blob/master/slides/optiq-nosql-now-2013.pdf?raw=true">SQL Now!</a> (NoSQL Now! conference, 2013)
* <a href="https://github.com/julianhyde/share/blob/master/slides/optiq-richrelevance-2013.pdf?raw=true">SQL on Big Data using Optiq</a> (2013)
* <a href="https://github.com/julianhyde/share/blob/master/slides/optiq-drill-user-group-2013.pdf?raw=true">Drill / SQL / Optiq</a> (2013)
* <a href="http://www.slideshare.net/julianhyde/how-to-integrate-splunk-with-any-data-solution">How to integrate Splunk with any data solution</a> (Splunk User Conference, 2012)
