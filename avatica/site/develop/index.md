---
layout: page
title: Developing Calcite
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

Want to help add a feature or fix a bug?

* TOC
{:toc}

## Source code

You can get the source code by
[downloading a release]({{ site.baseurl }}/downloads)
or from source control.

Calcite uses git for version control.  The canonical source is in
[Apache](https://git-wip-us.apache.org/repos/asf/calcite.git),
but most people find the
[Github mirror](https://github.com/apache/calcite) more
user-friendly.

## Download source, build, and run tests

Prerequisites are git, maven (3.2.1 or later) and Java (JDK 1.7 or
later, 1.8 preferred) on your path.

Create a local copy of the git repository, `cd` to its root directory,
then build using maven:

{% highlight bash %}
$ git clone git://github.com/apache/calcite.git
$ cd calcite/avatica
$ mvn install
{% endhighlight %}

The HOWTO describes how to
[build from a source distribution]({{ site.baseurl }}/docs/howto.html#building-from-a-source-distribution),
[run more or fewer tests]({{ site.baseurl }}/docs/howto.html#running-tests) and
[run integration tests]({{ site.baseurl }}/docs/howto.html#running-integration-tests).

## Contributing

We welcome contributions.

If you are planning to make a large contribution, talk to us first! It
helps to agree on the general approach. Log a
[JIRA case](https://issues.apache.org/jira/browse/CALCITE) for your
proposed feature or start a discussion on the dev list.

Fork the github repository, and create a branch for your feature.

Develop your feature and test cases, and make sure that
`mvn install` succeeds. (Run extra tests if your change warrants it.)

Commit your change to your branch, and use a comment that starts with
the JIRA case number, like this:

{% highlight text %}
[CALCITE-345] AssertionError in RexToLixTranslator comparing to date literal
{% endhighlight %}

If your change had multiple commits, use `git rebase -i master` to
squash them into a single commit, and to bring your code up to date
with the latest on the main line.

Then push your commit(s) to github, and create a pull request from
your branch to the calcite master branch. Update the JIRA case
to reference your pull request, and a committer will review your
changes.

## Continuous Integration Testing

Calcite has a collection of Jenkins jobs on ASF-hosted infrastructure.
They are all organized in a single view and available at
[https://builds.apache.org/view/A-D/view/Calcite-Avatica/](https://builds.apache.org/view/A-D/view/Calcite-Avatica/).

## Getting started

Calcite is a community, so the first step to joining the project is to introduce yourself.
Join the [developers list](http://mail-archives.apache.org/mod_mbox/calcite-dev/)
and send an email.

If you have the chance to attend a [meetup](http://www.meetup.com/Apache-Calcite/),
or meet [members of the community](http://calcite.apache.org/develop/#project-members)
at a conference, that's also great.

Choose an initial task to work on. It should be something really simple,
such as a bug fix or a [Jira task that we have labeled
"newbie"](https://issues.apache.org/jira/issues/?jql=labels%20%3D%20newbie%20%26%20project%20%3D%20Calcite%20%26%20status%20%3D%20Open).
Follow the [contributing guidelines](#contributing) to get your change committed.

After you have made several useful contributions we may
[invite you to become a committer](https://community.apache.org/contributors/).
We value all contributions that help to build a vibrant community, not just code.
You can contribute by testing the code, helping verify a release,
writing documentation or the web site,
or just by answering questions on the list.
