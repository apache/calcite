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
[Apache](https://gitbox.apache.org/repos/asf/calcite.git),
but most people find the
[Github mirror](https://github.com/apache/calcite) more
user-friendly.

## Download source, build, and run tests

Prerequisites are git, maven (3.5.2 or later)
and Java (JDK 8 or later, 9 preferred) on your path.

Create a local copy of the git repository, `cd` to its root directory,
then build using maven:

{% highlight bash %}
$ git clone git://github.com/apache/calcite.git
$ cd calcite
$ mvn install
{% endhighlight %}

The HOWTO describes how to
[build from a source distribution]({{ site.baseurl }}/docs/howto.html#building-from-a-source-distribution),
[set up an IDE for contributing]({{ site.baseurl }}/docs/howto.html#setting-up-an-ide-for-contributing),
[run more or fewer tests]({{ site.baseurl }}/docs/howto.html#running-tests) and
[run integration tests]({{ site.baseurl }}/docs/howto.html#running-integration-tests).

## Contributing

We welcome contributions.

If you are planning to make a large contribution, talk to us first! It
helps to agree on the general approach. Log a
[JIRA case](https://issues.apache.org/jira/browse/CALCITE) for your
proposed feature or start a discussion on the dev list.

Before opening up a new JIRA case, have a look in the existing issues.
The feature or bug that you plan to work on may already be there.

If a new issue needs to be created, it is important to provide a
concise and meaningful summary line. It should imply what the end user
was trying to do, in which component, and what symptoms were seen.
If it's not clear what the desired behavior is, rephrase: e.g.,
"Validator closes model file" to "Validator should not close model file".

Contributors to the case should feel free to rephrase and clarify the
summary line. If you remove information while clarifying, put it in
the description of the case.

Design discussions may happen in various places (email threads,
github reviews) but the JIRA case is the canonical place for those
discussions. Link to them or summarize them in the case.

When implementing a case, especially a new feature, make sure
the case includes a functional specification of the change. For instance,
"Add a IF NOT EXISTS clause to the CREATE TABLE command; the command is
a no-op if the table already exists." Update the description if
the specification changes during design discussions or implementation.

When implementing a feature or fixing a bug, endeavor to create
the jira case before you start work on the code. This gives others
the opportunity to shape the feature before you have gone too far down
(what the reviewer considers to be) the wrong path.

The best place to ask for feedback related to an issue is the developers list.
Please avoid tagging specific people in the JIRA case asking for feedback.
This discourages other contributors to participate in the discussion and
provide valuable feedback.

If there is a regression that seems to be related with a particular commit,
feel free to tag the respective contributor(s) in the discussion.

If you are going to take on the issue right away assign it to yourself.
To assign issues to yourself you have to be registered in JIRA as a contributor.
In order to do that, send an email to the developers list
and provide your JIRA username.

If you are committed to fixing the issue before the upcoming release set
the fix version accordingly (e.g., 1.20.0), otherwise leave it as blank.

If you pick up an existing issue, mark it 'in progress', and when it's
finished flag it with 'pull-request-available'.

If for any reason you decide that an issue cannot go into the ongoing
release, reset the fix version to blank.

During a release, the release manager will update the issues that were
not completed for the current release to the next release.

There are cases where the JIRA issue may be solved in the discussion
(or some other reason) without necessitating a change. In such cases,
the contributor(s) involved in the discussion should:
 * resolve the issue (do not close it);
 * select the appropriate resolution cause ("Duplicate", "Invalid", "Won't fix", etc.);
 * add a comment with the reasoning if that's not obvious.

Fork the GitHub repository, and create a branch for your feature.

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

In order to keep the commit history clean and uniform, you should
respect the following guidelines.
 * Read the messages of previous commits, and follow their style.
 * The first line of the commit message must be a concise and useful
description of the change.
 * The message is often, but not always, the same as the JIRA subject.
If the JIRA subject is not clear, change it (perhaps move the original
subject to the description of the JIRA case, if it clarifies).
 * Start with a capital letter.
 * Do not finish with a period.
 * Use imperative mood ("Add a handler ...") rather than past tense
("Added a handler ...") or present tense ("Adds a handler ...").
 * If possible, describe the user-visible behavior that you changed
("FooCommand now creates directory if it does not exist"), rather than
the implementation ("Add handler for FileNotFound").
 * If you are fixing a bug, it is sufficient to describe the bug
 ("NullPointerException if user is unknown") and people will correctly
 surmise that the purpose of your change is to fix the bug.
 * If you are not a committer, add your name in parentheses at the end
 of the message.

Then push your commit(s) to GitHub, and create a pull request from
your branch to the calcite master branch. Update the JIRA case
to reference your pull request, and a committer will review your
changes.

The pull request may need to be updated (after its submission) for three main
reasons:
1. you identified a problem after the submission of the pull request;
2. the reviewer requested further changes;
3. the Travis CI build failed and the failure is not caused by your changes.

In order to update the pull request, you need to commit the changes in your
branch and then push the commit(s) to GitHub. You are encouraged to use regular
 (non-rebased) commits on top of previously existing ones.

When pushing the changes to GitHub, you should refrain from using the `--force`
parameter and its alternatives. You may choose to force push your changes under
 certain conditions:
 * the pull request has been submitted less than 10 minutes ago and there is no
 pending discussion (in the PR and/or in JIRA) concerning it;
 * a reviewer has explicitly asked you to perform some modifications that
 require the use of the `--force` option.

In the special case, that the Travis CI build failed and the failure is not
caused by your changes create an empty commit (`git commit --allow-empty`) and
push it.

## Continuous Integration Testing

Calcite has a collection of Jenkins jobs on ASF-hosted infrastructure.
They are all organized in a single view and available at
[https://builds.apache.org/view/A-D/view/Calcite/](https://builds.apache.org/view/A-D/view/Calcite/).

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

