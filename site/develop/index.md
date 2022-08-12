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

Prerequisites are Git,
and Java (JDK 8u220 or later, 11 preferred) on your path.

Note: early OpenJDK 1.8 versions (e.g. versions before 1.8u202) are known to have issues with
producing bytecode for type annotations (see [JDK-8187805](https://bugs.openjdk.java.net/browse/JDK-8187805),
[JDK-8187805](https://bugs.openjdk.java.net/browse/JDK-8187805),
[JDK-8210273](https://bugs.openjdk.java.net/browse/JDK-8210273),
[JDK-8160928](https://bugs.openjdk.java.net/browse/JDK-8160928),
[JDK-8144185](https://bugs.openjdk.java.net/browse/JDK-8144185) ), so make sure you use up to date Java.

Create a local copy of the Git repository, `cd` to its root directory,
then build using Gradle:

{% highlight bash %}
$ git clone git://github.com/apache/calcite.git
$ cd calcite
$ ./gradlew build
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
`./gradlew build` succeeds. (Run extra tests if your change warrants it.)

Commit your change to your branch, and use a comment that starts with
the JIRA case number, like this:

{% highlight text %}
[CALCITE-345] AssertionError in RexToLixTranslator comparing to date literal
{% endhighlight %}

If your change had multiple commits, use `git rebase -i main` to
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
 * Leave a single space character after the JIRA id.
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

Then push your commit(s) to GitHub, and create a pull request from
your branch to the calcite main branch. Update the JIRA case
to reference your pull request, and a committer will review your
changes.

The pull request may need to be updated (after its submission) for three main
reasons:
1. you identified a problem after the submission of the pull request;
2. the reviewer requested further changes;
3. the CI build failed, and the failure is not caused by your changes.

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

In the special case, that the CI build failed, and the failure is not
caused by your changes create an empty commit (`git commit --allow-empty`) and
push it.

## Null safety

Apache Calcite uses the Checker Framework to avoid unexpected `NullPointerExceptions`.
You might find a detailed documentation at https://checkerframework.org/

Note: only main code is verified for now, so nullness annotation is not enforced in test code.

To execute the Checker Framework locally please use the following command:

    ./gradlew -PenableCheckerframework :linq4j:classes :core:classes

Here's a small introduction to null-safe programming:

* By default, parameters, return values and fields are non-nullable, so refrain from using `@NonNull`
* Local variables infer nullness from the expression, so you can write `Object v = ...` instead of `@Nullable Object v = ...`
* Avoid the use of `javax.annotation.*` annotations. The annotations from `jsr305` do not support cases like `List<@Nullable String>`
so it is better to stick with `org.checkerframework.checker.nullness.qual.Nullable`.
  Unfortunately, Guava (as of `29-jre`) has **both** `jsr305` and `checker-qual` dependencies at the same time,
  so you might want to configure your IDE to exclude `javax.annotation.*` annotations from code completion.

* The Checker Framework verifies code method by method. That means, it can't account for method execution order.
  That is why `@Nullable` fields should be verified in each method where they are used.
  If you split logic into multiple methods, you might want verify null once, then pass it via non-nullable parameters.
  For fields that start as null and become non-null later, use `@MonotonicNonNull`.
  For fields that have already been checked against null, use `@RequiresNonNull`.

* If you are absolutely sure the value is non-null, you might use `org.apache.calcite.linq4j.Nullness.castNonNull(T)`.
  The intention behind `castNonNull` is like `trustMeThisIsNeverNullHoweverTheVerifierCantTellYet(...)`

* If the expression is nullable, however, you need to pass it to a non-null method, use `Objects.requireNonNull`.
  It allows to have a better error message that includes context information.

* The Checker Framework comes with an annotated JDK, however, there might be invalid annotations.
  In that cases, stub files can be placed to `/src/main/config/checkerframework` to override the annotations.
  It is important the files have `.astub` extension otherwise they will be ignored.

* In array types, a type annotation appears immediately before the type component (either the array or the array component) it refers to.
  This is explained in the [Java Language Specification](https://docs.oracle.com/javase/specs/jls/se8/html/jls-9.html#jls-9.7.4).

        String nonNullable;
        @Nullable String nullable;

        java.lang.@Nullable String fullyQualifiedNullable;

        // array and elements: non-nullable
        String[] x;

        // array: nullable, elements: non-nullable
        String @Nullable [] x;

        // array: non-nullable, elements: nullable
        @Nullable String[] x;

        // array: nullable, elements: nullable
        @Nullable String @Nullable [] x;

        // arrays: nullable, elements: nullable
        // x: non-nullable
        // x[0]: non-nullable
        // x[0][0]: nullable
        @Nullable String[][] x;

        // x: nullable
        // x[0]: non-nullable
        // x[0][0]: non-nullable
        String @Nullable [][] x;

        // x: non-nullable
        // x[0]: nullable
        // x[0][0]: non-nullable
        String[] @Nullable [] x;

* By default, generic parameters can be both nullable and non-nullable:

        class Holder<T> { // can be both nullable
            final T value;
            T get() {
                return value; // works
            }
            int hashCode() {
                return value.hashCode(); // error here since T can be nullable
            }

* However, default bounds are non-nullable, so if you write `<T extends Number>`,
  then it is the same as `<T extends @NonNull Number>`.

        class Holder<T extends Number> { // note how this T never permits nulls
            final T value;
            Holder(T value) {
                this.value = value;
            }
            static <T> Holder<T> empty() {
                return new Holder<>(null); // fails since T must be non-nullable
            }

* If you need "either nullable or non-nullable `Number`", then use `<T extends @Nullable Number>`,

* If you need to ensure the type is **always** nullable, then use `<@Nullable T>` as follows:

        class Holder<@Nullable T> { // note how this requires T to always be nullable
            protected T get() { // Default implementation.
                // Default implementation returns null, so it requires that T must always be nullable
                return null;
            }
            static void useHolder() {
                // T is declared as <@Nullable T>, so Holder<String> would not compile
                Holder<@Nullable String> holder = ...;
                String value = holder.get();
            }

## Continuous Integration Testing

Calcite exploits [GitHub actions](https://github.com/apache/calcite/actions?query=branch%3Amain)
and [Travis](https://app.travis-ci.com/github/apache/calcite) for continuous integration testing.
In the past, there were also Jenkins jobs on the [ASF-hosted](https://builds.apache.org/)
infrastructure, but they are not maintained anymore.

## Getting started

Calcite is a community, so the first step to joining the project is to introduce yourself.
Join the [developers list](https://mail-archives.apache.org/mod_mbox/calcite-dev/)
and send an email.

If you have the chance to attend a [meetup](https://www.meetup.com/Apache-Calcite/),
or meet [members of the community](https://calcite.apache.org/develop/#project-members)
at a conference, that's also great.

Choose an initial task to work on. It should be something really simple,
such as a bug fix or a [Jira task that we have labeled
"newbie"](https://issues.apache.org/jira/issues/?jql=labels%20%3D%20newbie%20%26%20project%20%3D%20Calcite%20%26%20status%20%3D%20Open).
Follow the [contributing guidelines](#contributing) to get your change committed.

We value all contributions that help to build a vibrant community, not just code.
You can contribute by testing the code, helping verify a release, writing documentation, improving
the web site, or just by answering questions on the list.

After you have made several useful contributions we may invite you to become a [committer](https://community.apache.org/contributors/).
The most common way of becoming a committer is by contributing regularly to the project. In some
exceptional cases you can bypass the invitation process as described below.

If you are a committer in another **ASF project** (such as Drill, Flink, Hive, Ignite, Phoenix, etc.)
and you are familiar with the Calcite codebase you can request explicitly from the Calcite PMC to
consider your for committership. You can do that by sending an email to [private@calcite.apache.org](mailto:private@calcite.apache.org)
including the following information (template below):
* Apache ID, you must have one if you are committer;
* links to commits, discussions, presentations, blogs, etc., demonstrating your experience with
Calcite.

{% highlight text %}
Subject: [REQUEST] New committer: Stamatis Zampetakis
To: private@calcite.apache.org

Hi all,

My name is Stamatis Zampetakis and my Apache ID is zabetak:
https://home.apache.org/phonebook.html?uid=zabetak

I am a Hive committer and have been working with Calcite for the past 2 years.
I contributed various improvements and fixes in the cost based optimizer of
Hive, which relies on Calcite, and I am pretty familiar with the Calcite
codebase. Below you can find a list of contributions in the Hive repo that are
related with Calcite:

* https://github.com/apache/hive/commit/f29cb2245c97102975ea0dd73783049eaa0947a0
* https://github.com/apache/hive/commit/efae863fe010ed5c4b7de1874a336ed93b3c60b8
* https://github.com/apache/hive/commit/587c698fa25ca6da46d9c02e4199689426fec40f
* https://github.com/apache/hive/commit/9087fa93cd785223f4f2552ec836e7580c78830a
* https://github.com/apache/hive/commit/0616bcaa2436ccbf388b635bfea160b47849553c
* https://github.com/apache/hive/commit/6f7c55ab9bc4fd7c3d0c2a6ba3095275b17b3d2d
* https://github.com/apache/hive/commit/a0faf5ecb196a20cfef64d554df54961e8c074a7
* https://github.com/apache/hive/commit/4f4cbeda00d5ebb7d0b8cedee5daa2c03df4a755
* https://github.com/apache/hive/commit/6f2b8883c44edcf57538f3b1da2c5a599b0c5862
* https://github.com/apache/hive/commit/170d5b4c3edf2daaa47ef3299277d44def4d39a7

I would like to become a Calcite committer and help the project as much as I can.

Best,
Stamatis

{% endhighlight %}

After receiving your email the PMC will evaluate your request and get back to you in 1-2 weeks
(usually a vote for adding a new committer takes ~7days).
