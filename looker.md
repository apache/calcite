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
# Looker's branch of Calcite

This document describes how to develop Looker's branch of Calcite.

Do not merge to Calcite's master branch.

## Development

*Read the instructions* in the `looker-release.sh` script,
but do not run that script until you're ready to publish a production release.

Looker has poor infrastructure for testing with local builds of Calcite.
The easiest way is to upload a snapshot version to Looker's Artifact Registry repository and use it.
To upload a snapshot version, simply run `./looker-snapshot.sh`,
which runs `./gradlew build` and, if successful,
uploads the resulting snapshot artifacts to the repo
using the version number configured in `gradle.properties` (plus a "-SNAPSHOT" suffix).

Then, use that snapshot version in Looker's `internal/repositories/maven_deps.bzl` and repin,
and you're ready to build.

## Release


In the above fragment, replace the values of the `username` and
`password` properties with the secret credentials.

*NOTE* This fragment *must* be in a file outside of your git sandbox.
If the file were in the git sandbox, it would be too easy to
accidentally commit the secret credentials and expose them on a
public site.

*Read the instructions* in the `looker-release.sh` script, then run it.
The script will only make local changes.
You'll have a chance to review them before pushing anything to Artifact Registry or GitHub.
Upload to Artifact Registry by running `looker-upload-artifact-registry.sh` after checking the release looks right.

Each Looker release will have a name like `1.38.1-looker` (if the most
recent official Calcite release that it is based off of is `1.38`) and have a git tag
`calcite-1.38.1-looker`.

You should make it from a branch that differs from Calcite's
`master` branch in only minor ways:
* Cherry-pick commits from the previous `calcite-x.xx.x-looker`
  release that set up Looker's repositories (or, if you prefer,
  rebase the previous release branch onto the latest master)
* If necessary, include one or two commits for short-term fixes, but
  log [JIRA cases](https://issues.apache.org/jira/browse/CALCITE) to
  get them into `master`.
* Keep fixup commits coherent (squash incremental commits)
  and to a minimum.
* The final commit for a release must only set the version
  in `gradle.properties`, and it must be the only such commit
  that differs from Calcite trunk. Note that Calcite increments
  the version in trunk immediately after each release, so
  that version is typically unreleased. The Looker version
  should be named after the most recent Calcite release,
  so the version in trunk is generally decremented
  while adding the `-looker` suffix.

Check the artifacts on Artifact Registry.
