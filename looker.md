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

For development, build and deploy to your local Maven repository:

```
cd calcite
./gradlew publishToMavenLocal
```

On the HT side, edit `looker-libs/build.gradle`:

```
    compile "org.apache.calcite:calcite-core:1.xx.x-SNAPSHOT"
    compile "org.apache.calcite:calcite-babel:1.xx.x-SNAPSHOT"
```

## Release

Release will have a name like `1.21.1-looker` (if the most
recent official Calcite release is `1.21`) and have a git tag
`calcite-1.21.1-looker`.

You should make it from a branch that differs from Calcite's
`master` branch in only minor ways:
* Cherry-pick commits from the previous `calcite-x.xx.x-looker`
  release that set up Looker's repositories (or, if you prefer,
  rebase the previous release branch onto the latest master)
* If necessary, include one or two commits for short-term fixes, but
  log [JIRA cases](https://issues.apache.org/jira/browse/CALCITE) to
  get them into `master`.

In Calcite's `gradle.properties`, update the value of
`calcite.version` to the release name (something like
`1.22.1-looker`) and commit.

Define Looker's Nexus repository in your `~/.gradle/init.gradle.kts`
file:

```kotlin
allprojects {
    plugins.withId("maven-publish") {
        configure<PublishingExtension> {
            repositories {
                maven {
                    name = "lookerNexus"
                    val baseUrl = "https://nexusrepo.looker.com"
                    val releasesUrl = "$baseUrl/repository/maven-releases"
                    val snapshotsUrl = "$baseUrl/repository/maven-snapshots"
                    val release = !project.version.toString().endsWith("-SNAPSHOT")
                    // val release = project.hasProperty("release")
                    url = uri(if (release) releasesUrl else snapshotsUrl)
                    credentials {
                        username = "xxx"
                        password = "xxx"
                    }
                }
            }
        }
    }
}
```

In the above fragment, replace the values of the `username` and
`password` properties with the secret credentials.

*NOTE* This fragment *must* be in a file outside of your git sandbox.
If the file were in the git sandbox, it would be too easy to
accidentally commit the secret credentials and expose them on a
public site.

Publish:
```sh
./gradlew -Prelease -PskipSign publishAllPublicationsToLookerNexusRepository
```

Check the artifacts
[on Nexus](https://nexusproxy.looker.com/#browse/search=keyword%3Dorg.apache.calcite).

If the release was successful, tag the release and push the tag:
```sh
git tag calcite-1.21.1-looker HEAD
git push julianhyde calcite-1.21.1-looker
```
