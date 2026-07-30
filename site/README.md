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

# Apache Calcite docs site

This directory contains the sources/templates for generating the Apache Calcite website,
[calcite.apache.org](https://calcite.apache.org/). The actual generated content of the website
is present in the [calcite-site](https://github.com/apache/calcite-site) repository.

# Previewing the website locally using docker
## Setup your environment

1. Install [docker](https://docs.docker.com/install/)
2. Install [docker compose](https://docs.docker.com/compose/install/)

## Build site

1. `cd site`
2. `docker compose run build-site`

## Generate javadoc

1. `cd site`
2. `docker compose run generate-javadoc`

## Running development mode locally

You can preview your work while working on the site.

1. `cd site`
2. `docker compose run --service-ports dev`

The web server will be started on [http://localhost:4000](http://localhost:4000)

As you make changes to the site, the site will automatically rebuild.

# Publishing the website

We want to deploy project changes (for example, new committers, PMC members or upcoming talks)
immediately, but we want to deploy documentation of project features only when that feature appears
in a release.

Calcite publishes the website automatically since [CALCITE-3129](https://issues.apache.org/jira/browse/CALCITE-3129),
you do not need to do anything but just merge your changes to the `main` branch,
Github workflows will identify changes to website and automatically cherry-pick it to the `site` branch,
compile and publish it to [calcite-site](https://github.com/apache/calcite-site) repo.

## Non-release publishing

We'll publish the website changes such as community member changes and new blogs immediately after merging.
The rules and scripts are in `.github/workflows/publish-non-release-website-updates.yml`.

## Release publishing

We identify release publishing by checking new release tags. If you are the Release Manager,
you only need to push the new tag 'calcite-x.y.z' to [Calcite Github repo](https://github.com/apache/calcite),
and the Github workflow will do all the rest.
The rules and scripts are in `.github/workflows/publish-website-on-release.yml`.

## Legacy method: manually pushing to site

This should normally **not** be required any more, but in case the automatic publication fails,
these are the (legacy) steps to manually publish the site:
- Push the commit with the changes to the `main` branch of this repository.
- Cherry-pick the commit from the `main` branch to the `site` branch of this repository.
- Checkout the `site` branch and build the website using [docker-compose](#previewing-the-website-locally-using-docker).
- Commit the generated content to the `main` branch of the `calcite-site` repository following these steps:

1. `cd site/target`
2. `git init`
3. `git remote add origin git@github.com:apache/calcite-site.git`
4. `git fetch`
5. `git reset origin/main --soft`

If you have not regenerated the javadoc (you shouldn't unless you are publishing a new release)
and they are missing, restore them:

6. `git reset -- javadocAggregate/`
7. `git checkout -- javadocAggregate/`

Restore the avatica site

8. `git reset -- avatica/`
9. `git checkout -- avatica/`

Push the changes
10. `git add .`
11. Commit: `git commit -m "Your commit message goes here"`
12. Push the site: `git push origin main`

Within a few minutes, gitpubsub should kick in and you'll be able to
see the results at
[calcite.apache.org](https://calcite.apache.org/).

This process also publishes Avatica's web site. Avatica's web site has
separate source (under `avatica/site`) but configures Jekyll to
generate files to `site/target/avatica`, which becomes an
[avatica](https://calcite.apache.org/avatica)
sub-directory when deployed. See
[Avatica site README](https://github.com/apache/calcite-avatica/blob/main/site/README.md).
