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

# Apache Calcite Avatica site

This directory contains the code for the
[Avatica web site](https://calcite.apache.org/avatica),
a sub-directory of the
[Apache Calcite web site](https://calcite.apache.org).

You can build the site manually using your environment or use the docker compose file.

## Manually

### Setup your environment

Similar to the instructions to
[set up the Calcite web site](https://github.com/apache/calcite-avatica/blob/master/site/README.md).

Site generation currently works best with ruby-2.5.1.

1. `cd site`
2. `svn co https://svn.apache.org/repos/asf/calcite/site/avatica target/avatica`
3. `sudo apt-get install rubygems ruby2.5-dev zlib1g-dev` (linux)
4. `sudo gem install bundler`
5. Add avatica-go content: `./add-avatica-go-docs.sh`
6. `bundle install`

### Add javadoc

1. `cd avatica`
2. `./mvnw -DskipTests site`
3. `rm -rf site/target/avatica/apidocs site/target/avatica/testapidocs`
4. `mv target/site/apidocs target/site/testapidocs site/target/avatica`

### Running locally

Before opening a pull request, you can preview your contributions by
running from within the directory:

1. `bundle exec jekyll serve`
2. Open [http://localhost:4000/avatica/](http://localhost:4000/avatica/) (note the trailing slash)

## Using docker

### Setup your environment

1. Install [docker](https://docs.docker.com/install/)
2. Install [docker-compose](https://docs.docker.com/compose/install/)

### Build site
1. `cd site`
2. `docker-compose run build-site`

### Generate javadoc
1. `cd site`
2. `docker-compose run generate-javadoc`

### Running development mode locally
You can preview your work while working on the site.

1. `cd site`
2. `docker-compose run --service-ports dev`

The web server will be started on [http://localhost:4000/avatica/](http://localhost:4000/avatica/) (note the trailing slash)

As you make changes to the site, the site will automatically rebuild.

## Pushing to site

Push the Calcite site, which includes `avatica` as a sub-directory,
as described in its
[README](../site/README.md).
