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

This module contains a number of Dockerfiles to ease testing of
Avatica clients against a known-server.

## Docker

`src/main/docker` contains a number of Dockerfiles and Docker-compose
configuration files to launch a standalone-Avatica server. Maven automation
exists for the base Docker image "avatica-server" which can be invoked with
the "-Pdocker" Maven profile.

The other Dockerfiles must be built by hand.

### Provided Images

A number of Dockerfiles for different databases are provided. Presently, they include:

* [HyperSQL](https://github.com/apache/calcite-avatica/tree/master/docker/src/main/docker/hypersql)
* [MySQL](https://github.com/apache/calcite-avatica/tree/master/docker/src/main/docker/mysql)
* [PostgreSQL](https://github.com/apache/calcite-avatica/tree/master/docker/src/main/docker/postgresql)

## Dockerhub

`src/main/dockerhub` contains a copy of the same `avatica-server` Dockerfile
that is present in `src/main/docker` that is designed to be used with the
automation around publishing Docker images to the Apache Dockerhub account.

It is not expected that users would interact with this Dockerfile.
