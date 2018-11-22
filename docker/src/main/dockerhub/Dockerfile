# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM openjdk:8-jre-alpine
MAINTAINER Apache Avatica <dev@calcite.apache.org>

# Create an avatica user
RUN addgroup -S avatica && adduser -S -G avatica avatica
RUN mkdir -p /home/avatica/classpath

ARG AVATICA_VERSION

# Dependencies
ADD https://repository.apache.org/content/groups/public/org/apache/calcite/avatica/avatica-standalone-server/${AVATICA_VERSION}/avatica-standalone-server-${AVATICA_VERSION}-shaded.jar /home/avatica/classpath

# Make sure avatica owns its files
RUN chown -R avatica: /home/avatica

# Expose the default port as a convenience
EXPOSE 8765

# TODO Would like to do this, but screws up downstream due to https://github.com/docker/docker/issues/6119
# USER avatica

ENTRYPOINT ["/usr/bin/java", "-cp", "/home/avatica/classpath/*", "org.apache.calcite.avatica.standalone.StandaloneServer", "-p", "8765"]
