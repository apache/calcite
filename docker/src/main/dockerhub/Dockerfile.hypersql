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

ARG AVATICA_VERSION

FROM apache/calcite-avatica:$AVATICA_VERSION
MAINTAINER Apache Avatica <dev@calcite.apache.org>

ARG HSQLDB_VERSION="2.4.1"

# Dependencies
ADD https://repo1.maven.org/maven2/net/hydromatic/scott-data-hsqldb/0.1/scott-data-hsqldb-0.1.jar /home/avatica/classpath/
ADD https://repo1.maven.org/maven2/org/hsqldb/hsqldb/${HSQLDB_VERSION}/hsqldb-${HSQLDB_VERSION}.jar /home/avatica/classpath/

# Add on to avatica-server's entrypoint
CMD ["-u", "jdbc:hsqldb:res:scott"]

# End Dockerfile
