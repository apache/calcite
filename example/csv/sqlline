#!/bin/bash
# sqlline - Script to launch SQL shell
#
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
#
# Example:
# $ ./sqlline
# sqlline> !connect jdbc:calcite:model=target/test-classes/model.json admin admin

# Deduce whether we are running cygwin
case $(uname -s) in
(CYGWIN*) cygwin=true;;
(*) cygwin=;;
esac

# Build classpath on first call. (To force rebuild, remove .classpath.txt.)
cd $(dirname $0)
if [ ! -f target/classpath.txt ]; then
    mvn dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
fi

CP="target/classes:target/test-classes:$(cat target/classpath.txt)"
VM_OPTS=
if [ "$cygwin" ]; then
  CP=$(cygpath -wp "$CP")

  # Work around https://github.com/jline/jline2/issues/62
  VM_OPTS=-Djline.terminal=jline.UnixTerminal
fi

exec java $VM_OPTS -cp "${CP}" sqlline.SqlLine "$@"

# End sqlline
