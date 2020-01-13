#!/bin/bash
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

# Deduce whether we are running cygwin
case $(uname -s) in
(CYGWIN*) cygwin=true;;
(*) cygwin=;;
esac

# Jump to the project's root path
#cd ../../../
cd $(dirname $0)/../../../

# Set environment variables to control test execution
export RedisMiniServerEnabled=true

serverName="org.apache.calcite.adapter.redis.RedisMiniServer"
pid=`ps -ef| grep ${serverName}|lsof -i:6379|awk '{print $2}'|uniq`
if [[ -n "$pid" ]]
then
  echo "RedisServerMini is running, $pid"
else
  ../gradlew :redis:cleanTest :redis:test --tests ${serverName}
  echo "RedisServerMini is start!"
  exit -1
fi

unset RedisServerMiniEnabled

# End start.sh
