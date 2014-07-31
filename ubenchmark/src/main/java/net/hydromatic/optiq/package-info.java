/*
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
/**
 * Microbenchmarks to test optiq performance.
 * The way to run is
 * {@code mvn package && java -jar ./target/ubenchmarks.jar -wi 5 -i 5 -f 1}.
 * <p>
 * To run with profiling, use {@code java -Djmh.stack.lines=10 -jar
 * ./target/ubenchmarks.jar -prof hs_comp,hs_gc,stack -f 1 -wi 5}.
 */
package net.hydromatic.optiq;
