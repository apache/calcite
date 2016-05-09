#!/usr/bin/env ruby

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
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

require 'logger'
require 'yaml'

PASSED = "\033[32mPassed\033[0m"
FAILED = "\033[31mFailed\033[0m"

VERSIONS_KEY = 'versions'
CLIENT_JAR_KEY = 'client_jar'
SERVER_URL_KEY = 'server_url'
CLIENT_URL_TEMPLATE_KEY = 'client_url_template'

TCK_JAR_KEY = 'tck_jar'

# Special key for the client URL template to substitute the server's url
URL_REPLACEMENT_HOLDER = '<url>'

LOG = Logger.new(STDOUT)
LOG.level = Logger::WARN


def usage()
  puts "usage: test_runner.rb configuration.yml"
end

def run_test(tck_jar, client, server)
  client_jar = client[:config][CLIENT_JAR_KEY]
  client_url = client[:config][CLIENT_URL_TEMPLATE_KEY]
  server_url = server[:config][SERVER_URL_KEY]

  puts "\nRunning #{client[:name]} against #{server[:name]}"

  if server_url.end_with? 'X'
    LOG.error("Fill in the port for server version '#{server_url}' in the YAML configuration file")
    return false
  end

  # Make a copy here to be sure to not affect other tests
  LOG.debug("Updating server url #{server_url} in #{client_url}")
  client_url = client_url.gsub(URL_REPLACEMENT_HOLDER, server_url)

  cmd = "java -cp '#{tck_jar}:#{client_jar}' org.apache.calcite.avatica.tck.TestRunner -u '#{client_url}'"
  puts "Java command: '#{cmd}'"
  success = system(cmd)

  puts "Test of #{client[:name]} against #{server[:name]} " + (success ? PASSED : FAILED)
  return success
end

unless ARGV.size == 1
  usage()
  raise ArgumentError.new('YAML configuration file is required as the only argument')
end

# Parse the configuration file
config_file = ARGV[0]
config = YAML.load_file(config_file)

# The Avatica TCK jar
tck_jar = config[TCK_JAR_KEY]
if tck_jar.nil?
  raise "Configuration file does not contain '#{TCK_JAR_KEY}' key"
end

# A client url template specified globally, can be overriden in the version
global_url_template = config[CLIENT_URL_TEMPLATE_KEY]

# Map of version name to jar/configuration
all_versions = config[VERSIONS_KEY]
if all_versions.nil?
  raise "Configuration file does not contain '#{VERSIONS_KEY}' key"
end

# Push down the global client url template to each version when applicable
all_versions.each do |version|
  if version.length != 2
    LOG.warn("Unexpected number of arguments for version: #{version.to_s}")
  end
  version_config = version[1]
  if version_config[CLIENT_URL_TEMPLATE_KEY].nil? and not global_url_template.nil?
    version_config[CLIENT_URL_TEMPLATE_KEY] = global_url_template
  end
end

# Convert from a hash to an array of pairs
all_versions = all_versions.collect{ |k,v| {:name=>k, :config=>v} }

# Compute the "identity" mapping as a sanity check
identity_versions = all_versions.collect {|x| [x, x]}

# Create a cartesian product of the pairs, dropping entries where the pairs are equal
all_pairs = all_versions.product(all_versions).select {|x| x[0][:name] != x[1][:name]}

puts "Running identity test as a sanity check (client and server at the same version)"

identity_outcomes = identity_versions.collect{|pair| {:client => pair[0][:name], :server=>pair[1][:name], :result=>run_test(tck_jar, pair[0], pair[1])}}

puts "\nRunning cross-version tests"

# Run the TCK against each pair
outcomes = all_pairs.collect{|pair| {:client=>pair[0][:name], :server=>pair[1][:name], :result=>run_test(tck_jar, pair[0], pair[1])}}

puts "\n-------------------------------------\nSummary:\n\n"

puts "Identity test scenarios (ran #{identity_outcomes.size})\n\n"
identity_outcomes.each{|outcome| puts "Testing identity for version #{outcome[:client]}: #{(outcome[:result] ? PASSED : FAILED)}"}

puts "\nAll test scenarios (ran #{all_pairs.size})\n\n"
outcomes.each{|outcome| puts "Testing client #{outcome[:client]} against server #{outcome[:server]}: #{(outcome[:result] ? PASSED : FAILED)}"}
