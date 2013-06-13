/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.test.concurrent;

import java.util.ArrayList;

/**
 * Used to extend functionality of mtsql.
 */
public abstract class ConcurrentTestPlugin
{

    /**
     * Should containing test be disabled?
     *
     * @return true if containing test should be disabled
     */
    public boolean isTestDisabled() {
        return false;
    }

    /**
     * What commands are supported by this plugin within
     * a thread or repeat section. Commands should start with '@'.
     *
     * @return List of supported commands
     */
    public Iterable<String> getSupportedThreadCommands() {
        return new ArrayList<String>();
    }

    /**
     * What commands are supported by this plugin before
     * the setup section. Commands should start with '@'.
     *
     * @return List of supported commands
     */
    public Iterable<String> getSupportedPreSetupCommands() {
        return new ArrayList<String>();
    }

    /**
     *  Create and return plugin command for given name.
     * @param name Name of command plugin
     * @param params parameters for command.
     * @return Initialized plugin command.
     */
    public abstract ConcurrentTestPluginCommand getCommandFor(
        String name, String params);

    /**
     *  Do pre-setup action for given command and parameters.
     * @param name Name of command plugin
     * @param params parameters for command.
     */
    public void preSetupFor(String name, String params) {
    }
}
// End ConcurrentTestPlugin.java
