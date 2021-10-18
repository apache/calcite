#!/bin/bash
# Launch Tableau Desktop with Calcite connector on its path
PLUG="$(cd $(dirname $0) && pwd)"
#PLUG=$HOME/dev/tableau/connector-plugin-sdk/samples/plugins/
echo "/Applications/Tableau Desktop 2021.2.app/Contents/MacOS/Tableau" -DConnectPluginsPath="${PLUG}"
exec "/Applications/Tableau Desktop 2021.2.app/Contents/MacOS/Tableau" -DConnectPluginsPath="${PLUG}"
