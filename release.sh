#!/bin/bash

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

function terminate() {
    printf "\n\nUser terminated build. Exiting...\n"
    exit 1
}

trap terminate SIGINT

KEYS=()

init(){
    apk --no-cache add ca-certificates wget git gnupg
    wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub
    wget -q -O /tmp/glibc.apk https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.28-r0/glibc-2.28-r0.apk
    apk add /tmp/glibc.apk
}

GPG_COMMAND="gpg2"

get_gpg_keys(){
    GPG_KEYS=$($GPG_COMMAND --list-keys --with-colons --keyid-format LONG)

    KEY_NUM=1

    KEY_DETAILS=""

    while read -r line; do

        IFS=':' read -ra PART <<< "$line"

        if [ ${PART[0]} == "pub" ]; then

            if [ -n "$KEY_DETAILS" ]; then
                KEYS[$KEY_NUM]=$KEY_DETAILS
                KEY_DETAILS=""
                ((KEY_NUM++))

            fi

            KEY_DETAILS=${PART[4]}
        fi

        if [ ${PART[0]} == "uid" ]; then
            KEY_DETAILS="$KEY_DETAILS - ${PART[9]}"
        fi

    done <<< "$GPG_KEYS"

    if [ -n "$KEY_DETAILS" ]; then
        KEYS[$KEY_NUM]=$KEY_DETAILS
    fi
}

mount_gpg_keys(){
    mkdir -p /.gnupg

    if [ -z "$(ls -A /.gnupg)" ]; then
        echo "Please mount the contents of your .gnupg folder into /.gnupg. Exiting..."
        exit 1
    fi

    mkdir -p /root/.gnupg

    cp -r /.gnupg/ /root/

    chmod -R 700 /root/.gnupg/

    rm -rf /root/.gnupg/*.lock
}

SELECTED_GPG_KEY=""

select_gpg_key(){

    get_gpg_keys

    export GPG_TTY=/dev/console

    touch /root/.gnupg/gpg-agent.conf
    echo 'default-cache-ttl 10000' >> /root/.gnupg/gpg-agent.conf
    echo 'max-cache-ttl 10000' >> /root/.gnupg/gpg-agent.conf

    echo "Starting GPG agent..."
    gpg-agent --daemon

    while $INVALID_KEY_SELECTED; do

        if [ "${#KEYS[@]}" -le 0 ]; then
            echo "You do not have any GPG keys available. Exiting..."
            exit 1
        fi

        echo "You have the following GPG keys:"

        for i in "${!KEYS[@]}"; do
                echo "$i) ${KEYS[$i]}"
        done

        read -p "Select your GPG key for signing: " KEY_INDEX

        SELECTED_GPG_KEY=$(sed 's/ -.*//' <<< ${KEYS[$KEY_INDEX]})

        if [ -z $SELECTED_GPG_KEY ]; then
            echo "Selected key is invalid, please try again."
            continue
        fi

        echo "Authenticating your GPG key..."

        echo "test" | $GPG_COMMAND --local-user $SELECTED_GPG_KEY --output /dev/null --sign -

        if [ $? != 0 ]; then
            echo "Invalid GPG passphrase or GPG error. Please try again."
            continue
        fi

        echo "You have selected the following GPG key to sign the release:"
        echo "${KEYS[$KEY_INDEX]}"

        INVALID_CONFIRMATION=true

        while $INVALID_CONFIRMATION; do
            read -p "Is this correct? (y/n) " CONFIRM

            if [[ ($CONFIRM == "Y") || ($CONFIRM == "y") ]]; then
                INVALID_KEY_SELECTED=false
                INVALID_CONFIRMATION=false
            elif [[ ($CONFIRM == "N") || ($CONFIRM == "n") ]]; then
                INVALID_CONFIRMATION=false
            fi
        done
    done
}

RELEASE_VERSION=""
RC_NUMBER=""
DEV_VERSION=""
ASF_USERNAME=""
NAME=""

get_build_configuration(){

    while $NOT_CONFIRMED; do
        read -p "Enter the version number to be released (example: 1.12.0): " RELEASE_VERSION
        read -p "Enter the release candidate number (example: if you are releasing rc0, enter 0): " RC_NUMBER
        read -p "Enter the development version number (example: if your release version is 1.12.0, enter 1.13.0): " DEV_VERSION
        read -p "Enter your ASF username: " ASF_USERNAME
        read -p "Enter your name (this will be used for git commits): " NAME
        echo "Build configured as follows:"
        echo "Release: $RELEASE_VERSION-rc$RC_NUMBER"
        echo "Next development version: $DEV_VERSION-SNAPSHOT"
        echo "ASF Username: $ASF_USERNAME"
        echo "Name: $NAME"

        INVALID_CONFIRMATION=true

        while $INVALID_CONFIRMATION; do
            read -p "Is this correct? (y/n) " CONFIRM

            if [[ ($CONFIRM == "Y") || ($CONFIRM == "y") ]]; then
                NOT_CONFIRMED=false
                INVALID_CONFIRMATION=false
            elif [[ ($CONFIRM == "N") || ($CONFIRM == "n") ]]; then
                INVALID_CONFIRMATION=false
            fi
        done
    done
}

ASF_PASSWORD=""

set_git_credentials(){
    read -s -p "Enter your ASF password: " ASF_PASSWORD

    printf "\n"

    echo https://$ASF_USERNAME:$ASF_PASSWORD@git-wip-us.apache.org >> /root/.git-credentials
    git config --global credential.helper 'store --file=/root/.git-credentials'

    git config --global user.name "$NAME"
}

set_maven_credentials(){
    mkdir -p /root/.m2
    rm -f /root/.m2/settings.xml
    rm -f /root/.m2/settings-security.xml

    read -s -p "Enter a maven master password (used to encrypt your ASF password): " MAVEN_MASTER_PASSWORD

    printf "\n"

    ENCRYPTED_MAVEN_PASSWORD="$(mvn --encrypt-master-password $MAVEN_MASTER_PASSWORD)"

    cat <<EOF >> /root/.m2/settings-security.xml
<settingsSecurity>
  <master>$ENCRYPTED_MAVEN_PASSWORD</master>
</settingsSecurity>
EOF

    ENCRYPTED_ASF_PASSWORD="$(mvn --encrypt-password $ASF_PASSWORD)"

    cat <<EOF >> /root/.m2/settings.xml
<settings>
  <servers>
    <server>
      <id>apache.snapshots.https</id>
      <username>${ASF_USERNAME}</username>
      <password>${ENCRYPTED_ASF_PASSWORD}</password>
    </server>
    <server>
      <id>apache.releases.https</id>
      <username>${ASF_USERNAME}</username>
      <password>${ENCRYPTED_ASF_PASSWORD}</password>
    </server>
  </servers>
</settings>
EOF
}

case $1 in
    dry-run)
        init
        mount_gpg_keys
        select_gpg_key
        get_build_configuration

        mvn -Dmaven.artifact.threads=20 -DdryRun=true -DreleaseVersion=$RELEASE_VERSION -DdevelopmentVersion=$DEV_VERSION-SNAPSHOT -Dtag="avatica-$RELEASE_VERSION-rc$RC_NUMBER" -Papache-release -Duser.name=$ASF_USERNAME release:prepare -Darguments="-DskipDockerCheck -Dgpg.keyname=$SELECTED_GPG_KEY"
        ;;

    release)
        init
        mount_gpg_keys
        select_gpg_key
        get_build_configuration
        set_git_credentials
        set_maven_credentials

        mvn -Dmaven.artifact.threads=20 -DreleaseVersion=$RELEASE_VERSION -DdevelopmentVersion=$DEV_VERSION-SNAPSHOT -Dtag="avatica-$RELEASE_VERSION-rc$RC_NUMBER" -Papache-release -Duser.name=$ASF_USERNAME release:prepare -Darguments=-Dgpg.keyname=$SELECTED_GPG_KEY
        mvn -Dmaven.artifact.threads=20 -Papache-release -Duser.name=$ASF_USERNAME release:perform -Darguments="-DskipTests -Dgpg.keyname=$SELECTED_GPG_KEY"
        ;;

    clean)
        init
        mvn release:clean
        ;;

    *)
       echo $"Usage: $0 {dry-run|release|clean}"
       ;;

esac