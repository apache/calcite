pipeline {
   agent any

   stages {
      stage('source') {
         steps {
             git(
               url: 'http://taiga.datametica.com/rakesh.zingade/dm-calcite-nexus-publish.git',
               credentialsId: 'gitlab-rzingade',
               branch: "nexus-publish-gradle"
            )
           sh 'dir'
         }
      }
      stage('update release version') {
         steps {
            sh '''
              #!/bin/bash
              set -x
              release_number=$(curl -s -i -H "Accept: application/json" -u admin:admin123 "http://nexus2.datametica.com:8081/nexus/service/local/artifact/maven/redirect?r=thirdparty&g=org.apache.calcite&a=calcite-core&v=RELEASE" | grep Location | rev | cut -d/ -f2 | rev)
              released_major_ver=$(echo $release_number | cut -d. -f1,2)
              released_minor_ver=$(echo $release_number | cut -d. -f3)

              #get the calcite version from gradle.properties
              repo_calcite_ver=$(grep -e ^calcite.version gradle.properties | cut -d= -f2)
              repo_calcite_major_ver=$(echo $repo_calcite_ver | cut -d. -f1,2)
              repo_calcite_minor_ver=$(echo $repo_calcite_ver | cut -d. -f3)

              if [[ $(echo "$released_major_ver > $repo_calcite_major_ver" | bc -l) ]]; then
                  new_release_ver=$(expr $released_minor_ver + 1)
                  new_release_ver=$released_major_ver.$new_release_ver
                  sed -i "s/$repo_calcite_ver/$new_release_ver/" gradle.properties
              fi

            '''
         }
      }
      stage('build') {
         steps {
           sh '''
             #!/bin/bash
             ./gradlew --init-script init.gradle.kts assemble 
           '''
         }
      }
      stage('publish') {
          steps {
           sh './gradlew --init-script init.gradle.kts publishAllPublicationsToSecretNexusRepository --no-daemon'
          }
      }
   }
}

