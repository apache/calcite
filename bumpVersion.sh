#!/bin/bash

pom='pom.xml'

if [ ! -f ./$pom ] 
  then 	
    echo "Cannot find $pom in current dir"
    exit 1;	
fi

currVerParts=$(cat $pom | sed -n 's/<version>\(.*\)-c\([0-9][0-9]*\)-SNAPSHOT<\/version>.*/\1-\2/p' | head -n 1)
currInternalVer=$(echo $currVerParts | cut -d '-' -f 2)
currCalciteVer=$(echo $currVerParts | cut -d '-' -f 1)
nextInternalVer=$(($currInternalVer+1))

currVer=$currCalciteVer-c$currInternalVer-SNAPSHOT
nextVer=$currCalciteVer-c$nextInternalVer-SNAPSHOT
echo current version = $currVer
echo next version = $nextVer

echo start replacing versions in all $pom files 

for pomPath in `grep -rnw './' -e $currVer --include \$pom | cut -f1 -d ':'`
do 
  echo processing $pomPath
  sed -e "s/$currVer/$nextVer/g" $pomPath > /tmp/tmpPom 
  cat /tmp/tmpPom > $pomPath
  echo competed $pomPath
done
echo done
