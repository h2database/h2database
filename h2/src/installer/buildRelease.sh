#!/bin/sh
echo $(date "+%H:%M:%S") Start
cd ../..
rm -rf ../h2web
mkdir ../h2web

rm -rf bin
rm -rf temp
JAVA_HOME=$JAVA15
PATH=$JAVA15/bin:$PATH
./build.sh -quiet

JAVA_HOME=$JAVA16
PATH=$JAVA16/bin:$PATH
./build.sh -quiet compile
./build.sh -quiet spellcheck javadocImpl jarClient

echo $(date "+%H:%M:%S") JDK 1.4
export BCP=/System/Library/Frameworks/JavaVM.framework/Versions/1.4/Classes
JAVA_HOME=$JAVA15
PATH=$JAVA15/bin:$PATH
./build.sh -quiet clean -Dversion=1.4 switchSource -Dbcp=$BCP/classes.jar:$BCP/jsse.jar compile
./build.sh -quiet installer mavenDeployCentral

# ./build.sh -quiet compile benchmark
# == Copy the benchmark results 
# == and update the performance page and diagram

JAVA_HOME=$JAVA15
PATH=$JAVA15/bin:$PATH
./build.sh -quiet switchSource

echo $(date "+%H:%M:%S") Done
