#!/bin/sh
echo $(date "+%H:%M:%S") Start
cd ../..
TODAY=$(date "+%Y-%m-%d")
rm -rf ../h2web_$TODAY
rm -rf ../h2web
mkdir ../h2web

rm -rf bin
rm -rf temp
JAVA_HOME=$JAVA14
PATH=$JAVA14/bin:$PATH
./build.sh -quiet

JAVA_HOME=$JAVA16
PATH=$JAVA16/bin:$PATH
./build.sh -quiet compile
./build.sh -quiet spellcheck javadocImpl jarClient

echo $(date "+%H:%M:%S") JDK 1.4
JAVA_HOME=$JAVA14
PATH=$JAVA14/bin:$PATH
./build.sh -quiet clean compile
./build.sh -quiet installer mavenDeployCentral

# ./build.sh -quiet compile benchmark
# == Copy the benchmark results and update the performance page and diagram

JAVA_HOME=$JAVA15
PATH=$JAVA15/bin:$PATH
./build.sh -quiet switchSource
mv ../h2web ../h2web_$TODAY

echo $(date "+%H:%M:%S") Done
