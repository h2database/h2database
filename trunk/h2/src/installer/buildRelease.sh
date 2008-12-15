#!/bin/sh
cd ../..
set TODAY=$(date "+%Y-%m-%d")
rm -r ../h2web-$TODAY
rm -r ../h2web
mkdir ../h2web

rm -r bin
rm -r temp
set JAVA_HOME=$JAVA14
set PATH=$JAVA14/bin:$PATH
./build.sh -quiet

set JAVA_HOME=$JAVA16
set PATH=$JAVA16/bin:$PATH
./build.sh -quiet compile
./build.sh -quiet spellcheck javadocImpl jarClient

echo $(date "+%H:%M:%S") JDK 1.4
set JAVA_HOME=$JAVA14
set PATH=$JAVA14/bin:$PATH
./build.sh -quiet clean compile
./build.sh -quiet installer mavenDeployCentral

# ./build.sh -quiet compile benchmark
# == Copy the benchmark results and update the performance page and diagram

set JAVA_HOME=$JAVA16
set PATH=$JAVA16/bin:$PATH
./build.sh -quiet switchSource
mv ../h2web h2web-$TODAY

echo $(date "+%H:%M:%S") Done
