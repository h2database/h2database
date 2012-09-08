#!/bin/sh
echo $(date "+%H:%M:%S") Start
cd ../..
rm -rf ../h2web
mkdir ../h2web

rm -rf bin
rm -rf temp

./build.sh -quiet compile
./build.sh -quiet spellcheck javadocImpl jarClient
./build.sh -quiet clean compile installer mavenDeployCentral

# ./build.sh -quiet compile benchmark
# == Copy the benchmark results 
# == and update the performance page and diagram

echo $(date "+%H:%M:%S") Done
