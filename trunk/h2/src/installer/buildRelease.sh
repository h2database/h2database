#!/bin/sh
cd ../..
today=$(date "+%Y-%m-%d")
rmdir -r ../h2web-$today
rmdir -r ../h2web
mkdir ../h2web

rmdir -r bin
rmdir /s /q temp
call java14
./build.sh -quiet

call java16
call build -quiet compile
call build -quiet spellcheck javadocImpl jarClient

echo $(date "+%H:%M:%S") JDK 1.4
call java14
./build.sh -quiet clean compile
./build.sh -quiet installer mavenDeployCentral

rem ./build.sh -quiet compile benchmark
rem == Copy the benchmark results and update the performance page and diagram

call java16
./build.sh -quiet switchSource
mv ../h2web h2web-%today%

echo $(date "+%H:%M:%S") Done
