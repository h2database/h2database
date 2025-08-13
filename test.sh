#!/bin/bash
cd h2

export JAVA_HOME=/home/jo/.jdks/temurin-1.8.0_452
#export JAVA_HOME=/home/jo/.jdks/temurin-17.0.4.1

export JAVA_OPTS=-Xmx512m
export LANG=C

./build.sh clean jar testCI 2>&1 | tee testCI.log
