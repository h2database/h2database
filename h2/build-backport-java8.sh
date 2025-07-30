#! /usr/bin/bash

set -e # Fail on first nonzero return code
set -x # Print commands before executing

export JAVA_HOME=/home/jo/.jdks/temurin-1.8.0_452 # Replace with directory of whatever Java 8 you are using
#export JAVA_HOME=/home/jo/.jdks/temurin-17.0.4.1 # Uncomment for Java 11, e.g. to compile not-yet-backported code.

export JAVA_OPTS=-Xmx512m

export LANG=C # Prevent some language-dependent tests from failing

./build.sh compile # Recompile everything, to make sure we have a clean slate.
./build.sh testCI | tee testCI.log # testCI skips some tests that need special outside setup, e.g. certificates installed in the JVM.
./build.sh jar
