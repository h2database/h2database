#!/bin/sh
dir=$(dirname "$0")
java -cp "$dir/target/h2-2.3.239-SNAPSHOT.jar:$H2DRIVERS:$CLASSPATH" org.h2.tools.Console "$@"
