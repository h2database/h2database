#!/bin/sh
dir=$(dirname "$0")
ext_dir="$dir/../ext"
CLASSPATH="$CLASSPATH:$ext_dir/jackson-core-2.9.8.jar:$ext_dir/jackson-databind-2.9.8.jar:$ext_dir/jackson-annotations-2.9.8.jar"
export CLASSPATH
echo $CLASSPATH
java -cp "$dir/h2.jar:$H2DRIVERS:$CLASSPATH" org.h2.tools.Console "$@"
