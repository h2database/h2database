#!/bin/sh
dir="$(dirname $0)"
cp="$dir/h2.jar"
if [ -n "$H2DRIVERS" ] ; then
  cp="$cp:$H2DRIVERS"
fi
if [ -n "$CLASSPATH" ] ; then
  cp="$cp:$CLASSPATH"
fi
java -cp "$cp" org.h2.tools.Console $@

