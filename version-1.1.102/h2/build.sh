#!/bin/sh
if [ -z "$JAVA_HOME" ] ; then
  echo "Error: JAVA_HOME is not defined."
fi
if [ ! -d "temp" ] ; then
  mkdir temp
fi
if [ ! -d "bin" ] ; then
  mkdir bin
fi
if [ ! -f "bin/org/h2/build/Build.class" ] ; then
  javac -sourcepath src/tools -d bin src/tools/org/h2/build/*.java
fi
"$JAVA_HOME/bin/java" -Xmx512m -cp "bin:$JAVA_HOME/lib/tools.jar:temp" org.h2.build.Build $@
