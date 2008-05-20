#!/bin/sh
if [ -z "$JAVA_HOME" ] ; then
  echo "Error: JAVA_HOME is not defined."
fi
if [ ! -f "bin/org/h2/build/Build.class" ] ; then
  if [ ! -d "bin" ] ; then
    mkdir bin
  fi
  javac -nowarn -sourcepath src/tools -d bin src/tools/org/h2/build/*.java
fi
java -Xmx512m -cp "bin:$JAVA_HOME/lib/tools.jar:temp" org.h2.build.Build $@
