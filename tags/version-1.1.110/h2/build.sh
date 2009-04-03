#!/bin/sh
if [ -z "$JAVA_HOME" ] ; then
  if [ -d "/System/Library/Frameworks/JavaVM.framework/Home" ] ; then
    export JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Home
  else
    echo "Error: JAVA_HOME is not defined."
  fi
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
"$JAVA_HOME/bin/java" -Xmx256m -cp "bin:$JAVA_HOME/lib/tools.jar:temp" org.h2.build.Build $@
