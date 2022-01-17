#!/bin/sh
if [ -z "$JAVA_HOME" ] ; then
  if [[ "$OSTYPE" == "darwin"* ]]; then
    if [ -d "/System/Library/Frameworks/JavaVM.framework/Home" ] ; then
      export JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Home
    else
      export JAVA_HOME=`/usr/libexec/java_home`
    fi
  fi
fi
if [ -z "$JAVA_HOME" ] ; then
  echo "Error: JAVA_HOME is not defined."
fi
if [ "$1" = "clean" ] ; then rm -rf temp bin ; fi
if [ ! -d "temp" ] ; then mkdir temp ; fi
if [ ! -d "bin" ] ; then mkdir bin ; fi
"$JAVA_HOME/bin/javac" -sourcepath src/tools -d bin src/tools/org/h2/build/*.java
"$JAVA_HOME/bin/java" -Xmx1g -cp "bin:$JAVA_HOME/lib/tools.jar:temp" org.h2.build.Build $@
