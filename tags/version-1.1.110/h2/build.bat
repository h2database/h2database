@echo off
if "%JAVA_HOME%"=="" echo Error: JAVA_HOME is not defined.
if not exist temp mkdir temp
if not exist bin mkdir bin
if exist bin/org/h2/build/Build.class goto buildOK
javac -sourcepath src/tools -d bin src/tools/org/h2/build/*.java
:buildOK
"%JAVA_HOME%/bin/java" -Xmx256m -cp "bin;%JAVA_HOME%/lib/tools.jar;temp" org.h2.build.Build %*