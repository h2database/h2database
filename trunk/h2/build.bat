@echo off
if exist bin/org/h2/build/Build.class goto buildOK
if not exist bin mkdir bin
javac -sourcepath src/tools -d bin src/tools/org/h2/build/*.java
:buildOK
java -cp "bin;%JAVA_HOME%/lib/tools.jar;target" org.h2.build.Build %1