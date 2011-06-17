@echo off
if "%JAVA_HOME%"=="" echo Error: JAVA_HOME is not defined.
if "%1"=="clean" rmdir /s /q temp | rmdir /s /q bin
if not exist temp mkdir temp
if not exist bin mkdir bin
javac -sourcepath src/tools -d bin src/tools/org/h2/build/*.java
"%JAVA_HOME%/bin/java" -Xmx256m -cp "bin;%JAVA_HOME%/lib/tools.jar;temp" org.h2.build.Build %*