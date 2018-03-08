@echo off
setlocal
pushd "%~dp0"

copy /y /b ..\bin\h2-*.jar ..\bin\h2.jar
fc /b ..\bin\h2-*.jar ..\bin\h2.jar
if not errorlevel 1 goto :start
echo Please ensure there is only one h2-*.jar file.
echo Process stopped
pause
goto :end

:start
rem Copyright (c) 1999, 2006 Tanuki Software Inc.
rem
rem Java Service Wrapper general NT service install script
rem
if "%OS%"=="Windows_NT" goto nt
echo This script only works with NT-based versions of Windows.
goto :end

:nt
rem
rem Find the application home.
rem
rem %~dp0 is location of current script under NT
set _REALPATH=%~dp0

rem Decide on the wrapper binary.
set _WRAPPER_BASE=wrapper
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%-windows-x86-32.exe
if exist "%_WRAPPER_EXE%" goto conf
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%-windows-x86-64.exe
if exist "%_WRAPPER_EXE%" goto conf
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%.exe
if exist "%_WRAPPER_EXE%" goto conf
echo Unable to locate a Wrapper executable using any of the following names:
echo %_REALPATH%%_WRAPPER_BASE%-windows-x86-32.exe
echo %_REALPATH%%_WRAPPER_BASE%-windows-x86-64.exe
echo %_REALPATH%%_WRAPPER_BASE%.exe
pause
goto :end

:conf
rem
rem Find the wrapper.conf
rem
set _WRAPPER_CONF="%~f1"
if not %_WRAPPER_CONF%=="" goto startup
set _WRAPPER_CONF="%_REALPATH%wrapper.conf"

:startup
rem
rem Install the Wrapper as an NT service.
rem
"%_WRAPPER_EXE%" -i %_WRAPPER_CONF%
if not errorlevel 1 goto :end
pause

:end
popd