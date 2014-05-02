@echo off
echo %time:~0,8% Start

setlocal
cd ../..
set today=%date:~6%%date:~3,2%%date:~0,2%
rmdir /s /q ..\h2web-%today% 2>nul
rmdir /s /q ..\h2web 2>nul
mkdir ..\h2web

rmdir /s /q bin 2>nul
rmdir /s /q temp 2>nul

call java16 >nul 2>nul
call build -quiet compile
call build -quiet spellcheck javadocImpl jarClient
call build -quiet clean compile installer mavenDeployCentral

rem call build -quiet compile benchmark
rem == Copy the benchmark results and update the performance page and diagram

ren ..\h2web h2web-%today%

echo %time:~0,8% Done
