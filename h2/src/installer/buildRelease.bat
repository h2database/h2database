@echo off
echo %time:~0,8%

setlocal
cd ../..
set today=%date:~6%%date:~3,2%%date:~0,2%
rmdir /s /q ..\h2web-%today% 2>nul
rmdir /s /q ..\h2web 2>nul
mkdir ..\h2web

rem echo Test with Hibernate
rem echo Run FindBugs
rem pause

rmdir /s /q bin 2>nul
rmdir /s /q temp 2>nul
call java14 >nul 2>nul
call build -quiet compile
call build -quiet spellcheck
call build -quiet jarClient
rem echo Check jar file size
rem pause

call build -quiet jar
call build -quiet javadocImpl
call java16 >nul 2>nul
call build -quiet compile
set classpath=
call build -quiet javadoc
call build -quiet javadocImpl
rem echo Check if missing javadocs
rem pause 

call java14 >nul 2>nul
call build -quiet compile
call java16 >nul 2>nul
call build -quiet compile

rem echo Change version and build number in Constants.java
rem echo Maybe increase TCP_DRIVER_VERSION (old clients must be compatible!)
rem echo Check code coverage
rem echo No "  Message.get" (must be "throw Message.get")
rem echo Check that is no TODO in the docs
rem echo Run regression test with JDK 1.4 and 1.5
rem echo Use latest versions of other dbs
rem echo   Derby 10.4.1.3
rem echo   PostgreSQL 8.3.1
rem echo   MySQL 5.0.51
rem echo Change version(s) in performance.html
rem pause

rem call java14 >nul 2>nul
rem call build -quiet benchmark
rem Copy the benchmark results and update the performance page and diagram
rem echo Documentation: check if all Javadoc files are in the index
rem echo Update the changelog (add new version)
rem echo Update the newsfeed
rem pause

call build -quiet docs
rem echo Check dataWeb/index.html, versions and links in main, downloads, build
rem pause 

soffice.exe -invisible macro:///Standard.Module1.H2Pdf
rem echo Check in the PDF file:
rem echo - footer
rem echo - front page
rem echo - orphan control
rem echo - check images
rem echo - table of contents
rem pause

call java14 >nul 2>nul
call build -quiet all
copy ..\h2web\h2.pdf docs >nul
rem echo Check the pdf file is in h2/docs
rem pause
 
call build -quiet zip
makensis /v2 src/installer/h2.nsi
rem echo Test Console
rem echo Test all languages
rem echo Scan for viruses
rem pause

call build -quiet mavenDeployCentral
rem echo Upload to SourceForge
rem pause

call java16 >nul 2>nul
call build -quiet compile
rem echo svn commit
rem echo svn copy: /svn/trunk /svn/tags/version-1.0.x; Version 1.0.x (yyyy-mm-dd)
rem echo Newsletter: prepare (always to BCC!!)
rem echo Upload to h2database.com, http://code.google.com/p/h2database/downloads/list
rem echo Newsletter: send (always to BCC!!)
rem echo Add to freshmeat
rem echo http://en.wikipedia.org/wiki/H2_%28DBMS%29 (change version)
rem echo http://www.heise.de/software/

ren ..\h2web h2web-%today%
echo %time:~0,8%
