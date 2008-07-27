@echo off
echo %time:~0,8%

rem == Change version and build number in Constants.java
rem == Maybe increase TCP_DRIVER_VERSION (old clients must be compatible!)
rem == Update the changelog (add new version)
rem == Update the newsfeed
rem == No "  Message.get" (must be "throw Message.get")
rem == Documentation: check if all Javadoc files are in the index
rem == Check that is no TODO in the docs

rem == Check code coverage
rem == Run regression test with JDK 1.4 and 1.5
rem == Use latest versions of other dbs
rem ==   - Derby 10.4.1.3
rem ==   - PostgreSQL 8.3.1
rem ==   - MySQL 5.0.51
rem == Change version(s) in performance.html

setlocal
cd ../..
set today=%date:~6%%date:~3,2%%date:~0,2%
rmdir /s /q ..\h2web-%today% 2>nul
rmdir /s /q ..\h2web 2>nul
mkdir ..\h2web

rmdir /s /q bin 2>nul
rmdir /s /q temp 2>nul
call java14 >nul 2>nul
call build -quiet compile
call build -quiet spellcheck
call build jarClient

call build -quiet jar
call build -quiet javadocImpl
call java16 >nul 2>nul
call build -quiet compile
set classpath=
call build -quiet javadoc
call build -quiet javadocImpl

call java14 >nul 2>nul
call build -quiet compile
call java16 >nul 2>nul
call build -quiet compile

rem call java14 >nul 2>nul
rem call build -quiet benchmark
rem == Copy the benchmark results and update the performance page and diagram

call build -quiet docs

soffice.exe -invisible macro:///Standard.Module1.H2Pdf
call java14 >nul 2>nul
call build -quiet all
copy ..\h2web\h2.pdf docs >nul
xcopy /s /q /y dataWeb ..\h2web >nul
call build -quiet zip
makensis /v2 src/installer/h2.nsi

call build -quiet mavenDeployCentral

call java16 >nul 2>nul
call build -quiet compile
ren ..\h2web h2web-%today%
echo %time:~0,8%

rem == Test with Hibernate
rem == Run FindBugs
rem == Check if missing javadocs
rem == Check jar file size

rem == Check dataWeb/index.html, versions and links in main, downloads, build

rem == Check the pdf file is in h2/docs
rem == Check in the PDF file:
rem == - footer
rem == - front page
rem == - orphan control
rem == - check images
rem == - table of contents

rem == Test Console
rem == Test all languages

rem == Scan for viruses

rem == Upload to SourceForge

rem == svn commit
rem == svn copy: /svn/trunk /svn/tags/version-1.0.x; Version 1.0.x (yyyy-mm-dd)
rem == Newsletter: prepare (always to BCC!!)
rem == Upload to h2database.com, http://code.google.com/p/h2database/downloads/list
rem == Newsletter: send (always to BCC!!)
rem == Add to freshmeat
rem == http://en.wikipedia.org/wiki/H2_%28DBMS%29 (change version)
rem == http://www.heise.de/software/
