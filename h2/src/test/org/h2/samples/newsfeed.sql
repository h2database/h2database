/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

CREATE TABLE CHANNEL(TITLE VARCHAR, LINK VARCHAR, DESC VARCHAR,
    LANGUAGE VARCHAR, PUB TIMESTAMP, LAST TIMESTAMP, AUTHOR VARCHAR);

INSERT INTO CHANNEL VALUES('H2 Database Engine' ,
    'http://www.h2database.com', 'H2 Database Engine', 'en-us', NOW(), NOW(), 'Thomas Mueller');

CREATE TABLE ITEM(ID INT PRIMARY KEY, TITLE VARCHAR, ISSUED TIMESTAMP, DESC VARCHAR);

INSERT INTO ITEM VALUES(33,
'New version available: 1.0.63 (2007-12-02)', '2007-12-02 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>Performance optimization for IN(...) and IN(SELECT...), 
    currently disabled by default. To enable, use java -Dh2.optimizeInJoin=true
</li><li>The H2 Console has been translated to Ukrainian by Igor Dobrovolskyi. Thanks a lot! 
</li><li>The SecurePassword example has been improved.
</li><li>Improved FTP server: now the PORT command is supported.
</li><li>New function TABLE_DISTINCT. 
</li></ul>
<b>Bugfixes:</b>
<ul><li>Certain setting in the Server didn''t work.
</li><li>In timezones where the summer time saving limit is at midnight, 
  some dates did not work in some virtual machines, 
    for example 2007-10-14 in Chile, using the Sun JVM 1.6.0_03-b05.
</li><li>The native fulltext search was not working properly after re-connecting. 
</li><li>Temporary views (FROM(...)) with UNION didn''t work if nested. 
</li><li>Using LIMIT with values close to Integer.MAX_VALUE didn''t work. 
</li></ul>
For future plans, see the ''Roadmap'' page at
http://groups.google.com/group/h2-database/web/roadmap
');

INSERT INTO ITEM VALUES(32,
'New version available: 1.0.62 (2007-11-25)', '2007-11-25 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>Large updates and deletes are now supported.
</li><li>Google Android is supported.
</li><li>Large CSV operations are now faster.
</li><li>A patch for Apache DDL Utils is available.
</li><li>Eduardo Velasques has translated H2 to Brazilian Portuguese.
</li><li>Now using custom toString() for JDBC objects.
</li><li>The setting h2.emergencySpaceInitial is now 256 KB.
</li></ul>
<b>Bugfixes:</b>
<ul><li>Creating a table from GROUP_CONCAT didn''t always work.
</li><li>CSV: Using an empty field delimiter didn''t work.
</li><li>Nested temporary views with parameters didn''t always work.
</li><li>Cluster mode: could not connect if only one server was running.
</li><li>ARRAY values are now sorted as in PostgreSQL.
</li><li>The console did not display multiple spaces correctly.
</li><li>Duplicate column names were not detected when renaming columns.
</li><li>The H2 Console now also supports -ifExists.
</li><li>Changing a user with a schema made the schema inaccessible.
</li><li>Referential integrity checks didn''t lock the referenced table.
</li><li>Now changing MVCC too late throws an Exception.
</li></ul>
For future plans, see the ''Roadmap'' page at
http://groups.google.com/group/h2-database/web/roadmap
');

INSERT INTO ITEM VALUES(31,
'New version available: 1.0.61 (2007-11-10)', '2007-11-10 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>Read-only databases in zip (or jar) files are now supported: jdbc:h2:zip:c:/temp/db.zip!/test
</li><li>File access is now done using an extensible API. Additional file systems are easy to implement.
</li><li>Descending indexes are supported.
</li><li>The Lucene fulltext search is included in the h2.jar.
</li><li>MODE is now a database level setting (not global).
</li><li>Vlad Alexahin has translated H2 Console to Russian. Thanks a lot!
</li><li>INSTR, LOCATE: backward searching is now supported by using a negative start position.
</li><li>CREATE SEQUENCE: New option CACHE (number of pre-allocated numbers).
</li><li>Converting decimal to integer now rounds like MySQL and PostgreSQL.
</li><li>Math operations using only parameters are now interpreted as decimal.
</li><li>MVCC: The system property h2.mvcc has been removed.
</li></ul>
<b>Bugfixes:</b>
<ul><li>ResultSetMetaData.getColumnDisplaySize is now calculated correctly.
</li><li>A few MVCC bugs have been fixed.
</li><li>The code coverage is now at 83%.
</li></ul>
For future plans, see the ''Roadmap'' page at
http://groups.google.com/group/h2-database/web/roadmap
');

INSERT INTO ITEM VALUES(30,
'New version available: 1.0.60 (2007-10-20)', '2007-10-20 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>User defined aggregate functions are now supported.
</li><li>New Italian translation from PierPaolo Ucchino. Thanks a lot!
</li><li>CSV: New methods to set the escape character and field delimiter in the Csv tool and the CSVWRITE and CSVREAD methods.
</li><li>CSVREAD, RUNSCRIPT and so on now support URLs as well, using
    URL.openStream(). Example: select * from csvread(''jar:file:///c:/temp/test.jar!/test.csv'');
</li></ul>
<b>Bugfixes:</b>
<ul><li>Prepared statements could not be used after data definition statements (creating tables and so on). Fixed.
</li><li>PreparedStatement.setMaxRows could not be changed to a higher value after the statement was executed.
</li><li>Linked tables: now tables in non-default schemas are supported as well 
</li><li>JdbcXAConnection: starting a transaction before getting the connection didn''t switch off autocommit.
</li><li>Server.shutdownTcpServer was blocked when first called with force=false and then force=true.
    Now documentation is improved, and it is no longer blocked.
</li><li>Stack traces did not include the SQL statement in all cases where they could have. 
    Also, stack traces with SQL statement are now shorter.
</li><li>The H2 Console could not connect twice to the same H2 embedded database at the same time. Fixed.
</li></ul>
For future plans, see the ''Roadmap'' page at
http://groups.google.com/group/h2-database/web/roadmap
');

INSERT INTO ITEM VALUES(29,
'New version available: 1.0.59 (2007-10-03)', '2007-10-03 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>Fulltext search is now documented (see Tutorial).
</li><li>H2 Console: Progress information when logging into a H2 embedded database.
</li><li>SCRIPT: the SQL statements in the result set now include the terminating semicolon.
</li></ul>
<b>Bugfixes:</b>
<ul><li>If the process was killed while the database was running, 
    sometimes the database could not be opened.
</li><li>Comparing columns with constants that are out of range works again.
</li><li>When the data type was unknown in a subquery, sometimes the wrong exception was thrown.
</li><li>Multi-threaded kernel (MULTI_THREADED=1): A synchronization problem has been fixed.
</li><li>A PreparedStatement that was cancelled could not be reused. 
</li><li>When the database was closed while logging was disabled (LOG 0), 
    re-opening the database was slow.
</li><li>The Console did not always refresh the table list when required.
</li><li>When creating a table using CREATE TABLE .. AS SELECT, 
    the precision for some data types was wrong in some cases.
</li><li>When using the (undocumented) in-memory file system 
    (jdbc:h2:memFS:x or jdbc:h2:memLZF:x), and using multiple connections, 
    a ConcurrentModificationException could occur. 
</li><li>REGEXP compatibility: now Matcher.find is used.
</li><li>When using a subquery with group by as a table, some columns could not be used.
</li><li>Views with subqueries as tables and queries with nested subqueries as tables did not always work.
</li></ul>
For future plans, see the new ''Roadmap'' page on the web site.
');

INSERT INTO ITEM VALUES(28,
'New version available: 1.0.58 (2007-09-15)', '2007-09-15 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>Empty space in the database files is now better reused
</li><li>The database file sizes now increased in smaller blocks
</li><li>Optimization for independent subqueries
</li><li>Improved explain plan 
</li><li>Maven 2: new version are now automatically synced
</li><li>The version (build) number is now included in the manifest file.
</li><li>The default value for MAX_MEMORY_UNDO is now 100000
</li><li>Improved MultiDimension tool (for spatial queries)
</li><li>New method DatabaseEventListener.opened
</li><li>Optimization for COLUMN IN(.., NULL)
</li><li>Oracle compatibility for SYSDATE and CHR
</li><li>System.exit is no longer called by the WebServer
</li></ul>
<b>Bugfixes:</b>
<ul><li>About 230 bytes per database was leaked
</li><li>Using spaces in column and table aliases did not always work
</li><li>In some systems, SecureRandom.generateSeed is very slow
</li><li>Console: better support for Internet Explorer
</li><li>A database can now be opened even if user class is missing
</li><li>User defined functions may not overload built-in functions
</li><li>Adding a foreign key failed when the reference contained NULL
</li><li>For PgServer, character encoding other than UTF-8 did not work
</li><li>When using IFNULL, NULLIF, COALESCE, LEAST, or GREATEST, 
    and the first parameter was ?, an exception was thrown
</li><li>When comparing TINYINT or SMALLINT columns, the index was not used
</li><li>The documentation indexer does no longer index Japanese pages
</li><li>Using a function in a GROUP BY expression did not always work
</li></ul>
For future plans, see the new ''Roadmap'' page on the web site.
');

INSERT INTO ITEM VALUES(27,
'New version available: 1.0.57 (2007-08-25)', '2007-08-25 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').

<br />
<b>Changes and new functionality:</b>
<ul><li>
The default lock mode is now read committed instead of serialized.
</li><li>The build now issues a warning if the source code is switched to the wrong version.
</li><li>The H2 Console can now connect to databases using JNDI. 
</li><li>New experimental feature MVCC (multi version concurrency control). 
</li><li>The version number is now major.minor.micro where micro is the build number. 
</li><li>New Japanese translation of the error messages thanks to Ikemoto Masahiro.
</li><li>Disabling / enabling referential integrity for a table can now be used inside a transaction.
</li><li>Check and foreign key constraints now checks if the existing data is consistent.
</li><li>Can now incrementally translate the documentation.
</li><li>Improved error messages.
</li></ul>
<b>Bugfixes:</b>
<ul><li>
Some unit tests failed on Linux because the file system works differently. 
</li><li>Rights checking for dynamic tables (SELECT * FROM (SELECT ...)) did not work. 
</li><li>More than 10 views that depend on each other was very slow. 
</li><li>When used as as Servlet, the H2 Console did not work with SSL (using Tomcat). 
</li><li>Problem when altering a table with foreign key constraint, if there was no manual index.
</li><li>The backup tool (org.h2.tools.Backup) did not work. 
</li><li>Opening large read-only databases was very slow. Fixed.
</li><li>OpenOffice compatibility: support database name in column names.
</li><li>The column name C_CURRENT_TIMESTAMP did not work in the last release.
</li><li>Two-phase commit: commit with transaction name was only supported in the recovery scan. 
</li><li>PG server: data was truncated when reading large VARCHAR columns and decimal columns.
</li><li>PG server: error when the same database was accessed multiple times using the PostgreSQL ODBC driver.
</li><li>Some file operations didn''t work for files in the root directory. 
</li><li>In the Restore tool, the parameter -file did not work.
</li><li>The CONVERT function did not work with views when using UNION.
</li><li>Google translate did not work for the H2 homepage.
</li></ul>
For future plans, see the new ''Roadmap'' page on the web site.
');

INSERT INTO ITEM VALUES(26,
'New version available: 1.0 / 2007-08-02', '2007-08-02 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').

<br />
<b>Changes and new functionality:</b>
<ul>
</li><li>H2 databases can now be accessed using the PostgreSQL ODBC driver.
</li><li>The old H2 ODBC driver has been removed.
</li><li>Function REGEXP_REPLACE and regular expression LIKE: REGEXP.
</li><li>CREATE TABLE ... AS SELECT now needs less memory.
</li><li>The per session undo log can now be disabled.
</li><li>Referential integrity can now be disabled.
</li><li>To avoid memory problems when using large transactions,
    h2.defaultMaxMemoryUndo is now 50000.    
</li><li>DEFAULT_MAX_LENGTH_INPLACE_LOB is now 1024.
</li><li>The cache size is now measured in KB.
</li><li>Optimization for NOT, boolean columns, and certain joins. 
</li><li>Part of the documentation has been translated to Japanese.
</li><li>The error messages (src/main/org/h2/res/_*.*) can now be translated.
</li><li>A new tool to help translation has been implemented
</li><li>The SysTray tool has been removed.
</ul>
<b>Bugfixes:</b>
<ul>
</li><li>Running out of memory while inserting could corrupt the database.
</li><li>Some Unicode characters where not supported as identifiers.
</li><li>H2 Console: The shutdown button works again.
</li><li>LOBs were not backed up using the BACKUP statement or tool when 
    h2.lobFilesInDirectories was enabled. 
</li><li>Calculation of cache memory usage has been improved.
</li><li>In some situations records were released from the cache too late.
</li><li>Documentation: the source code in ''Compacting a Database'' was incorrect.
</li><li>Result set in the H2 Console can now be modified again.
</li><li>Views using UNION did not work correctly.
</li><li>Function tables did not work with views and EXPLAIN.
</li>
</ul>
For future plans, see the new ''Roadmap'' page on the web site.
');

INSERT INTO ITEM VALUES(25,
'New version available: 1.0 / 2007-07-12', '2007-07-12 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br />
<b>Changes and new functionality:</b>
<ul>
<li>The H2 Console can run as a standalone web application.
</li><li>The default database name is now jdbc:h2:~/test.
</li><li>The new view implementation is now enabled by default. 
</li><li>The Polish translation is available. Thanks a lot to Tomek!
</li><li>Support for the system property baseDir.
</li><li>Improved PostgreSQL compatibility.
</li><li>New column ID in INFORMATION_SCHEMA tables.
</li><li>PreparedStatement.getMetaData is now implemented.
</li><li>New setting h2.allowBigDecimalExtensions.
</li><li>The SQL statement SET ASSERT has been deprecated.
</li><li>The trace level for JdbcDataSourceFactory is now ERROR.
</li><li>Referential integrity violation: Two SQL states are now used.
</li><li>DatabaseEventListener.exceptionThrown is changed.
</li><li>The catalog name can now be used in queries.
</li><li>The default result set type is now FETCH_FORWARD.
</li>
</ul>
<b>Bugfixes:</b>
<ul>
</li><li>Views did not work in some cases. 
</li><li>LIKE ESCAPE did not work correctly in some cases.
</li><li>In some situations, large objects were deleted.
</li><li>CREATE TABLE AS SELECT .. UNION .. did not work.
</li><li>Sometimes temp files were not deleted.
</li><li>PooledConnection.getConnection is now faster.
</li><li>Deleting databases in the root directory now works.
</li><li>Windows service: the CLASSPATH was not included.
</li><li>For READ_COMMITTED, when the multi-threaded 
    kernel is enabled, read locks are now acquired but released 
    immediately after a query.
</li>
</ul>
For future plans, see the new ''Roadmap'' page on the web site.
');

INSERT INTO ITEM VALUES(24,
'New version available: 1.0 / 2007-06-17', '2007-06-17 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br />
<b>Changes and new functionality:</b>
<ul>
<li>New Console starter application uses the JDK 1.6 system tray 
functionality if available, or a simple AWT frame for other platforms. 
To try it out, execute java org.h2.tools.Console. Feedback is welcome. 
This console starter application is not the default yet, 
but the plan is to remove the SysTray tool in the future.
</li><li>If a Reader or InputStream of a LOB is not closed, 
the LOB can not be deleted (embedded mode only). The exception is 
typically ''Error while renaming file''). As a workaround, set the 
system property ''h2.lobCloseBetweenReads'' to true to close the LOB 
files between read operations. However this slows down reading.
</li><li>Views support has been partially re-implemented. 
Views are up to 6 times faster. Compared to regular queries, only 
20% overhead. Because this is a bigger change, it is not enabled 
by default. To enable it, set the system property ''h2.indexNew'' 
to true (java -Dh2.indexNew=true ..., or in source code 
Constants.INDEX_NEW = true). If no problems are found, this will 
be enabled by default in the next release.
</li><li>Support for the data type CHAR. The difference to 
VARCHAR is: trailing spaces are ignored. This data type is supported 
for compatibility with other databases and older applications.
</li><li>Compatibility: Support for the data type notation 
CHARACTER VARYING.
</li><li>File names starting with ~ are now in the user directory 
(Java system property user.home)
</li><li>Can now ORDER BY -1 (meaning order by first column, 
descending), and ORDER BY ? (parameterized column number).
</li><li>Linked tables can now emit UPDATE statements if 
''EMIT UPDATES'' is specified in the CREATE LINKED TABLE statement. 
So far, updating a row always deleted the old row and then inserted 
the new row.
</li><li>New functions LEAST and GREATEST to get the smallest or 
largest value from a list.
</li><li>For most IOExceptions now the file name is included in 
the error message.
</li><li>New method Csv.write(Writer writer, ResultSet rs)
</li><li>The table id (important for LOB files) is now included in 
INFORMATION_SCHEMA.TABLES.
</li><li>The aggregate function COUNT(...) now returns a long 
instead of an int.
</li>
</ul>
<b>Bugfixes:</b>
<ul>
<li>In the last release, the H2 Console opened two connection 
when logging into a database, and only closed one connection 
when logging out. Fixed.
</li><li>In many situations, views did not use an index if they 
could have. Fixed. Also the explain plan for views works now.
</li><li>Server mode: the server stack trace was included in 
SQLException messages. Fixed.
</li><li>Databases with invalid linked tables (for example, because 
the target database is not accessible) can now be opened. Old 
table links don''t work however.
</li><li>In INSERT and MERGE statements, each column may 
only be specified once now.
</li><li>A java.util.Date object is now converted to a TIMESTAMP 
in the JDBC API. Previously it was converted to a DATE.
</li><li>After calling SHUTDOWN and closing the connection and 
a superfluous error message appeared in the trace file. Fixed.
</li><li>When using DISTINCT, ORDER BY a function works now 
as long as it is in the column list.
</li><li>The ''ordering'' of data types was not always correct, 
for example an operation involving REAL and DOUBLE produced 
a result of type REAL. Fixed.
</li><li>CSV tool: If the same instance was used for reading 
and writing, the tool wrote the column names twice. Fixed.
</li><li>There was a small memory leak in the trace module. 
One object per opened connection was kept in a hash map.
</li>
</ul>
For future plans, see the new ''Roadmap'' page on the web site.
');

SELECT 'newsfeed-rss.xml' FILE,
    XMLSTARTDOC() ||
    XMLNODE('rss', XMLATTR('version', '2.0'),
        XMLNODE('channel', NULL,
            XMLNODE('title', NULL, C.TITLE) ||
            XMLNODE('link', NULL, C.LINK) ||
            XMLNODE('description', NULL, C.DESC) ||
            XMLNODE('language', NULL, C.LANGUAGE) ||
            XMLNODE('pubDate', NULL, FORMATDATETIME(C.PUB, 'EEE, d MMM yyyy HH:mm:ss z', 'en', 'GMT')) ||
            XMLNODE('lastBuildDate', NULL, FORMATDATETIME(C.LAST, 'EEE, d MMM yyyy HH:mm:ss z', 'en', 'GMT')) ||
            GROUP_CONCAT(
                XMLNODE('item', NULL,
                    XMLNODE('title', NULL, I.TITLE) ||
                    XMLNODE('link', NULL, C.LINK) ||
                    XMLNODE('description', NULL, XMLCDATA(I.TITLE))
                )
            ORDER BY I.ID DESC SEPARATOR '')
        )
    ) CONTENT
FROM CHANNEL C, ITEM I
UNION
SELECT 'newsfeed-atom.xml' FILE,
    XMLSTARTDOC() ||
    XMLNODE('feed', XMLATTR('version', '0.3') || XMLATTR('xmlns', 'http://purl.org/atom/ns#') || XMLATTR('xml:lang', C.LANGUAGE),
        XMLNODE('title', XMLATTR('type', 'text/plain') || XMLATTR('mode', 'escaped'), C.TITLE) ||
        XMLNODE('author', NULL, XMLNODE('name', NULL, C.AUTHOR)) ||
        XMLNODE('link', XMLATTR('rel', 'alternate') || XMLATTR('type', 'text/html') || XMLATTR('href', C.LINK), NULL) ||
        XMLNODE('modified', NULL, FORMATDATETIME(C.LAST, 'yyyy-MM-dd''T''HH:mm:ss.SSS', 'en', 'GMT')) ||
        GROUP_CONCAT(
            XMLNODE('entry', NULL,
                XMLNODE('title', XMLATTR('type', 'text/plain') || XMLATTR('mode', 'escaped'), I.TITLE) ||
                XMLNODE('link', XMLATTR('rel', 'alternate') || XMLATTR('type', 'text/html') || XMLATTR('href', C.LINK), NULL) ||
                XMLNODE('id', NULL, XMLTEXT(C.LINK || '/' || I.ID)) ||
                XMLNODE('issued', NULL, FORMATDATETIME(I.ISSUED, 'yyyy-MM-dd''T''HH:mm:ss.SSS', 'en', 'GMT')) ||
                XMLNODE('modified', NULL, FORMATDATETIME(I.ISSUED, 'yyyy-MM-dd''T''HH:mm:ss.SSS', 'en', 'GMT')) ||
                XMLNODE('content', XMLATTR('type', 'text/html') || XMLATTR('mode', 'escaped'), XMLCDATA(I.DESC))
            )
        ORDER BY I.ID DESC SEPARATOR '')
    ) CONTENT
FROM CHANNEL C, ITEM I
UNION
SELECT '-newsletter-' FILE, I.DESC CONTENT FROM ITEM I WHERE I.ID = (SELECT MAX(ID) FROM ITEM)
