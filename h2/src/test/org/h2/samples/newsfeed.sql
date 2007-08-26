/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

CREATE TABLE CHANNEL(TITLE VARCHAR, LINK VARCHAR, DESC VARCHAR,
    LANGUAGE VARCHAR, PUB TIMESTAMP, LAST TIMESTAMP, AUTHOR VARCHAR);

INSERT INTO CHANNEL VALUES('H2 Database Engine' ,
    'http://www.h2database.com', 'H2 Database Engine', 'en-us', NOW(), NOW(), 'Thomas Mueller');

CREATE TABLE ITEM(ID INT PRIMARY KEY, TITLE VARCHAR, ISSUED TIMESTAMP, DESC VARCHAR);

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

INSERT INTO ITEM VALUES(23,
'New version available: 1.0 / 2007-04-29', '2007-04-29 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br />
<b>Changes and new functionality:</b>
<ul>
<li>New function TABLE to define ad-hoc (temporary) tables in a query. 
    This also solves problems with variable-size IN(...) queries: 
    instead of SELECT * FROM TEST WHERE ID IN(?, ?, ...) you can now write:
    SELECT * FROM TABLE(ID INT=?) X, TEST WHERE X.ID=TEST.ID
    In this case, the index is used.
</li><li>New data type ARRAY. Actually it was there before, but is now documented 
    and better tested (however it must still be considered experimental).
    The java.sql.Array implementation is incomplete, but setObject(1, new Object[]{...})
    and getObject(..) can be used. New functions ARRAY_GET and ARRAY_LENGTH.
</li><li>Autocomplete in the Console application: now the result frame scrolls to the top when the list is updated.
</li><li>SimpleResultSet now has some basic data type conversion features.
</li><li>The BACKUP command is better tested and documented.
    This means hot backup (online backup) is now possible.
</li><li>The old ''Backup'' tool is now called ''Script'' (as the SQL statement).
</li><li>There are new ''Backup'' and ''Restore'' tools that work with database files directly.
</li><li>The new function LINK_SCHEMA simplifies linking all tables of a schema.
</li><li>SCRIPT DROP now also drops aliases (Java functions) if they exist.
</li><li>For encrypted databases, the trace option can no longer be enabled manually by creating a file.
</li><li>For linked tables, NULL in the unique key is now supported.
</li><li>For read-only databases, temp files are now created in the default temp directory instead
    of the database directory.
</li><li>CSVWRITE now returns the number of rows written.
</li><li>The Portuguese (Europe) translation is available. Thanks a lot to Antonio Casqueiro!
</li><li>The error message for invalid views has been improved (the root cause is included in the message now).
</li><li>SQLException.getCause of the now works for JDK 1.4 and higher.
</li>
</ul>
<b>Bugfixes:</b>
<ul>
<li>Unnamed private in-memory database (jdbc:h2:mem:) were not ''private'' as documented. Fixed.
</li><li>GROUP BY expressions did not work correctly in subqueries. Fixed.
</li><li>When using JDK 1.5 or later, and switching on h2.lobFilesInDirectories, 
    the performance for creating LOBs was bad. This has been fixed, however
    creating lots of LOBs it is still faster when the setting is switched off.
</li><li>A problem with multiple unnamed dynamic tables (FROM (SELECT...)) has been fixed.
</li><li>Appending ''Z'' to a timestamp did not have an effect. Now it is interpreted as +00:00 (GMT).
</li><li>The complete syntax for referential and check constraints is now supported 
    when written as part of the column definition, behind PRIMARY KEY.
</li><li>CASE WHEN ... returned the wrong result when the condition evaluated to NULL.
</li><li>Sending CLOB data was slow in some systems when using the server version. Fixed.
</li><li>The data type of NULLIF was NULL if the first expression was a column. Now the data type is set correctly.
</li><li>Indexes (and other related objects) for local temporary tables where not dropped 
    when the session was closed. Fixed.
</li><li>ALTER TABLE did not work for tables with computed columns.
</li><li>If the index file was deleted, an error was logged in the .trace.db file. This is no longer done.
</li><li>IN(SELECT ...) was not working correctly if the subquery returned a NULL value. Fixed.
</li><li>DROP ALL OBJECTS did not drop constants.
</li><li>DROP ALL OBJECTS dropped the role PUBLIC, which was wrong. Fixed.
</li><li>CASE was parsed as a function if the expression was in (). Fixed.
</li><li>When ORDER BY was used together with DISTINCT, it was required to type the column
    name exactly in the select list and the order list exactly in the same way. 
    This is not required any longer.
</li>
</ul>
For future plans, see the new ''Roadmap'' page on the web site.
');

INSERT INTO ITEM VALUES(22,
'New version available: 1.0 / 2007-03-04', '2007-03-04 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br />
<b>Changes and new functionality:</b>
<ul>
<li>System sequences (automatically created sequences for IDENTITY or AUTO_INCREMENT columns) are now
    random (UUIDs) to avoid clashes when merging databases using RUNSCRIPT.
</li><li>Now the server tool (org.h2.tools.Server) terminates with an exit code if a problem occured.
</li><li>The JDBC driver is now loaded if the JdbcDataSource class is loaded.
</li><li>After renaming a user the password becomes invalid. This is now documented.
</li><li>Truncating a table is now allowed if the table references another table 
    (but still not allowed if the table is references by another table).
</li>
</ul>
<b>Bugfixes:</b>
<ul>
<li>The precision for linked tables was not correct for some data types, for example VARCHAR. Fixed.
</li><li>Many problems and bugs in the XA support (package javax.sql) have been fixed.
</li><li>ORDER BY picked the wrong column if the same column name (but with a different table name) 
    was used twice in the select list.
</li><li>When a subquery was used in the select list of a query, and GROUP BY was used at the same time,
    a NullPointerException could occur. Fixed.
</li><li>ORDER BY did not work when DISTINCT was used at the same time in some situations. Fixed.
</li><li>When using IN(...) on a case insensitive column (VARCHAR_IGNORECASE), 
    an incorrect optimization was made and the result was wrong sometimes.
</li><li>XAResource.recover didn''t work. Fixed. 
</li><li>XAResource.recover did throw an exception with the code XAER_OUTSIDE if there
    was no connection. Now the code is XAER_RMERR.  
</li><li>SCRIPT did not work correctly with BLOB or CLOB data. Fixed.
</li><li>BACKUP TO ''test.zip'' now works with encrypted databases and CLOB and BLOB data.
</li><li>The function CASE WHEN ... didn''t convert the returned value to the same data type,
    resulting in unexpected behavior in many cases. Fixed.
</li>
</ul>
For future plans, see the new ''Roadmap'' page on the web site.
');

INSERT INTO ITEM VALUES(21,
'New version available: 1.0 / 2007-01-30', '2007-01-30 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br />
<b>Changes and new functionality:</b>
<ul>
<li>Experimental online backup feature using the SQL statement BACKUP TO ''fileName''.</li>
<li>Temporary files are now deleted earlier.</li>
<li>Benchmark: Added a multi-client test case, BenchB (similar to TPC-B).</li>
<li>The Console is now translated to Hungarian thanks to Andras Hideg, and to Indonesian thanks to Joko Yuliantoro.</li>
<li>Can now use UUID columns as generated key values.</li>
</ul>
<b>Bugfixes:</b>
<ul>
<li>In some situations, it was possible that SUM threw a class cast exception.</li>
<li>Compatibility: SCHEMA_NAME.SEQUENCE_NAME.NEXTVAL now works as expected.</li>
<li>XAConnection: A NullPointerException was thrown if addConnectionEventListener was called before opening the connection.</li>
<li>In case the result set of a subquery was re-used, an exception was throws if the subquery result did not fit in memory.</li>
<li>The command "drop all objects delete files" did not work on Linux.</li>
<li>DataSource: improved exception when setting the URL to an empty string.</li>
<li>Parsing of LIKE .. ESCAPE did not stop at the expected point.</li>
<li>Forum subscriptions (the emails sent from the forum) now work.</li>
</ul>
For future plans, see the new ''Roadmap'' page on the web site.
');

INSERT INTO ITEM VALUES(20,
'New version available: 1.0 / 2007-01-17', '2007-01-17 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br />
<b>Changes and new functionality:</b>
<ul>
<li>The Console is now translated to Japanese thanks to 
    IKEMOTO, Masahiro (ikeyan (at) arizona (dot) ne (dot) jp).
</li><li>The database engine can now be compiled with JDK 1.3 using ant codeswitch. 
    There are still some limitations, and the ant script to build the jar does not work yet.
</li><li>SCRIPT NODATA now writes the row count for each table.
</li><li>Timestamps with timezone information (Z or +/-hh:mm) and dates before year 1 
    can now be parsed. However dates before year 1 are not formatted correctly.
</li></ul>
<b>Bugfixes:</b>
<ul>
<li>Fixed a problem where data in the log file was not written to the data file 
    (recovery failure) after a crash, if an index was deleted previously.
</li><li>Setting the collation (SET COLLATOR) was very slow on some systems (up to 24 seconds).
</li><li>Selecting a column using the syntax schemaName.tableName.columnName did not work in all cases.
</li><li>When stopping the TCP server from an application and immediately afterwards starting 
    it again using a different TCP password, an exception was thrown sometimes.
</li><li>Now PreparedStatement.setBigDecimal(..) can only be called with an object of 
    type java.math.BigDecimal. Derived classes are not allowed any more. Many thanks to 
    Maciej Wegorkiewicz for finding this problem.
</li><li>It was possible to manipulate values in the byte array after calling PreparedStatement.setBytes, 
    and this could lead to problems if the same byte array was used again. Now the byte array 
    is copied if required.
</li><li>Date, time and timestamp objects were cloned in cases where it was not required. Fixed. 
</li></ul>
For future plans, see the new ''Roadmap'' page on the web site.
');

INSERT INTO ITEM VALUES(19,
'New version available: 1.0 / 2007-01-02', '2007-01-02 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br />
<b>Changes and new functionality:</b>
<ul>
<li>H2 is now available in Maven. The groupId is com.h2database,     the 
    artifactId is h2 and the version 1.0.20061217 (the new version will be 
    available in a few days). To create the maven artifacts yourself, use 
    ''ant mavenUploadLocal'' and ''ant mavenBuildCentral''.
</li><li>Many settings are now initialized from system properties and can be 
    changed on the command line without having recompile the database. 
    See Advances / Settings Read from System Properties.
</li><li>The (relative or absolute) directory where the script files are stored 
    or read can now be changed using the system property h2.scriptDirectory
</li><li>Client trace files now created in the directory ''trace.db'' and no 
    longer the application directory. This can be changed using the system 
    property h2.clientTraceDirectory.
</li><li>Build: Now using ant-build.properties. The JDK is automatically updated 
    when using ant codeswitch...
</li><li>Cluster: Now the server can detect if a query is read-only, and in this 
    case the result is only read from the first cluster node. However, there 
    is currently no load balancing made to avoid problems with transactions 
    / locking.
</li></ul>
<b>Bugfixes:</b>
<ul>
<li>If a CLOB or BLOB was deleted in a transaction and the database crashed 
    before the transaction was committed or rolled back, the object was lost if 
    it was large. Fixed.
</li><li>Prepared statements with non-constant functions such as 
    CURRENT_TIMESTAMP() did not get re-evaluated if the result of the 
    function changed. Fixed.
</li><li>In some situations the log file got corrupt if the process was terminated 
    while the database was opening.
</li><li>Using ;RECOVER=1 in the database URL threw a syntax exception. Fixed.
</li><li>It was possible to drop the sequence of a temporary tables with DROP 
    ALL OBJECTS, resulting in a null pointer exception afterwards.
</li></ul>
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