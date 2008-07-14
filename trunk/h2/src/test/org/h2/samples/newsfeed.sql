/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0 
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

CREATE TABLE CHANNEL(TITLE VARCHAR, LINK VARCHAR, DESC VARCHAR,
    LANGUAGE VARCHAR, PUB TIMESTAMP, LAST TIMESTAMP, AUTHOR VARCHAR);

INSERT INTO CHANNEL VALUES('H2 Database Engine' ,
    'http://www.h2database.com', 'H2 Database Engine', 'en-us', NOW(), NOW(), 'Thomas Mueller');

CREATE TABLE ITEM(ID INT PRIMARY KEY, TITLE VARCHAR, ISSUED TIMESTAMP, DESC VARCHAR);

INSERT INTO ITEM VALUES(45,
'New version available: 1.0.75 (2008-07-14)', '2008-07-14 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>The JaQu (Java Query) tool has been improved.
</li><li>The H2 Console can be started with an open connection to inspect a database while debugging.
</li><li>The referential constraint checking performance has been improvement.
</li></ul>
<b>Bugfixes:</b>
<ul><li>Running out of memory could result in incomplete transactions or corrupted databases. Fixed.
</li><li>CSVREAD did not process NULL correctly when using a whitespace field separator.
</li><li>Stopping a WebServer didn't always work. Fixed.
</li><li>Sometimes, order by in a query that uses the same table multiple times didn't work.
</li><li>A multi version concurrency (MVCC) problem has been fixed.
</li><li>Some views with multiple joined tables didn't work.
</li><li>The Oracle mode now allows multiple rows with NULL in a unique index.
</li><li>Some database metadata calls returned the wrong data type for DATA_TYPE columns.
</li><li>A bug int the Lucene fulltext implementation has been fixed.
</li><li>The character '$' could not be used in identifier names.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(44,
'New version available: 1.0.74 (2008-06-21)', '2008-06-21 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>JaQu (Java Query), a tool similar to LINQ (Language Integrated Query) 
    is now included. See also
    <a href="http://code.google.com/p/h2database/source/browse/trunk/h2/src/test/org/h2/test/jaqu/SamplesTest.java">
    code examples</a>.
</li><li>Support for overloaded Java methods. Many thanks to Gary Tong!
</li><li>Deadlocks are now detected.
</li><li>Linked tables: statements executed against the target are list with trace level 3.
</li><li>RunScript tool: new options to show and check the results of queries.
</li><li>Improved compatibility with databases that only allow one row with 'NULL' in a unique 
    index. Use the compatibility mode to enable this feature.
</li><li>The source code is now switched to Java 1.6 by default.
</li><li>The ChangePassword tool is now called ChangeFileEncryption.
</li><li>It is no longer allowed to create columns with the data type NULL.
</li></ul>
<b>Bugfixes:</b>
<ul><li>The Lucene fulltext index was always re-created when opening a database.
</li><li>Setting a column default with a different data type did not work.
</li><li>Opening big databases was sometimes very slow. Fixed.
</li><li>RUNSCRIPT could throw a NullPointerException.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(43,
'New version available: 1.0.73 (2008-05-31)', '2008-05-31 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>ParameterMetaData now returns the right data type for most cases.
</li><li>New column INFORMATION_SCHEMA.CONSTRAINTS.UNIQUE_INDEX_NAME.
</li><li>Some SET statements no longer commit a transaction. 
</li><li>The table SYSTEM_RANGE now supports parameters.
</li><li>The SCRIPT command does now emit IF NOT EXISTS for CREATE ROLE.
</li><li>Improved MySQL compatibility for AUTO_INCREMENT columns.
</li><li>The aggregate functions BOOL_OR and BOOL_AND are now supported.
</li><li>Negative scale values are now supported.
</li><li>Infinite numbers and NaN are now better supported.
</li><li>The fulltext search now supports CLOB.
</li><li>A right can now be granted multiple times.
</li></ul>
<b>Bugfixes:</b>
<ul><li>Disconnecting or unmounting drives while the database is open 
    now throws the right exception.
</li><li>The H2 Console could not be shut down from within the tool.
</li><li>If the password was passed as a char array, it was kept in an internal buffer
        longer than required. Theoretically the password could have been stolen
        if the main memory was swapped to disk before the garbage collection was run.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(42,
'New version available: 1.0.72 (2008-05-10)', '2008-05-10 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>SLF4J is now supported by using adding TRACE_LEVEL_FILE=4
        to the database URL.
</li><li>A subset of the PostgreSQL 'dollar quoting' feature is now supported.
</li><li>Updates made to updatable rows are now visible within the same result set. 
        DatabaseMetaData.ownUpdatesAreVisible now returns true.
</li><li>ParameterMetaData now returns the correct data 
        for INSERT and UPDATE statements.
</li><li>Shell tool: DESCRIBE now supports an schema name.
</li><li>The Shell tool now uses java.io.Console to read the password
        when using JDK 1.6
</li><li>The Japanese translation of the error messages and the H2 Console 
        has been completed by Masahiro Ikemoto (Arizona Design Inc.)
</li><li>Statements can now be cancelled remotely 
        (when using remote connections).
</li><li>Triggers are no longer executed when executing an changing the table
        structure (ALTER TABLE).
</li></ul>
<b>Bugfixes:</b>
<ul><li>Some databases could not be opened when appending 
        ;RECOVER=1 to the database URL.
</li><li>The recovery tool did not work if the table name contained spaces
        or if there was a comment on the table.
</li><li>When setting BLOB or CLOB values larger than 65 KB using 
        a remote connection, temporary files were kept on the client
        longer than required (until the connection was closed or the 
        object is garbage collected). Now they are removed as soon
        as the PreparedStatement is closed, or when the value is
        overwritten.
</li><li>When using read-only databases and setting LOG=2, an exception
        was written to the trace file when closing the database. Fixed.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(41,
'New version available: 1.0.71 (2008-04-25)', '2008-04-25 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>H2 is now dual-licensed under the Eclipse Public License (EPL) and the
    old ''H2 License'' (which is basically MPL).
</li><li>New traditional Chinese translation. Thanks a lot to Derek Chao!
</li></ul>
<b>Bugfixes:</b>
<ul><li>Sometimes an exception ''File ID mismatch'' or ''try to add a record twice''
    occurred after large records (8 KB or larger) are updated or deleted.
    See also http://code.google.com/p/h2database/issues/detail?id=22
</li><li>H2 Console: The tools can now be translated 
    (it didn''t work in the last release).
</li><li>Indexes were not used when enabling the optimization for 
    IN(SELECT...) (system property h2.optimizeInJoin).
</li></ul>
For details, see the ''Change Log'' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the ''Roadmap'' page at
http://www.h2database.com/html/roadmap.html
');

INSERT INTO ITEM VALUES(40,
'New version available: 1.0.70 (2008-04-20)', '2008-04-20 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>The plan is to dual-license H2. The additional license is EPL (Eclipse Public License).
    The current license (MPL, Mozilla Public License) will stay.
    Current users are not affected because they can keep MPL. 
    EPL is very similar to MPL, the only bigger difference is related to patents
    (EPL is a bit more business friendly in this regard).
    See also http://opensource.org/licenses/eclipse-1.0.php,
    http://www.eclipse.org/legal/eplfaq.php (FAQ),
    http://blogs.zdnet.com/Burnette/?p=131
</li><li>The ConvertTraceFile tool now generates SQL statement statistics.
</li><li>New system property h2.enableAnonymousSSL (default: true).
</li><li>The precision if SUBSTR is now calculated if possible.    
</li><li>The autocomplete in the H2 Console has been improved a bit.
</li><li>The tools in the H2 Console are now translatable.
</li><li>The servlet and lucene jar files are now automatically downloaded when building.
</li><li>The code switch tool has been replaced by a simpler tool. 
</li><li>Started to write a Ant replacement (''JAnt'') that uses pure Java 
    build definitions.Future plan: support creating custom h2 
    distributions (for embedded use). Maybe create a new project ''Jant''
    or ''Javen'' if other people are interested.
</li><li>The jar file is now about 10% smaller because the variable debugging info 
    is no longer included.
</li><li>Added shell scripts run.sh and build.sh.
</li><li>The Japanese translation of the error messages and the 
  H2 Console has been improved. Thanks a lot to Masahiro IKEMOTO. 
</li><li>Optimization for MIN() and MAX() when using MVCC.
</li><li>To protect against remote brute force password attacks, 
    the delay after each unsuccessful login now gets double as long.
</li><li>The built-in connection pool is not called JdbcConnectionPool. 
</li><li>Nested joins are now supported (A JOIN B JOIN C ON .. ON ..)
</li></ul>
<b>Bugfixes:</b>
<ul><li>Multi version concurrency (MVCC): when a row was updated, 
    and the updated column was not indexed, this update was visible sometimes 
    for other sessions even if it was not committed.
</li><li>Calling SHUTDOWN on one connection and starting a query on 
    another connection concurrently could result in a Java level deadlock.
</li><li>Databases in zip files: large queries are now supported.
</li><li>Invalid inline views threw confusing SQL exceptions.
</li><li>After setting the query timeout and then resetting it, the next query
    would still timeout. Fixed.
</li><li>Adding a IDENTITY column to a table with data threw a lock timeout.
</li><li>OutOfMemoryError could occur when using EXISTS or IN(SELECT ..).
</li></ul>
For details, see the ''Change Log'' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the ''Roadmap'' page at
http://www.h2database.com/html/roadmap.html
');

INSERT INTO ITEM VALUES(39,
'New version available: 1.0.69 (2008-03-29)', '2008-03-29 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>Most command line tools can now be called within the H2 Console.
</li><li>A new Shell tools is now included to query a database from the command line.
</li><li>Some command line options have changed (the old behavior is still supported).
</li><li>New system property h2.sortNullsHigh to invert the default NULL sorting.
</li><li>ALTER TABLE or CREATE TABLE now support parameters.
</li><li>TRACE_LEVEL_ settings are no longer persistent.
</li></ul>
<b>Bugfixes:</b>
<ul><li>When a log file switch occurred in the middle of certain operations,
    the database could not be started normally (RECOVER=1 was required).
</li><li>Altering a sequence didn''t unlock the system table with autocommit disabled.
</li><li>CSVWRITE caused a NullPointerException when not specifying a nullString.
</li><li>Years below 1 were not supported correctly.
</li><li>The recovery tool didn''t work correctly for tables without rows.
</li><li>It is no longer possible to create a role with the name of an existing user.
</li><li>The memory usage of native fulltext search has been improved.
</li><li>Performance was very slow when using LOG=2. 
</li><li>The linear hash has been removed because it was slow and sometimes incorrect.
</li></ul>
For details, see the ''Change Log'' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the ''Roadmap'' page at
http://www.h2database.com/html/roadmap.html
');

INSERT INTO ITEM VALUES(38,
'New version available: 1.0.68 (2008-03-15)', '2008-03-15 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>Faster text comparison when using a collator.
</li><li>CSVWRITE now supports a ''null string'' to parse NULL.
</li><li>UPDATE SET column=DEFAULT is now supported.
</li><li>Large SELECT DISTINCT and UNION queries are now supported
    (this is disabled by default).
</li><li>Committing inside a trigger is not allowed any more.
    </li><li>ALTER SEQUENCE can now be used inside a transaction.
</li><li>New system property h2.aliasColumnName. When enabled, aliased columns 
    return the real table and column name in ResultSetMetaData calls.
</li><li>Improved performance when using lob files in directories 
    (however this is still disabled by default)
</li><li>Fulltext search: new method SEARCH_DATA.
</li><li>New experimental optimization for GROUP BY queries.
</li><li>Better browser starting for the H2 Console on Linux.
</li><li>Improved support for IKVM.
</li></ul>
<b>Bugfixes:</b>
<ul><li>With MVCC=TRUE, duplicate rows could appear in the result set.
</li><li>Queries with many outer join tables were very slow.
</li><li>Unused LOB files were deleted much too late. 
</li><li>H2 Console: remote connections were very slow.
</li><li>H2 Console: autocomplete didn''t work with very large scripts.
</li><li>H2 Console: improved compatibility with Safari (Safari requires keep-alive)
</li><li>Random: the process didn''t stop if generateSeed was very slow.
</li><li>SELECT UNION with a different number of ORDER BY columns didn''t work.
</li><li>When using a view, the column precision was wrong sometimes.
</li><li>Some long running queries could not be cancelled.
</li><li>When using encrypted databases, and using the wrong file password,
    the log file was renamed sometimes.
</li></ul>
For details, see the ''Change Log'' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the ''Roadmap'' page at
http://www.h2database.com/html/roadmap.html
');

INSERT INTO ITEM VALUES(37,
'New version available: 1.0.67 (2008-02-22)', '2008-02-22 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>New function FILE_READ to read a file or from an URL.
</li><li>CREATE TABLE AS SELECT now supports a column list.
</li><li>The CSV tool now supports a custom lineSeparator.
</li><li>A user now has all rights on his own local temporary tables.
</li><li>Opening databases with ACCESS_MODE_DATA=r is now supported.
</li><li>Protection against password dictionary attacks in server mode.
</li><li>The value cache is improved to save memory.
</li><li>Large result sets are now a bit faster.
</li><li>ALTER SEQUENCE now support parameters.
</li><li>Statement.setQueryTimeout() is now supported.
</li><li>New session setting QUERY_TIMEOUT.
</li><li>The H2 Console has been translated to Dutch. Thanks a lot to Remco Schoen!
</li><li>The H2 Console is now faster for databases containing many tables.
</li><li>Databases can now be opened even if trigger classes are not in the classpath.
</li><li>Changing the transaction log level (SET LOG) is now written to the trace file.
</li><li>Primary key are now ordered before foreign key constraints.
</li></ul>
<b>Bugfixes:</b>
<ul><li>When using multiple connections, empty space was reused too early sometimes. 
    This could corrupt the database when recovering.
</li><li>The DbStarter servlet didn''t start the TCP listener even if configured.
</li><li>The user directory prefix (''~'') was ignored sometimes.
</li><li>Connecting to a TCP server and at shutting it down at the same time could fail.
</li><li>CREATE INDEX on a table with many rows used too much memory.
</li><li>A referential constraint to a table in a different schema could not be created.
</li></ul>
For details, see the ''Change Log'' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the ''Roadmap'' page at
http://www.h2database.com/html/roadmap.html
');

INSERT INTO ITEM VALUES(36,
'New version available: 1.0.66 (2008-02-02)', '2008-02-02 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>New tool: <a href="http://www.h2database.com/html/sourceError.html">Online Error Analyzer</a>.
</li><li>In the H2 Console, errors now link to the docs and source code.
</li><li>IKVM (www.ikvm.net) is now better supported.
</li><li>The exception ''Value too long for column'' now includes the data.
</li><li>Statements that contain very large subqueries are now faster.
</li><li>Fulltext search is now supported in named in-memory databases.
</li><li>Primary keys can now have a constraint name.
</li><li>Calling EXTRACT(HOUR FROM ...) returned the wrong values.
    Please check if your application relies on the old behavior before upgrading.
</li><li>The meta data compatibility with other databases has been improved.
</li></ul>
<b>Bugfixes:</b>
<ul><li>CHAR data type equals comparison was wrong.
</li><li>The table name was missing in the documentation of CREATE INDEX.
</li><li>The cache memory usage calculation has been improved.
</li><li>The exception "Hex string contains non-hex character" was not thrown.
</li><li>The acting as PostgreSQL server, a base directory was not set correctly.
</li><li>Variables: large objects now work correctly.
</li><li>H2 Console: multiple consecutive spaces in the setting name did not work.
</li></ul>
For details, see the ''Change Log'' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the ''Roadmap'' page at
http://www.h2database.com/html/roadmap.html
');

INSERT INTO ITEM VALUES(35,
'New version available: 1.0.65 (2008-01-18)', '2008-01-18 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>User defined variables are now supported.
</li><li>JNDI support in the H2 Console has been improved.
</li><li>The bind IP address can be set (system property h2.bindAddress).
</li><li>The build automatically switches the source code to the correct JDK.
</li><li>The SCRIPT command uses multi-row inserts to save space.
</li><li>Large result sets in the server mode are now faster.
</li><li>The performance for DROP has been improved. 
</li><li>Optimization for single column distinct queries with an index.
</li><li>LIKE comparisons are now faster.
</li><li>Encrypted databases are now faster.
</li><li>PostgreSQL compatibility: COUNT(T.*) is now supported. 
</li><li>The ChangePassword API has been improved. 
</li><li>The Ukrainian translation has been improved.
</li><li>CALL statements can now be used in batch updates.
</li><li>New read-only setting CREATE_BUILD.
</li></ul>
<b>Bugfixes:</b>
<ul><li>A recovery bug has been fixed.
</li><li>The optimizer did not always use the right indexes.
</li><li>BatchUpdateException.printStackTrace() could run out of memory. 
</li><li>Sometimes unused indexes where not dropped when altering a table.
</li><li>The SCRIPT command did not split up CLOB data correctly. 
</li><li>DROP ALL OBJECTS did not drop some objects. 
</li></ul>
For details, see the ''Change Log'' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the ''Roadmap'' page at
http://www.h2database.com/html/roadmap.html
');

INSERT INTO ITEM VALUES(34,
'New version available: 1.0.64 (2007-12-27)', '2007-12-27 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>An exclusive mode is now supported.
</li><li>The method Trigger.init has been changed. 
</li><li>New built-in functions RPAD and LPAD.
</li><li>New meta data table INFORMATION_SCHEMA.SESSIONS and LOCKS.
</li><li>H2 Console / autocomplete: Ctrl+Space now shows the list in all modes. 
</li><li>Loading classes and calling methods can be restricted.
</li><li>Thanks to Fulvio Biondi, the FTP server now supports a event listener.
</li><li>New system function CANCEL_SESSION.
</li><li>The H2 Console has been translated to Turkish by Ridvan Agar!
</li><li>H2 Console: when editing result sets, columns can now be set to null. 
</li><li>ResultSet methods with column name are now faster.
</li><li>Improved debugging support: toString now returns a meaningful text.
</li><li>The classes DbStarter and WebServlet have been moved to src/main. 
</li></ul>
<b>Bugfixes:</b>
<ul><li>The PostgreSQL ODBC driver did not work in the last release. 
</li><li>CSV tool: some escape/separator characters did not work.
</li><li>CSV tool: the character # could not be used as a separator. 
</li><li>3-way union queries could return the wrong results. 
</li><li>The MVCC mode did not work well with in-memory databases. 
</li><li>The Ukrainian translation was not working in the last release. 
</li><li>Creating many tables (many hundreds) was slow. 
</li><li>Opening a database with many indexes (thousands) was slow. 
</li><li>A stack trace thrown for systems with a slow secure random source.
</li><li>The column INFORMATION_SCHEMA.TRIGGERS.SQL is now correct.
</li><li>This database could not be used in applets. 
</li><li>The index file is now re-created automatically when required.
</li></ul>
For future plans, see the ''Roadmap'' page at
http://www.h2database.com/html/roadmap.html
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
SELECT 'newsletter.txt' FILE, I.DESC CONTENT FROM ITEM I WHERE I.ID = (SELECT MAX(ID) FROM ITEM)
