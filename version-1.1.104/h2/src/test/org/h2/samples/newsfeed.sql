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

INSERT INTO ITEM VALUES(54,
'New version available: 1.1.104 (beta; 2008-11-28)', '2008-11-28 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>ResultSet.getObject for a lob will return java.sql.Clob / Blob.
</li><li>The interface CloseListener has a new method 'remove'.
</li><li>Compatibility for MS SQL Server DATEDIFF(YYYY, .., ..)
</li><li>The emergency reserve file has been removed.
</li><li>The H2DatabaseProvider for ActiveObjects is now included.
</li><li>The H2Platform for Oracle Toplink Essential has been improved.
</li><li>Build: JAVA_HOME is now automatically detected on Mac OS X.
</li><li>The cache memory usage calculation is more conservative.
</li><li>Large databases on FAT file system are now supported.
</li><li>The database now tries to detect if the web application is stopped.
</li></ul>
<b>Bugfixes:</b>
<ul><li>Fulltext search: a memory leak has been fixed.
</li><li>A query with group by that was used like a table could throw an exception.
</li><li>JaQu: tables are now auto-created when running a query.
</li><li>The optimizer had problems with function tables.
</li><li>The function SUM could overflow when using large values.
</li><li>The function AVG could overflow when using large values.
</li><li>Testing for local connections was very slow on some systems.
</li><li>Allocating space got slower and slower the larger the database.
</li><li>ALTER TABLE ALTER COLUMN could throw the wrong exception.
</li><li>Updatable result sets: the key columns can now be updated.
</li><li>The Windows service to start H2 didn't work in version 1.1.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(53,
'New version available: 1.1.103 (beta; 2008-11-07)', '2008-11-07 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>New column INFORMATION_SCHEMA.COLUMNS.SEQUENCE_NAME.
</li><li>Aliases for built-in data types can now be re-mapped.
</li><li>Improved PostgreSQL compatibility for NEXTVAL and CURRVAL.
</li><li>The Japanese translation has been completed by Masahiro Ikemoto. 
</li><li>New system property h2.browser to set the browser to use.
</li><li>To start the browser, java.awt.Desktop.browse is now used if available.
</li><li>Less heap memory is needed when multiple databases are open.
</li></ul>
<b>Bugfixes:</b>
<ul><li>Could not order by a formula when the formula was in the group by list 
    but not in the select list.
</li><li>Date values that match the daylight saving time end were not allowed in 
    times zones were the daylight saving time ends at midnight, for years larger than 2037. 
    This is a problem of Java, however a workaround is implemented in H2 that solves 
    most problems (except the problems of java.util.Date itself).
</li><li>ALTER TABLE used a lot of memory when using multi-version concurrency.
</li><li>Referential integrity for in-memory databases didn't work in some cases.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(52,
'New version available: 1.1.102 (beta; 2008-10-24)', '2008-10-24 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>The French translation of the H2 Console has been improved by Olivier Parent.
</li><li>Translating the H2 Console is now simpler.
</li><li>Common exception (error code 23*) are no longer written to the .trace.db file by default.
</li></ul>
<b>Bugfixes:</b>
<ul><li>ResultSetMetaData.getColumnName now returns the alias name except for columns.
</li><li>Temporary files are now deleted when the database is closed, even
    if they were not garbage collected so far.
</li><li>There was a memory leak when creating and dropping tables and 
    indexes in a loop (persistent database only).
</li><li>SET LOG 2 was not effective if executed after opening the database.
</li><li>In-memory databases don't write LOBs to files any longer.
</li><li>Self referencing constraints didn't restrict deleting rows that reference
    itself if there is another row that references it.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(51,
'New version available: 1.1.101 (beta; 2008-10-17)', '2008-10-17 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>OSGi meta data is included in the manifest file. 
</li><li>Queries with more than 10 tables are now faster.
</li><li>Opening large database is now faster.
</li><li>Opening a connection with AUTO_SERVER=TRUE is now fast.
</li><li>There is now a default timeout of 2 seconds to connect to a server.
</li><li>Improved Glassfish / Toplink support in H2Platform.
</li><li>New functions ISO_YEAR, ISO_WEEK, ISO_DAY_OF_WEEK.
</li><li>IF [NOT] EXISTS is supported for named constraints.
</li><li>The methods getTableName() and getColumnName() now return the real names.
</li><li>In SQL scripts created with SCRIPT TO, schemas are now only created if they don't exist yet.
</li><li>Local temporary tables now support indexes.
</li><li>RUNSCRIPT no longer uses a temporary file.
</li><li>New system table INFORMATION_SCHEMA.SESSION_STATE.
</li><li>After an automatic re-connect, part of the session state stays.
</li><li>After re-connecting to a database, the database event listener (if set) is informed about it.
</li><li>New system property h2.maxReconnect (default 3).
</li><li>The error messages have been translated to Spanish by Dario V. Fassi.
</li><li>The date functions DAYOF... are now called DAY_OF_... (the old names still work).
</li><li>Linked tables: compatibility with MS SQL Server has been improved.
</li><li>The default value for MAX_MEMORY_UNDO is now 50000.
</li><li>Fulltext search: new method FT_DROP_INDEX.
</li></ul>
<b>Bugfixes:</b>
<ul><li>Linked tables: the automatic connection sharing didn't work.
</li><li>The wrong parameters were bound to subqueries with parameters.
</li><li>Unset parameters were not detected when the query was re-compiled.
</li><li>An out of memory error could result in a strange exception.
</li><li>Renaming tables that have foreign keys didn't work.
</li><li>Auto-reconnect didn't work when using auto-server.
</li><li>The optimization to group using an index didn't work sometimes.
</li><li>The build didn't work if the directory temp didn't exist before.
</li><li>WHERE .. IN (SELECT ...) could throw a NullPointerException. 
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(50,
'New version available: 1.1.100 (beta; 2008-10-04)', '2008-10-04 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>In version H2 1.1, some features are now enabled by default.
</li><li>New auto-reconnect feature. 
    To enable, append ;AUTO_RECONNECT=TRUE to the database URL. 
</li><li>The H2 Console tool now works with the JDBC-ODBC bridge.
</li><li>The H2 Console tool now supports command line options.
</li><li>The h2console.war can now be built using the Java build.
</li><li>If you want that each connection opens its own database, append 
    ;OPEN_NEW=TRUE to the database URL.
</li><li>CreateCluster: the property 'serverlist' is now called 'serverList'.
</li><li>Databases names can now be one character long. 
</li></ul>
<b>Bugfixes:</b>
<ul><li>Connections from a local address other than 'localhost' were not allowed by default.
</li><li>Large objects did not work for in-memory databases in server mode in Linux.
</li><li>The ConvertTraceFile tool could not parse some files.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(49,
'New version available: 1.0.79 (2008-09-26)', '2008-09-26 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>Row level locking for MVCC is now enabled.
</li><li>Multiple processes can now access the same database by appending 
    ;AUTO_SERVER=TRUE to the database URL. 
</li><li>The database supports the SHOW command for better MySQL and PostgreSQL compatibility.
</li><li>Result sets with just a unique index can now be updated.
</li><li>Linked tables can now share the connection.
</li><li>Linked tables can now be read-only.
</li><li>Linked tables: the schema name can now be set.
</li><li>Linked tables: worked around a bug in Oracle with the CHAR data type.
</li><li>Temporary linked tables are now supported.
</li><li>Faster storage re-use algorithm thanks to Greg Dhuse from cleversafe.com.
</li><li>Faster hash code calculation for large binary arrays.
</li><li>Multi-Version Concurrency may no longer be used when using 
    the multi-threaded kernel feature.
</li><li>The H2 Console now abbreviates large texts in results.
</li><li>SET SCHEMA_SEARCH_PATH is now documented.
</li><li>Can now start a TCP server with port 0 (automatically select a port).
</li><li>The server tool now displays the correct IP address if networked.
</li></ul>
<b>Bugfixes:</b>
<ul><li>Multiple UNION queries could not be used in derived tables. 
</li><li>It was possible to create tables in read-only databases.
</li><li>SET SCHEMA did not work for views.
</li><li>The maximum log file size setting was ignored for large databases.
</li><li>The data type JAVA_OBJECT could not be used in updatable result sets.
</li><li>The system property h2.optimizeInJoin did not work correctly.
</li><li>Conditions such as ID=? AND ID&gt;? were slow.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(48,
'New version available: 1.0.78 (2008-08-28)', '2008-08-28 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>Column aliases can now be used in GROUP BY and HAVING.
</li><li>Java methods with variable number of parameters can now be used (for Java 1.5 or newer).
</li><li>The build target 'build jarSmall' now includes the embedded database.
</li><li>JdbcDataSource now keeps the password in a char array where possible.
</li><li>Jason Brittain has contributed MySQL date functions. Thanks a lot!
    They are not in the h2.jar file currently, but in src/tools/org/h2/mode/FunctionsMySQL.java.
    To install, add this class to the classpath and call FunctionsMySQL.register(conn) in the Java code.
</li><li>The Japanese translation has been improved by Masahiro Ikemoto. Thanks a lot!
</li><li>The documentation no longer uses a frameset (except the Javadocs).
</li></ul>
<b>Bugfixes:</b>
<ul><li>The H2 Console replaced an empty user name with a single space. 
</li><li>ResultSet.absolute did not always work with large result sets.
</li><li>When using DB_CLOSE_DELAY, sometimes a NullPointerException is thrown when
    the database is opened almost at the same time as it is closed automatically.
    Thanks a lot to Dmitry Pekar for finding this!
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);
    
INSERT INTO ITEM VALUES(47,
'New version available: 1.0.77 (2008-08-16)', '2008-08-16 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>JaQu is now using prepared statements and supports Date, Time, Timestamp.
</li><li>Support a comma before closing a list, as in: create table test(id int,)
</li><li>DB2 compatibility: the DB2 fetch-first-clause is supported.
</li><li>ResultSet.setFetchSize is now supported.
</li></ul>
<b>Bugfixes:</b>
<ul><li>When using remote in-memory databases, large LOB objects did not work.
</li><li>Timestamp columns such as TIMESTAMP(6) were not compatible to other database. 
</li><li>Opening a large database was slow if there was a problem opening the previous time.
</li><li>Oracle compatibility: old style outer join syntax using (+) did work correctly sometimes.
</li><li>MySQL compatibility: linked tables had lower case column names on some systems. 
</li><li>NOT IN(SELECT ...) was incorrect if the subquery returns no rows.
</li><li>CREATE TABLE AS SELECT did not work correctly in the multi-version concurrency mode.
</li><li>It has been reported that when using Install4j on some Linux systems and enabling the 'pack200' option, 
    the h2.jar becomes corrupted by the install process, causing application failure. 
    A workaround is to add an empty file h2.jar.nopack next to the h2.jar file. 
    The reason for this problem is not known.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);
    
INSERT INTO ITEM VALUES(46,
'New version available: 1.0.76 (2008-07-27)', '2008-07-27 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>Key values can now be changed in updatable result sets.
</li><li>Changes in updatable result sets are now always visible.
</li><li>There is a problem with Hibernate when using Boolean columns, see 
    http://opensource.atlassian.com/projects/hibernate/browse/HHH-3401
</li><li>The comment of a domain (user defined data type) is now used.
</li></ul>
<b>Bugfixes:</b>
<ul><li>ResultSetMetaData.getColumnClassName now returns the correct 
    class name for BLOB and CLOB.
</li><li>Fixed the Oracle mode: Oracle allows multiple rows only where 
    all columns of the unique index are NULL. 
</li><li>ORDER BY on tableName.columnName didn't work correctly if the column 
    name was also used as an alias.
</li><li>Invalid database names are now detected and a better error message is thrown.
</li><li>H2 Console: The progress display when opening a database has been improved.
</li><li>The error message when the server doesn't start has been improved.
</li><li>Temporary files were sometimes deleted too late when executing large insert, update, 
    or delete operations.
</li><li>The database file was growing after deleting many rows, and after large update operations.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

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
</li><li>Statements can now be canceled remotely 
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
