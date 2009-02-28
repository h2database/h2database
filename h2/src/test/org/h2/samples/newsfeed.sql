/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

CREATE TABLE CHANNEL(TITLE VARCHAR, LINK VARCHAR, DESC VARCHAR,
    LANGUAGE VARCHAR, PUB TIMESTAMP, LAST TIMESTAMP, AUTHOR VARCHAR);

INSERT INTO CHANNEL VALUES('H2 Database Engine' ,
    'http://www.h2database.com', 'H2 Database Engine', 'en-us', NOW(), NOW(), 'Thomas Mueller');

CREATE TABLE ITEM(ID INT PRIMARY KEY, TITLE VARCHAR, ISSUED TIMESTAMP, DESC VARCHAR);

INSERT INTO ITEM VALUES(58,
'New version available: 1.1.108 (beta; 2009-02-28)', '2009-02-28 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>Support for multiple read-write connections without starting a server.
</li><li>MySQL compatibility for CREATE TABLE is improved.
</li><li>The exception message of failed INSERT statements now includes all values.
</li><li>The DbStarter now closes all connections to the configured database.
</li><li>Improved exception message when connecting to a just started server fails.
</li><li>Connection.isValid is a bit faster.
</li><li>H2 Console: The autocomplete feature has been improved a bit.
</li><li>When restarting a web application in Tomcat, an exception was thrown sometimes.
    The root cause of the problem is now documented in the FAQ.
</li></ul>
<b>Bugfixes:</b>
<ul><li>When the shutdown hook closed the database, the last log file
    was deleted too early. This could cause uncommitted changes to be persisted.
</li><li>The database file locking mechanism didn't work correctly on Mac OS.
</li><li>Recovery did not work if there were more than 255 lobs stored as files.
</li><li>If opening a database failed with an out of memory exception, some files were not closed.
</li><li>The WebServlet did not close the database when un-deploying the web application.
</li><li>JdbcConnectionPool: it was possible to set a negative connection pool size.
</li><li>Fulltext search did not support table names with a backslash.
</li><li>A bug in the internal IntArray was fixed.
</li><li>The H2 Console web application (war file) now supports all Unicode characters.
</li><li>DATEADD does no longer require that the argument is a timestamp.
</li><li>Some built-in functions reported the wrong precision, scale, and display size.
</li><li>Optimizer: the expected runtime calculation was incorrect. The fixed calculation
    should give slightly better query plans when using many joins.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(57,
'New version available: 1.1.107 (beta; 2009-01-24)', '2009-01-24 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>Enabling the trace mechanism by creating a file is no longer supported.
</li><li>The MySQL compatibility extension fromUnixTime now used the English locale.
</li></ul>
<b>Bugfixes:</b>
<ul><li>Some DatabaseMetaData operations did not work for non-admin users.
</li><li>When using LOG=2, the index file grew quickly in some situations.
</li><li>In versions 1.1.105-106, old encrypted script files could not be processed.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(56,
'New version available: 1.1.106 (beta; 2009-01-04)', '2009-01-04 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>The license change a bit: so far the license was modified to say
    'Swiss law'. This is now changed back to the original 'US law'.
</li><li>CREATE DOMAIN: Built-in data types can now only be changed if no tables exist.
</li><li>DatabaseMetaData.getPrimaryKeys: The column PK_NAME now contains the
    constraint name instead of the index name (compatibility for PostgreSQL and Derby).
</li></ul>
<b>Bugfixes:</b>
<ul><li>Statement.setQueryTimeout did not work correctly for some statements.
</li><li>Linked tables: a workaround for Oracle DATE columns has been implemented.
</li><li>Using IN(..) inside a IN(SELECT..) did not always work.
</li><li>Views with IN(..) that used a view itself did not work.
</li><li>Union queries with LIMIT or ORDER BY that are used in a view or subquery did not work.
</li><li>Constraints for local temporary tables now session scoped. So far they were global.
    Thanks a lot to Eric Faulhaber for finding and fixing this problem!
</li><li>When using the auto-server mode, and if the lock file was modified in the future,
    the wrong exception was thrown.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(55,
'New version available: 1.1.105 (beta; 2008-12-19)', '2008-12-19 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>New meta data column TABLES.LAST_MODIFICATION.
</li><li>The setting STORAGE=TEXT is no longer supported.
</li><li>Natural join: the joined columns are not repeated any more.
</li><li>MySQL compatibility: support for := assignment.
</li><li>INSERT INTO TEST(SELECT * FROM TEST) is now supported.
</li><li>H2 Console: Columns are now listed for up to 500 tables.
</li><li>H2 Console: support for the 'command' key.
</li><li>JaQu: the maximum length of a column can now be defined.
</li><li>The fulltext search documentation has been improved.
</li><li>Ridvan Agar has completed the Turkish translation.
</li></ul>
<b>Bugfixes:</b>
<ul><li>The tool DeleteDbFiles could deleted LOB files of other databases.
</li><li>When used in a subquery, LIKE and IN(..) did not work correctly.
</li><li>ARRAY_GET returned the wrong data type.
</li><li>User defined aggregate functions: the method getType was incorrect.
</li><li>User defined aggregate functions did not work sometimes.
</li><li>Each session threw an invisible exception when garbage collected.
</li><li>Foreign key constraints that refer to a quoted column did not work.
</li><li>Shell: line comments didn't work correctly.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

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
