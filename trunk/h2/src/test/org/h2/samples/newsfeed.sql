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

INSERT INTO ITEM VALUES(65,
'New version available: 1.1.115 (2009-06-27)', '2009-06-27 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>The new storage mechanism is now alpha quality.
    To try it out, enable the system property "h2.pageStore" to "true".
    The database file size is smaller, and there is only one file.
</li><li>java.util.UUID is now supported.
</li><li>H2 Console: improved Polish translation.
</li><li>The download page now included the SHA1 checksums.
</li><li>Shell tool: the file encoding workaround is now documented.
</li><li>Data types: LONG is now an alias for BIGINT.
</li></ul>
<b>Bugfixes:</b>
<ul><li>ALTER TABLE could throw an exception "object already exists".
</li><li>Views: in some situations, an ArrayIndexOutOfBoundsException was thrown.
</li><li>H2 Console: the language was reset to the browser language.
</li><li>Server-less multi-connection mode: more bugs are fixed.
</li><li>RunScript did not work with LZF.
</li><li>Fulltext search: searching for NULL or an empty string threw an exception.
</li><li>Lucene fulltext search: FTL_DROP_ALL did not drop triggers.
</li><li>Backup: the backup could included a file entry for the LOB directory.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(64,
'New version available: 1.1.114 (2009-06-01)', '2009-06-01 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>Java 1.5 is now required to run H2.
</li><li>Fulltext search: Data is no longer deleted and
    re-inserted if there was no change.
</li><li>Microsoft Windows: when using the the installer, Vista wrote
    "This program may not have installed correctly."
    This message should no longer appear.
</li></ul>
<b>Bugfixes:</b>
<ul><li>ResultSetMetaData.getColumnClassName returned the wrong
    class for CLOB and BLOB columns.
</li><li>In some situations, an ArrayIndexOutOfBoundsException was
    thrown when adding rows.
</li><li>The Recover tool did not always work.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(63,
'New version available: 1.1.113 (2009-05-21)', '2009-05-21 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>This is the last version compiled against Java 1.4.
</li><li>JDK 1.5 is now required to build the jar file.
</li><li>A second level soft-references cache is now supported.
    It speeds up large databases, but reduces performance for small
    databases. Thanks a lot to Jan Kotek for the patch!
</li><li>MS SQL Server compatibility: support for linked tables with
    NVARCHAR, NCHAR, NCLOB, and LONGNVARCHAR.
</li><li>Android workaround for read-only databases in zip files.
</li><li>Calling execute() or prepareStatement() with null as the
    SQL statement now throws an exception.
</li><li>H2 Console: command line settings are no longer stored.
</li></ul>
<b>Bugfixes:</b>
<ul><li>When deleting or updating many rows in a table, the space
    in the index file was not re-used.
</li><li>Identifiers with a digit and then a dollar sign didn't work.
</li><li>Shell tool: the built-in commands didn't work with a semicolon.
</li><li>Benchmark: the number of executed statements was incorrect.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(62,
'New version available: 1.1.112 (2009-05-01)', '2009-05-01 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>MERGE now returns 0 as the generated on update.
</li><li>A file system implementation can now be registered.
</li><li>The database file system is no longer included.
</li><li>EclipseLink: Added H2Platform.supportsIdentity().
</li><li>Connection pool: the login timeout is now 5 minutes.
</li></ul>
<b>Bugfixes:</b>
<ul><li>Opening large databases could become slow.
</li><li>GROUP BY queries with a self-join were wrong sometimes.
</li><li>Bugs in the server-less multi-connection mode have been fixed.
</li><li>JdbcPreparedStatement.toString() could fail.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(61,
'New version available: 1.1.111 (2009-04-10)', '2009-04-10 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>In-memory databases can now run inside the Google App Engine.
</li><li>The Shell tool no longer truncates results with only one column.
</li></ul>
<b>Bugfixes:</b>
<ul><li>Queries that are ordered by an indexed column returned no rows in certain cases.
</li><li>The wrong exception was thrown when using unquoted text for some SQL statements.
</li><li>The built-in connection pool did not roll back transactions and
    enable autocommit enabled after closing a connection.
</li><li>Sometimes a StackOverflow occurred when checking for deadlock.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(60,
'New version available: 1.1.110 (2009-04-03)', '2009-04-03 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>Improved OSGi support.
</li><li>Support for non-persistent tables in regular databases.
    Thanks a lot to Sergi Vladykin for the patch!
</li><li>Creating a JdbcConnectionPool has been simplified a bit.
</li><li>Improved Javadoc navigation (similar to Scaladoc).
</li><li>The API of the tools changed a bit.
</li><li>The FTP server is no longer included in the h2*.jar file.
</li><li>Linked tables to SQLite database can now be created.
</li><li>CREATE TABLE: improved compatibility with other databases.
</li><li>Improved error message for unsupported features.
</li><li>H2 Console: the browser setting now supports arguments.
</li></ul>
<b>Bugfixes:</b>
<ul><li>The built-in JdbcConnectionPool is now about 70 times faster.
</li><li>The H2 Console no longer trims the password.
</li><li>ResultSet.findColumn now also checks for column names, not only labels.
</li><li>Nested IN(IN(...)) didn't work.
</li><li>NIO storage: the nio: prefix was using memory mapped files.
</li><li>Deterministic user defined functions did not work.
</li><li>JdbcConnectionPool.setLoginTimeout with 0 was broken.
</li><li>The data type of a SUBSTRING method was wrong.
</li><li>H2 Console: auto-complete of identifiers did not work correctly.
</li><li>DISTINCT and GROUP BY on a CLOB column was broken.
</li><li>Some internal caches did not use the LRU mechanism.
</li><li>DatabaseMetaData.getSQLKeywords now returns the correct list.
</li><li>More bugs in the server-less multi-connection mode have been fixed.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(59,
'New version available: 1.1.109 (2009-03-14)', '2009-03-14 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>New experimental NIO storage (faster on Mac OS
    but slower on Windows). Thanks a lot to Jan Kotek!
</li><li>User defined functions can now be deterministic.
</li><li>New system function TRANSACTION_ID().
</li><li>Bit functions and MOD now use BIGINT.
</li><li>The optimization for IN(...) is now only used if it helps.
</li></ul>
<b>Bugfixes:</b>
<ul><li>Could not use a linked table multiple times in the same query.
</li><li>Multiple nested queries with parameters did not always work.
</li><li>When converting CLOB to BINARY, each character resulted in one byte.
</li><li>Bugs in the server-less multi-connection mode have been fixed.
</li><li>Column names could not be named "UNIQUE" (with the quotes).
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(58,
'New version available: 1.1.108 (2009-02-28)', '2009-02-28 12:00:00',
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
