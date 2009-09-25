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

/*
<ul><li>The new page store mechanism is now alpha-level quality.
</li><li>New committer: Christian Peter. He works for Docware.
</li><li>The context class loader is used for user defined classes.
</li><li>Non-unique in-memory hash indexes are now supported.
</li><li>Improved performance for joins if indexes are missing.
</li><li>New system property h2.defaultMaxLengthInplaceLob.
</li><li>New system property h2.nullConcatIsNull.
</li><li>The Recover tool now also processes the log files.
</li><li>New sample application that shows how to pass data to a trigger.
</li><li>The cache algorithm TQ is disabled.
</li></ul>

<ul><li>ChangeFileEncryption did not work with Lob subdirectories.
</li><li>SELECT COUNT(*) FROM SYSTEM_RANGE(...) returned the wrong result.
</li><li>More bugs in the server-less multi-connection mode have been fixed.
</li><li>Updating many rows with LOB could throw an exception.
</li><li>The native fulltext index could leak memory.
</li><li>Statement.getConnection() didn't work if the connection was closed.
</li><li>Issue 121: JaQu: new simple update and merge methods.
</li><li>Issue 120: JaQu didn't close result sets.
</li><li>Issue 119: JaQu creates wrong WHERE conditions on some inputs.
</li></ul>

*/

INSERT INTO ITEM VALUES(68,
'New version available: 1.1.118 (2009-09-04)', '2009-09-04 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>Better optimizations for OR, IN(..), and IN(SELECT..) are available.
</li><li>Better support GaeVFS (Google App Engine Virtual File System).
</li><li>JaQu: the plan is to support pure Java / Scala conditions using de-compilation.
</li><li>Various tools now use Java 5 var-args.
</li><li>H2 Console: indexes in non-default schemas are now listed.
</li><li>H2 Console: PierPaolo Ucchino has completed the Italian translation. Thanks a lot!
</li><li>The stack trace of common exceptions is no longer logged.
</li></ul>
<b>Bugfixes:</b>
<ul><li>SHOW COLUMNS only listed indexed columns.
</li><li>When calling SHUTDOWN IMMEDIATELY, a file was not closed.
</li><li>DatabaseMetaData.getPrimaryKeys: the wrong constraint name was reported.
</li><li>AUTO_INCREMENT now does not create a primary key for ALTER TABLE.
</li><li>Native fulltext search: FT_INIT() now only needs to be called once.
</li><li>Various bugfixes and improvements in the page store mechanism.
</li><li>PreparedStatement.setObject now supports java.lang.Character.
</li><li>MVCC / duplicate primary key after rollback.
</li><li>MVCC / wrong exception is thrown.
</li><li>Sequence.NEXTVAL and CURRVAL did not respect the schema search path.
</li><li>The exception "Row not found when trying to delete" was thrown sometimes.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(67,
'New version available: 1.1.117 (2009-08-09)', '2009-08-09 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>CSV reading and parsing SQL scripts is now faster.
</li><li>Support for Java 6 DatabaseMetaData.getTables, getColumns.
</li><li>JaQu: the order of the fields no longer needs to match.
</li><li>Improved MySQL compatibility for SHOW COLUMNS.
</li><li>Improved PostgreSQL compatibility for timestamp literals.
</li><li>Sam Van Oort is now a committer.
</li><li>LIKE: the escape mechanism can now be disable using ESCAPE ''.
</li><li>Sergi Vladykin translated the error messages to Russian. Thanks a lot!
</li><li>The function LENGTH now return BIGINT.
</li><li>CLOB and BLOB: the maximum precision is now Long.MAX_VALUE.
</li><li>MVCC: the complete undo log must fit in memory.
</li></ul>
<b>Bugfixes:</b>
<ul><li>SimpleResultSet.newInstance(SimpleRowSource rs) did not work.
</li><li>Views using functions were not re-evaluated when necessary.
</li><li>Rollback of a large transaction could fail.
</li><li>Various bugfixes and improvements in the page store mechanism.
</li><li>Multi-threaded kernel synchronization bugs fixed.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(66,
'New version available: 1.1.116 (2009-07-18)', '2009-07-18 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>Result sets are now read-only by default.
</li><li>New system property h2.defaultResultSetConcurrency.
</li><li>Using an invalid result set type or concurrency now throws an exception.
</li><li>H2 Console: column of non-default schemas are now also listed.
</li><li>H2 Console: Oracle system tables are no longer listed.
</li><li>PG Server: improved compatibility and new system property h2.pgClientEncoding.
    Thanks a lot to Sergi Vladykin for the patch!
</li><li>To enable the page store mechanism, append ;PAGE_STORE=TRUE to the URL.
    This mechanism is now relatively stable.
</li><li>The built-in help is smaller.
</li></ul>
<b>Bugfixes:</b>
<ul><li>Server-less multi-connection mode: more bugs are fixed.
</li><li>If a pooled connection was not closed, an exception could occur.
</li><li>Removing an auto-increment or identity column didn't remove the sequence.
</li><li>Fulltext search: an exception was thrown when updating a value sometimes.
</li><li>The Recover tool did not always work.
</li><li>The soft-references cache (CACHE_TYPE=SOFT_LRU) could throw an exception.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

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
</li><li>Fulltext search: data is no longer deleted and
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
</li><li>EclipseLink: added H2Platform.supportsIdentity().
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
</li><li>H2 Console: the autocomplete feature has been improved a bit.
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
</li><li>CREATE DOMAIN: built-in data types can now only be changed if no tables exist.
</li><li>DatabaseMetaData.getPrimaryKeys: the column PK_NAME now contains the
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
</li><li>H2 Console: columns are now listed for up to 500 tables.
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
