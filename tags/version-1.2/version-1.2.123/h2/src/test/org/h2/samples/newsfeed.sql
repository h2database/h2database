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

INSERT INTO ITEM VALUES(73,
'New version available: 1.2.123 (2009-11-08)', '2009-11-08 12:00:00',
$$A new version of H2 is available for
<a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
For details, see the
<a href="http://www.h2database.com/html/changelog.html">change log</a>.
<br />
For future plans, see the
<a href="http://www.h2database.com/html/roadmap.html">roadmap</a>.
$$);

INSERT INTO ITEM VALUES(72,
'New version available: 1.2.122 (2009-10-28)', '2009-10-28 12:00:00',
$$A new version of H2 is available for
<a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
For details, see the
<a href="http://www.h2database.com/html/changelog.html">change log</a>.
<br />
For future plans, see the
<a href="http://www.h2database.com/html/roadmap.html">roadmap</a>.
$$);

INSERT INTO ITEM VALUES(71,
'New version available: 1.2.121 (2009-10-11)', '2009-10-11 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>This is a beta version.
</li><li>If a database in the old format exists, it is now used.
</li><li>New system property h2.pageStoreTrim to disable shrinking the database.
</li><li>Better support GaeVFS (Google App Engine Virtual File System)
    thanks to Thanks to Vince Bonfanti.
</li></ul>
<b>Bugfixes:</b>
<ul><li>Page store bugs were fixed.
</li><li>The page store did not work when using Retrotranslator.
</li><li>CSVREAD didn't close the file. Thanks to Vince Bonfanti for the patch!
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(70,
'New version available: 1.2.120 (2009-10-04)', '2009-10-04 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
<ul><li>This is a beta version.
</li><li>New databases are now stored in a new file format.
    Existing databases are kept in the old file format.
</li><li>Databases created by this version can not be opened with older versions.
</li><li>In version 1.2, the following system properties are enabled by default:
    h2.pageStore, h2.nullConcatIsNull, h2.optimizeInList.
</li><li>PostgreSQL compatibility: function LASTVAL() as an alias for IDENTITY().
</li><li>Linked tables now support default values when inserting, updating or merging.
</li><li>Possibility to set a vendor id in Constants.java.
</li><li>Allow writing to linked tables in readonly databases.
</li></ul>
<b>Bugfixes:</b>
<ul><li>Issue 125: Renaming primary keys was not persistent. Fixed.
</li><li>Issue 124: Hibernate schema validation failed for decimal/numeric columns.
</li><li>Bugfixes in the page store.
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

INSERT INTO ITEM VALUES(69,
'New version available: 1.1.119 (2009-09-26)', '2009-09-26 12:00:00',
$$A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
<b>Changes and new functionality:</b>
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
</li><li>SQL statements in the exception are no longer always included.
</li></ul>
<b>Bugfixes:</b>
<ul><li>ChangeFileEncryption did not work with Lob subdirectories.
</li><li>SELECT COUNT(*) FROM SYSTEM_RANGE(...) returned the wrong result.
</li><li>More bugs in the server-less multi-connection mode have been fixed.
</li><li>Updating many rows with LOB could throw an exception.
</li><li>The native fulltext index could leak memory.
</li><li>Statement.getConnection() didn't work if the connection was closed.
</li><li>Issue 121: JaQu: new simple update and merge methods.
</li><li>Issue 120: JaQu didn't close result sets.
</li><li>Issue 119: JaQu creates wrong WHERE conditions on some inputs.
</li><li>Temporary local tables did not always work after reconnect if AUTO_SERVER=TRUE
</li></ul>
For details, see the 'Change Log' at
http://www.h2database.com/html/changelog.html
<br />
For future plans, see the 'Roadmap' page at
http://www.h2database.com/html/roadmap.html
$$);

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
