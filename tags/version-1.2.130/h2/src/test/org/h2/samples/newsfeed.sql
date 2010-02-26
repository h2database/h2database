/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

CREATE TABLE CHANNEL(TITLE VARCHAR, LINK VARCHAR, DESC VARCHAR,
    LANGUAGE VARCHAR, PUB TIMESTAMP, LAST TIMESTAMP, AUTHOR VARCHAR);

INSERT INTO CHANNEL VALUES('H2 Database Engine' ,
    'http://www.h2database.com/', 'H2 Database Engine', 'en-us', NOW(), NOW(), 'Thomas Mueller');

CREATE TABLE ITEM(ID INT PRIMARY KEY, TITLE VARCHAR, ISSUED TIMESTAMP, DESC VARCHAR);

INSERT INTO ITEM VALUES(80,
'New version available: 1.2.130 (2010-02-26)', '2010-02-26 12:00:00',
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

INSERT INTO ITEM VALUES(79,
'New version available: 1.2.129 (2010-02-19)', '2010-02-19 12:00:00',
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

INSERT INTO ITEM VALUES(78,
'New version available: 1.2.128 (2010-01-30)', '2010-01-30 12:00:00',
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

INSERT INTO ITEM VALUES(77,
'New version available: 1.2.127 (2010-01-15)', '2010-01-15 12:00:00',
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

INSERT INTO ITEM VALUES(76,
'New version available: 1.2.126 (2009-12-18)', '2009-12-18 12:00:00',
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

INSERT INTO ITEM VALUES(75,
'New version available: 1.2.125 (2009-12-06)', '2009-12-06 12:00:00',
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

INSERT INTO ITEM VALUES(74,
'New version available: 1.2.124 (2009-11-20)', '2009-11-20 12:00:00',
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
    XMLNODE('feed', XMLATTR('xmlns', 'http://www.w3.org/2005/Atom') || XMLATTR('xml:lang', C.LANGUAGE),
        XMLNODE('title', XMLATTR('type', 'text'), C.TITLE) ||
        XMLNODE('id', NULL, XMLTEXT(C.LINK)) ||
        XMLNODE('author', NULL, XMLNODE('name', NULL, C.AUTHOR)) ||
        XMLNODE('link', XMLATTR('rel', 'self') || XMLATTR('href', 'http://www.h2database.com/html/newsfeed-atom.xml'), NULL) ||
        XMLNODE('updated', NULL, FORMATDATETIME(C.LAST, 'yyyy-MM-dd''T''HH:mm:ss''Z''', 'en', 'GMT')) ||
        GROUP_CONCAT(
            XMLNODE('entry', NULL,
                XMLNODE('title', XMLATTR('type', 'text'), I.TITLE) ||
                XMLNODE('link', XMLATTR('rel', 'alternate') || XMLATTR('type', 'text/html') || XMLATTR('href', C.LINK), NULL) ||
                XMLNODE('id', NULL, XMLTEXT(C.LINK || '/' || I.ID)) ||
                XMLNODE('updated', NULL, FORMATDATETIME(I.ISSUED, 'yyyy-MM-dd''T''HH:mm:ss''Z''', 'en', 'GMT')) ||
                XMLNODE('content', XMLATTR('type', 'html'), XMLCDATA(I.DESC))
            )
        ORDER BY I.ID DESC SEPARATOR '')
    ) CONTENT
FROM CHANNEL C, ITEM I
UNION
SELECT 'newsletter.txt' FILE, I.DESC CONTENT FROM ITEM I WHERE I.ID = (SELECT MAX(ID) FROM ITEM)
