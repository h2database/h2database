/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

CREATE TABLE VERSION(ID INT PRIMARY KEY, VERSION VARCHAR, CREATED VARCHAR);
INSERT INTO VERSION VALUES
(125, '1.3.175', '2014-01-18'),
(124, '1.3.174', '2013-10-19'),
(123, '1.3.173', '2013-07-28'),
(122, '1.3.172', '2013-05-25'),
(121, '1.3.171', '2013-03-17'),
(120, '1.3.170', '2012-11-30'),
(119, '1.3.169', '2012-09-09'),
(118, '1.3.168', '2012-07-13'),
(117, '1.3.167', '2012-05-23'),
(116, '1.3.166', '2012-04-08'),
(115, '1.3.165', '2012-03-18'),
;

CREATE TABLE CHANNEL(TITLE VARCHAR, LINK VARCHAR, DESC VARCHAR,
    LANGUAGE VARCHAR, PUB TIMESTAMP, LAST TIMESTAMP, AUTHOR VARCHAR);

INSERT INTO CHANNEL VALUES('H2 Database Engine' ,
    'http://www.h2database.com/', 'H2 Database Engine', 'en-us', NOW(), NOW(), 'Thomas Mueller');

CREATE VIEW ITEM AS
SELECT ID, 'New version available: ' || VERSION || ' (' || CREATED || ')' TITLE,
CAST((CREATED || ' 12:00:00') AS TIMESTAMP) ISSUED,
$$A new version of H2 is available for
<a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
For details, see the
<a href="http://www.h2database.com/html/changelog.html">change log</a>.
<br />
For future plans, see the
<a href="http://www.h2database.com/html/roadmap.html">roadmap</a>.
$$ AS DESC FROM VERSION;

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
UNION
SELECT 'doap-h2.rdf' FILE,
    XMLSTARTDOC() ||
$$<rdf:RDF xmlns="http://usefulinc.com/ns/doap#" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xml:lang="en">
<Project rdf:about="http://h2database.com">
    <name>H2 Database Engine</name>
    <homepage rdf:resource="http://h2database.com"/>
    <programming-language>Java</programming-language>
    <category rdf:resource="http://projects.apache.org/category/database"/>
    <category rdf:resource="http://projects.apache.org/category/library"/>
    <category rdf:resource="http://projects.apache.org/category/network-server"/>
    <license rdf:resource="http://usefulinc.com/doap/licenses/mpl"/>
    <bug-database rdf:resource="http://code.google.com/p/h2database/issues/list"/>
    <download-page rdf:resource="http://h2database.com/html/download.html"/>
    <shortdesc xml:lang="en">H2 Database Engine</shortdesc>
    <description xml:lang="en">
    H2 is a relational database management system written in Java.
    It can be embedded in Java applications or run in the client-server mode.
    The disk footprint is about 1 MB. The main programming APIs are SQL and JDBC,
    however the database also supports using the PostgreSQL ODBC driver by acting like a PostgreSQL server.
    It is possible to create both in-memory tables, as well as disk-based tables.
    Tables can be persistent or temporary. Index types are hash table and tree for in-memory tables,
    and b-tree for disk-based tables.
    All data manipulation operations are transactional. (from Wikipedia)
    </description>
    <repository>
        <SVNRepository>
            <browse rdf:resource="http://code.google.com/p/h2database/source/browse"/>
            <location rdf:resource="http://h2database.googlecode.com/svn/trunk"/>
        </SVNRepository>
    </repository>
    <mailing-list rdf:resource="http://groups.google.com/group/h2-database"/>
$$ ||
    GROUP_CONCAT(
        XMLNODE('release', NULL,
            XMLNODE('Version', NULL,
                XMLNODE('name', NULL, 'H2 ' || V.VERSION) ||
                XMLNODE('created', NULL, V.CREATED) ||
                XMLNODE('revision', NULL, V.VERSION)
            )
        )
        ORDER BY V.ID DESC SEPARATOR '') ||
'    </Project>
</rdf:RDF>' CONTENT
FROM VERSION V
