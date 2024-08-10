/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

CREATE TABLE VERSION(ID INT PRIMARY KEY, VERSION VARCHAR, CREATED VARCHAR);
INSERT INTO VERSION VALUES

(161, '2.3.232', '2024-08-11'),
(160, '2.3.230', '2024-07-15'),
(159, '2.2.224', '2023-09-17'),
(158, '2.2.222', '2023-08-22'),
(157, '2.2.220', '2023-07-04'),
(156, '2.1.214', '2022-06-13'),
(155, '2.1.212', '2022-04-09'),
(154, '2.1.210', '2022-01-17'),
(153, '2.0.206', '2022-01-04'),
(152, '2.0.204', '2021-12-21'),
(151, '2.0.202', '2021-11-25'),
(150, '1.4.200', '2019-10-14'),
(149, '1.4.199', '2019-03-13'),
(148, '1.4.198', '2019-02-22'),
(147, '1.4.197', '2018-03-18'),
(146, '1.4.196', '2017-06-10');

CREATE TABLE CHANNEL(TITLE VARCHAR, LINK VARCHAR, DESC VARCHAR,
    LANGUAGE VARCHAR, PUB TIMESTAMP, LAST TIMESTAMP, AUTHOR VARCHAR);

INSERT INTO CHANNEL VALUES('H2 Database Engine' ,
    'https://h2database.com/', 'H2 Database Engine', 'en-us', LOCALTIMESTAMP, LOCALTIMESTAMP, 'Thomas Mueller');

CREATE VIEW ITEM AS
SELECT ID, 'New version available: ' || VERSION || ' (' || CREATED || ')' TITLE,
CAST((CREATED || ' 12:00:00') AS TIMESTAMP) ISSUED,
$$A new version of H2 is available for
<a href="https://h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
For details, see the
<a href="https://h2database.com/html/changelog.html">change log</a>.
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
        XMLNODE('link', XMLATTR('rel', 'self') || XMLATTR('href', 'https://h2database.com/html/newsfeed-atom.xml'), NULL) ||
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
<Project rdf:about="https://h2database.com">
    <name>H2 Database Engine</name>
    <homepage rdf:resource="https://h2database.com"/>
    <programming-language>Java</programming-language>
    <category rdf:resource="http://projects.apache.org/category/database"/>
    <category rdf:resource="http://projects.apache.org/category/library"/>
    <category rdf:resource="http://projects.apache.org/category/network-server"/>
    <license rdf:resource="http://usefulinc.com/doap/licenses/mpl"/>
    <bug-database rdf:resource="https://github.com/h2database/h2database/issues"/>
    <download-page rdf:resource="https://h2database.com/html/download.html"/>
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
            <browse rdf:resource="https://github.com/h2database/h2database"/>
            <location rdf:resource="https://github.com/h2database/h2database"/>
        </SVNRepository>
    </repository>
    <mailing-list rdf:resource="https://groups.google.com/g/h2-database"/>
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
