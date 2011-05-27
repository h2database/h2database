/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

CREATE TABLE CHANNEL(TITLE VARCHAR, LINK VARCHAR, DESC VARCHAR,
    LANGUAGE VARCHAR, PUB TIMESTAMP, LAST TIMESTAMP, AUTHOR VARCHAR);

INSERT INTO CHANNEL VALUES('H2 Database Engine' ,
    'http://www.h2database.com', 'H2 Database Engine', 'en-us', NOW(), NOW(), 'Thomas Mueller');

CREATE TABLE ITEM(ID INT PRIMARY KEY, TITLE VARCHAR, ISSUED TIMESTAMP, DESC VARCHAR);

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
http://groups.google.com/group/h2-database/web/change-log
<br />
For future plans, see the ''Roadmap'' page at
http://groups.google.com/group/h2-database/web/roadmap
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
http://groups.google.com/group/h2-database/web/change-log
<br />
For future plans, see the ''Roadmap'' page at
http://groups.google.com/group/h2-database/web/roadmap
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
http://groups.google.com/group/h2-database/web/roadmap
');

INSERT INTO ITEM VALUES(33,
'New version available: 1.0.63 (2007-12-02)', '2007-12-02 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>Performance optimization for IN(...) and IN(SELECT...), 
    currently disabled by default. To enable, use java -Dh2.optimizeInJoin=true
</li><li>The H2 Console has been translated to Ukrainian by Igor Dobrovolskyi. Thanks a lot! 
</li><li>The SecurePassword example has been improved.
</li><li>Improved FTP server: now the PORT command is supported.
</li><li>New function TABLE_DISTINCT. 
</li></ul>
<b>Bugfixes:</b>
<ul><li>Certain setting in the Server didn''t work.
</li><li>In timezones where the summer time saving limit is at midnight, 
  some dates did not work in some virtual machines, 
    for example 2007-10-14 in Chile, using the Sun JVM 1.6.0_03-b05.
</li><li>The native fulltext search was not working properly after re-connecting. 
</li><li>Temporary views (FROM(...)) with UNION didn''t work if nested. 
</li><li>Using LIMIT with values close to Integer.MAX_VALUE didn''t work. 
</li></ul>
For future plans, see the ''Roadmap'' page at
http://groups.google.com/group/h2-database/web/roadmap
');

INSERT INTO ITEM VALUES(32,
'New version available: 1.0.62 (2007-11-25)', '2007-11-25 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>Large updates and deletes are now supported.
</li><li>Google Android is supported.
</li><li>Large CSV operations are now faster.
</li><li>A patch for Apache DDL Utils is available.
</li><li>Eduardo Velasques has translated H2 to Brazilian Portuguese.
</li><li>Now using custom toString() for JDBC objects.
</li><li>The setting h2.emergencySpaceInitial is now 256 KB.
</li></ul>
<b>Bugfixes:</b>
<ul><li>Creating a table from GROUP_CONCAT didn''t always work.
</li><li>CSV: Using an empty field delimiter didn''t work.
</li><li>Nested temporary views with parameters didn''t always work.
</li><li>Cluster mode: could not connect if only one server was running.
</li><li>ARRAY values are now sorted as in PostgreSQL.
</li><li>The console did not display multiple spaces correctly.
</li><li>Duplicate column names were not detected when renaming columns.
</li><li>The H2 Console now also supports -ifExists.
</li><li>Changing a user with a schema made the schema inaccessible.
</li><li>Referential integrity checks didn''t lock the referenced table.
</li><li>Now changing MVCC too late throws an Exception.
</li></ul>
For future plans, see the ''Roadmap'' page at
http://groups.google.com/group/h2-database/web/roadmap
');

INSERT INTO ITEM VALUES(31,
'New version available: 1.0.61 (2007-11-10)', '2007-11-10 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>Read-only databases in zip (or jar) files are now supported: jdbc:h2:zip:c:/temp/db.zip!/test
</li><li>File access is now done using an extensible API. Additional file systems are easy to implement.
</li><li>Descending indexes are supported.
</li><li>The Lucene fulltext search is included in the h2.jar.
</li><li>MODE is now a database level setting (not global).
</li><li>Vlad Alexahin has translated H2 Console to Russian. Thanks a lot!
</li><li>INSTR, LOCATE: backward searching is now supported by using a negative start position.
</li><li>CREATE SEQUENCE: New option CACHE (number of pre-allocated numbers).
</li><li>Converting decimal to integer now rounds like MySQL and PostgreSQL.
</li><li>Math operations using only parameters are now interpreted as decimal.
</li><li>MVCC: The system property h2.mvcc has been removed.
</li></ul>
<b>Bugfixes:</b>
<ul><li>ResultSetMetaData.getColumnDisplaySize is now calculated correctly.
</li><li>A few MVCC bugs have been fixed.
</li><li>The code coverage is now at 83%.
</li></ul>
For future plans, see the ''Roadmap'' page at
http://groups.google.com/group/h2-database/web/roadmap
');

INSERT INTO ITEM VALUES(30,
'New version available: 1.0.60 (2007-10-20)', '2007-10-20 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>User defined aggregate functions are now supported.
</li><li>New Italian translation from PierPaolo Ucchino. Thanks a lot!
</li><li>CSV: New methods to set the escape character and field delimiter in the Csv tool and the CSVWRITE and CSVREAD methods.
</li><li>CSVREAD, RUNSCRIPT and so on now support URLs as well, using
    URL.openStream(). Example: select * from csvread(''jar:file:///c:/temp/test.jar!/test.csv'');
</li></ul>
<b>Bugfixes:</b>
<ul><li>Prepared statements could not be used after data definition statements (creating tables and so on). Fixed.
</li><li>PreparedStatement.setMaxRows could not be changed to a higher value after the statement was executed.
</li><li>Linked tables: now tables in non-default schemas are supported as well 
</li><li>JdbcXAConnection: starting a transaction before getting the connection didn''t switch off autocommit.
</li><li>Server.shutdownTcpServer was blocked when first called with force=false and then force=true.
    Now documentation is improved, and it is no longer blocked.
</li><li>Stack traces did not include the SQL statement in all cases where they could have. 
    Also, stack traces with SQL statement are now shorter.
</li><li>The H2 Console could not connect twice to the same H2 embedded database at the same time. Fixed.
</li></ul>
For future plans, see the ''Roadmap'' page at
http://groups.google.com/group/h2-database/web/roadmap
');

INSERT INTO ITEM VALUES(29,
'New version available: 1.0.59 (2007-10-03)', '2007-10-03 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>Fulltext search is now documented (see Tutorial).
</li><li>H2 Console: Progress information when logging into a H2 embedded database.
</li><li>SCRIPT: the SQL statements in the result set now include the terminating semicolon.
</li></ul>
<b>Bugfixes:</b>
<ul><li>If the process was killed while the database was running, 
    sometimes the database could not be opened.
</li><li>Comparing columns with constants that are out of range works again.
</li><li>When the data type was unknown in a subquery, sometimes the wrong exception was thrown.
</li><li>Multi-threaded kernel (MULTI_THREADED=1): A synchronization problem has been fixed.
</li><li>A PreparedStatement that was cancelled could not be reused. 
</li><li>When the database was closed while logging was disabled (LOG 0), 
    re-opening the database was slow.
</li><li>The Console did not always refresh the table list when required.
</li><li>When creating a table using CREATE TABLE .. AS SELECT, 
    the precision for some data types was wrong in some cases.
</li><li>When using the (undocumented) in-memory file system 
    (jdbc:h2:memFS:x or jdbc:h2:memLZF:x), and using multiple connections, 
    a ConcurrentModificationException could occur. 
</li><li>REGEXP compatibility: now Matcher.find is used.
</li><li>When using a subquery with group by as a table, some columns could not be used.
</li><li>Views with subqueries as tables and queries with nested subqueries as tables did not always work.
</li></ul>
For future plans, see the new ''Roadmap'' page on the web site.
');

INSERT INTO ITEM VALUES(28,
'New version available: 1.0.58 (2007-09-15)', '2007-09-15 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
(You may have to click ''Refresh'').
<br />
<b>Changes and new functionality:</b>
<ul><li>Empty space in the database files is now better reused
</li><li>The database file sizes now increased in smaller blocks
</li><li>Optimization for independent subqueries
</li><li>Improved explain plan 
</li><li>Maven 2: new version are now automatically synced
</li><li>The version (build) number is now included in the manifest file.
</li><li>The default value for MAX_MEMORY_UNDO is now 100000
</li><li>Improved MultiDimension tool (for spatial queries)
</li><li>New method DatabaseEventListener.opened
</li><li>Optimization for COLUMN IN(.., NULL)
</li><li>Oracle compatibility for SYSDATE and CHR
</li><li>System.exit is no longer called by the WebServer
</li></ul>
<b>Bugfixes:</b>
<ul><li>About 230 bytes per database was leaked
</li><li>Using spaces in column and table aliases did not always work
</li><li>In some systems, SecureRandom.generateSeed is very slow
</li><li>Console: better support for Internet Explorer
</li><li>A database can now be opened even if user class is missing
</li><li>User defined functions may not overload built-in functions
</li><li>Adding a foreign key failed when the reference contained NULL
</li><li>For PgServer, character encoding other than UTF-8 did not work
</li><li>When using IFNULL, NULLIF, COALESCE, LEAST, or GREATEST, 
    and the first parameter was ?, an exception was thrown
</li><li>When comparing TINYINT or SMALLINT columns, the index was not used
</li><li>The documentation indexer does no longer index Japanese pages
</li><li>Using a function in a GROUP BY expression did not always work
</li></ul>
For future plans, see the new ''Roadmap'' page on the web site.
');

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
