/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

CREATE TABLE CHANNEL(TITLE VARCHAR, LINK VARCHAR, DESC VARCHAR,
    LANGUAGE VARCHAR, PUB TIMESTAMP, LAST TIMESTAMP, AUTHOR VARCHAR);

INSERT INTO CHANNEL VALUES('H2 Database Engine' ,
    'http://www.h2database.com', 'H2 Database Engine', 'en-us', NOW(), NOW(), 'Thomas Mueller');

CREATE TABLE ITEM(ID INT PRIMARY KEY, TITLE VARCHAR, ISSUED TIMESTAMP, DESC VARCHAR);

INSERT INTO ITEM VALUES(19,
'New version available: 1.0 / 2007-01-02', '2007-01-02 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Changes and new functionality:</b>
<ul>
<li>H2 is now available in Maven. The groupId is com.h2database, 	the 
	artifactId is h2 and the version 1.0.20061217 (the new version will be 
	available in a few days). To create the maven artifacts yourself, use 
	''ant mavenUploadLocal'' and ''ant mavenBuildCentral''.
<li>Many settings are now initialized from system properties and can be 
	changed on the command line without having recompile the database. 
	See Advances / Settings Read from System Properties.
<li>The (relative or absolute) directory where the script files are stored 
	or read can now be changed using the system property h2.scriptDirectory
<li>Client trace files now created in the directory ''trace.db'' and no 
	longer the application directory. This can be changed using the system 
	property h2.clientTraceDirectory.
<li>Build: Now using ant-build.properties. The JDK is automatically updated 
	when using ant codeswitch_...
<li>Cluster: Now the server can detect if a query is read-only, and in this 
	case the result is only read from the first cluster node. However, there 
	is currently no load balancing made to avoid problems with transactions 
	/ locking.
</ul>
<b>Bugfixes:</b>
<ul>
<li>If a CLOB or BLOB was deleted in a transaction and the database crashed 
	before the transaction was committed or rolled back, the object was lost if 
	it was large. Fixed.
<li>Prepared statements with non-constant functions such as 
	CURRENT_TIMESTAMP() did not get re-evaluated if the result of the 
	function changed. Fixed.
<li>In some situations the log file got corrupt if the process was terminated 
	while the database was opening.
<li>Using ;RECOVER=1 in the database URL threw a syntax exception. Fixed.
<li>It was possible to drop the sequence of a temporary tables with DROP 
	ALL OBJECTS, resulting in a null pointer exception afterwards.
</ul>
For future plans, see the new ''Roadmap'' page on the web site.
</ul>
');

INSERT INTO ITEM VALUES(18,
'New version available: 1.0 / 2006-12-17', '2006-12-17 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Changes and new functionality:</b>
<ul>
<li>Large BLOB and CLOB support for the server and the cluster mode. 
    Larger objects will temporarily be buffered on the client side.
<li>Can be compiled with JDK 1.6.
     However, only very few of the JDBC 4.0 features are implemented so far.
<li>Table aliases are now supported in DELETE and UPDATE. 
    Example: DELETE FROM TEST T0.
<li>The RunScript tool can now include other files using a new syntax: 
    @INCLUDE fileName. This is only required for server and cluster modes.
    It was already possible to use embedded RUNSCRIPT statements, 
    but those are always executed locally.
<li>When the database URL contains ;RECOVERY=TRUE then 
    the index file is now deleted if it was not closed before.
<li>Deleting old temp files now uses a phantom reference queue. 
    Generally, temp files should now be deleted earlier.
<li>Opening a large database is now much faster 
    even when using the default log mode (LOG=1), 
    if the database was previously closed.
<li>Support for indexed parameters in PreparedStatements: 
    UPDATE TEST SET NAME = ?2 WHERE ID = ?1
</ul>
<b>Bugfixes:</b>
<ul>
<li>Unfortunately, the Hibernate dialect has changed due to a change 
    in the meta data in the last release (INFORMATION_SCHEMA.SEQUENCES).
<li>String.toUpperCase and toLowerCase can not be used to parse commands.
    Now using toUpperCase(Locale.ENGLISH) or Character.toUpperCase(..).
<li>The scale of a NUMERIC(1) column is now 0. It used to be 32767.
<li>PreparedStatement.setObject(x, y, Types.OTHER) does now 
    serialize the object in every case (even for Integer).
<li>EXISTS subqueries with parameters were not re-evaluated 
    when the prepared statement was reused. This could lead to incorrect results.
</ul>
For future plans, see the new ''Roadmap'' page on the web site.
</ul>
');

INSERT INTO ITEM VALUES(17,
'New version available: 1.0 / 2006-12-03', '2006-12-03 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Changes and new functionality:</b>
<ul>
<li>New connection time setting CACHE_TYPE=TQ to use the 2Q page replacement 
    algorithm. 2Q should be more resistant to table scan.
<li>Improved performance for CREATE INDEX (about 10%).
<li>Optimization: The left and right side of a AND and OR conditions are now 
    ordered by the expected cost.
<li>Java functions returning a result set (such as CSVREAD) now can be used in 
    a SELECT statement like a table. The behaviour has been changed: Now first 
    call contains the values if set, but the connection URL is different 
    (jdbc:columnlist:connection instead of jdbc:default:connection).
<li>The service wrapper is now included in the default installation and documented.
<li>New system function SESSION_ID().
<li>Mixing certain data types in an operation, for example VARCHAR and 
    TIMESTAMP, now converts both expressions to TIMESTAMP.
<li>Change behaviour: If both sides of a comparison are parameters with unknown 
    data type, then an exception is thrown now. The same happens for UNION 
    SELECT if both columns are parameters.
</ul>
<b>Bugfixes:</b>
<ul>
<li>There was a bug in the database encryption algorithm. 
    Pattern of repeated encrypted bytes where generated for empty blocks in the 
    file. If you have an encrypted database, you will need to decrypt the database 
    using the org.h2.tools.ChangePassword (using the old database engine), and 
    encrypt the database using the new engine. Alternatively, you can use the 
    Backup and RunScript tools or the SQL commands SCRIPT and RUNSCRIPT.
<li>Deeply nested views where slow to execute, because the query was parsed 
    many times. Fixed.
<li>The SQL statement COMMENT did not work as expected. If you already have 
    comments in the database, it is recommended to backup and restore the 
    database, using the Backup and RunScript tools or the SQL commands SCRIPT 
    and RUNSCRIPT.
<li>Server: In some situations, calling close() on a ResultSet or Statement threw 
    an exception. This has been fixed.
</ul>
For future plans, see the new ''Roadmap'' page on the web site.
</ul>
');

INSERT INTO ITEM VALUES(16,
'New version available: 1.0 / 2006-11-20', '2006-11-20 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Changes and new functionality:</b>
<ul>
<li>New SQL statement SET SCHEMA to change the current schema of this session.
<li>New system function SCHEMA() to get the current schema.
<li>SCRIPT: New option BLOCKSIZE to split BLOBs and CLOBs into separate blocks, to avoid OutOfMemory problems.
<li>CURRVAL and NEXTVAL functions: New optional sequence name parameter.
<li>The default cache size is now 65536 pages instead of 32768.
<li>New optimization to reuse subquery results. Can be disabled with SET OPTIMIZE_REUSE_RESULTS 0.
<li>EXPLAIN... results are now formatted on multiple lines so they are easier to read.
<li>The Spanish translation was completed by Miguel Angel. Thanks a lot! Translations to other languages are always welcome.
<li>The Recovery tool has been improved. It now creates a SQL script file that can be executed directly.
<li>LENGTH now returns the precision for CLOB, BLOB, and BINARY (and is therefore faster).
<li>The built-in FTP server can now access a virtual directory stored in a database.
</ul>
<b>Bugfixes:</b>
<ul>
<li>When using the READ_COMMITTED isolation level, a transaction now waits until there are no write locks.
<li>INSERT INTO ... SELECT ... and ALTER TABLE with CLOBs and/or BLOBs did not work.
<li>CSV tool: the methods setFieldSeparatorWrite and setRowSeparatorWrite where not accessible.
<li>ALTER TABLE ADD did throw a strange message if the table contained views. Now the message is better, 
    but it is still not possible to do that if views on this table exist.
<li>ALTER TABLE: If there was a foreign key in another table that references to the change table, the constraint was dropped.
<li>Direct links to the Javadoc were not working.
<li>Inserting rows into linked tables did not work for HSQLDB when the value was NULL.
<li>Oracle SYNONYM tables are now listed in the H2 Console.
<li>CREATE LINKED TABLE didn''t work for Oracle SYNONYM tables.
<li>When using the server version, when not closing result sets or using nested DatabaseMetaData result sets, the connection could break.
</ul>
For future plans, see the new ''Roadmap'' page on the web site.
</ul>
');

INSERT INTO ITEM VALUES(15,
'New version available: 1.0 / 2006-11-03', '2006-11-03 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Changes and new functionality:</b>
<ul>
<li>New SQL statement COMMENT ON ... IS ...
<li>Two simple full text search implementations (Lucene and native) are now included. 
    This is work in progress, and currently undocumented. 
    See test/org/h2/samples/fullTextSearch.sql and History for more details.
<li>Index names of referential constraints are now prefixed with the constraint name.
<li>Improved search functionality in the HTML documentation (highlight).
<li>Triggers can now be defined on a list of actions.
<li>On some systems (for example, some Linux VFAT and USB flash disk drivers), 
    RandomAccessFile.setLength does not work correctly. 
    A workaround for this problem has been implemented.
<li>DatabaseMetaData.getTableTypes now also returns SYSTEM TABLE and TABLE VIEW.
    Please tell me if this breaks other applications or tools.
<li>Java functions with Blob or Clob parameters are now supported.
<li>Added a ''remarks'' column to most system tables. 
<li>New system table INFORMATION_SCHEMA.TRIGGERS
<li>PostgreSQL compatibility: Support for the date format 2006-09-22T13:18:17.061
<li>MySQL compatibility: ResultSet.getString("PEOPLE.NAME") is now supported.
<li>JDBC 4.0 driver auto discovery: When using JDK 1.6, 
    Class.forName("org.h2.Driver") is no longer required.
</ul>
<b>Bugfixes:</b>
<ul>
<li>Wide b-tree indexes (with large VARCHAR columns for example) could get corrupted.
<li>If a custom shutdown hook was installed, and the database was called at shutdown, 
    a NullPointException was thrown. 
    Now, a error message with instructions how to fix this is thrown.
<li>If SHUTDOWN was called and DB_CLOSE_DELAY was set, the database was not closed.
<li>Subqueries with order by outside the column list didn''t work correctly.
<li>Linked Tables: Only the first column was linked when linking to PostgreSQL.
<li>Sequences: When the database is not closed normally, the value was not set correctly.
<li>The optimization for IN(SELECT...) was too agressive.
<li>Blob.getBytes skipped the wrong number of bytes.
<li>Group by a function didn''t work if a column alias was specified in the select list.
<li>LOCK_MODE 0 (READ_UNCOMMITTED) did not work when using multiple connections.
</ul>
For future plans, see the new ''Roadmap'' page on the web site.
</ul>
');

INSERT INTO ITEM VALUES(14,
'New version available: 1.0 / 2006-10-10', '2006-10-10 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Changes and new functionality:</b>
<ul>
<li>Support for DOMAIN (user defined data types).
<li>Support for UUID.
<li>Function aliases may now optionally include parameter classes.
<li>A small FTP server is now included (disabled by default).
<li>Can now compile everything with JDK 1.6 (however, only very few of the JDBC 4.0 features are implemented currently).
<li>The multi-threaded kernel can not be enabled using SET MULTI_THREADED 1.
    A new tests has been written for this feature, and additional synchronization has been added.
</ul>
<b>Bugfixes:</b>
<ul>
<li>Could not re-connect to a database when ALLOW_LITERALS or COMPRESS_LOB was set.
<li>Opening and closing connections in many threads sometimes failed.
<li>Reconnect didn''t work after renaming a user if rights were granted for this user.
<li>GROUP BY an formula or function didn''t work if the same expression was used in the select list.
<li>Redundant () in a IN subquery is now supported: where id in ((select id from test)).
</ul>
For future plans, see the new ''Roadmap'' page on the web site.
</ul>
');

INSERT INTO ITEM VALUES(13,
'New version available: 1.0 / 2006-09-24', '2006-09-24 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Changes and new functionality:</b>
<ul>
<li>Protection against SQL injection: New command SET ALLOW_LITERALS {NONE|ALL|NUMBERS}.
    With SET ALLOW_LITERALS NONE, SQL injections are not possible because literals in SQL statements are rejected;
    User input must be set using parameters (''?'') in this case.
<li>New concept ''Constants'': New SQL statements CREATE CONSTANT and DROP CONSTANT.
<li>CREATE TABLE ... AS SELECT ... is now supported.
<li>New data type OTHER (alternative names OBJECT and JAVA_OBJECT). When using this data type,
    Java Objects are automatically serialized and deserialized in the JDBC layer.
<li>Improved performance for MetaData calls
<li>Improved BatchUpdateException
<li>DatabaseMetaData.getProcedures and getProcedureColumns are implemented now
<li>An exception is now thrown on unknown setting in the database URL (which are most likely typos)
<li>The log size is now automatically increased to at least 10% of the data file.
<li>Backup and Runscript tools now support options (for compression and so on)
<li>InetAddress.getByName("127.0.0.1")  instead of InetAddress.getLocalHost() is now used to get the loopback address
<li>CREATE SCHEMA: The authorization part is now optional.
<li>DROP TABLE: Can now drop more than one column in one step: DROP TABLE A, B
<li>LOBs are now automatically converted to files by default.
<li> Optimizations for WHERE ... IN(...) and SELECT MIN(..), MAX(..) are now enabled by default.
<li>New system function LOCK_MODE()
<li>Connection.setTransactionIsolation and getTransactionIsolation now set / get the LOCK_MODE of the database.
<li>New LOCK_MODE 3 (READ_COMMITTED). Table level locking, but only when writing (no read locks).
</ul>
<b>Bugfixes:</b>
<ul>
<li>Wide b-tree indexes (with large VARCHAR columns for example) with a long common prefix (where many
    rows start with the same text) could get corrupted. Fixed.
<li>Reading from compressed LOBs didn''t work in some cases. Fixed.
<li>CLOB / BLOB: Copying LOB data directly from one table to another, and altering a table with with LOBs did not work,
    the BLOB data was deleted when the old table was deleted. Fixed.
<li>[NOT] EXISTS(SELECT ... EXCEPT SELECT ...) did not work in all cases. Fixed.
<li>Functions returning a result set are now called as documented.
</ul>
For future plans, see the new ''Roadmap'' page on the web site.
</ul>
');

INSERT INTO ITEM VALUES(12,
'New version available: 1.0 / 2006-09-10', '2006-09-10 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Changes and new functionality:</b>
<ul>
<li>SET IGNORECASE is now supported for compatibility with HSQLDB.
<li>New SQL statement DROP ALL OBJECTS [DELETE FILES] to drop all tables, sequences and so on.
<li>Improved OpenOffice compatibility.
<li>New setting SET COMPRESS_LOB {NO|LZF|DEFLATE} to automatically compress BLOBs and CLOBs.
<li>The script can now be compressed. Syntax: SCRIPT TO ''file'' COMPRESSION {DEFLATE|LZF|ZIP|GZIP}.
<li>Now an exception is thrown when the an overflow occurs for mathematical operations (sum, multiply and so on) for the data type selected.
    This was implemented in the previous version but is now enabled by default.
<li>Updated the performance test so that Firebird can be tested as well. Results are not included currently,
    information how to best test Firebird should be sent to the support address or posted in the Forum.
</ul>
<b>Bugfixes:</b>
<ul>
<li>ORDER BY an expression didn''t work when using GROUP BY at the same time.
<li>A problem with referential constraints in the SCRIPT file has been fixed.
<li>Console: The setting ;hsqldb.default_table_type=cached was added to the H2 database instead of the HSQLDB database.
<li>Docs: The cross references in the SQL grammar docs where broken in the last release.
<li>Deleting many rows from a table with a self-referencing constraint with ''on delete cascade'' did not work.
<li>ROWNUM didn''t always work as expected when using subqueries.
<li>Correlated subqueries: It is now possible to use columns of the outer query in the select list of the inner query.
</ul>
For future plans, see the new ''Roadmap'' page on the web site.
</ul>
');

INSERT INTO ITEM VALUES(11,
'Article about the 1.0 release on InfoQ', '2006-09-05 12:00:00',
'There is an article about the 1.0 release at InfoQ:
See <a href="http://www.infoq.com/news/h2-released">http://www.infoq.com/news/h2-released</a>.
');

INSERT INTO ITEM VALUES(10,
'New version available: 1.0 / 2006-08-31', '2006-08-31 12:00:00',
'H2 version 1.0 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Bugfixes:</b>
<ul>
<li>In some situations, wide b-tree indexes (with large VARCHAR columns for example) could get corrupted. Fixed.
<li>ORDER BY was broken in the last release when using table aliases. Fixed.
</ul>
For details see also the history.
For future plans, see the new ''Roadmap'' page on the web site.
');

INSERT INTO ITEM VALUES(9,
'New version available: 1.0 RC 2 / 2006-08-28', '2006-08-28 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Changes and new functionality:</b>
<ul>
<li>Linked tables: The table name is no longer quoted when accessing the foreign database.
    This allows to use schema names, and possibly subqueries as table names (when used in queries).
    Also, the compatibility with other databases has been improved.
<li>New setting MAX_LENGTH_INPLACE_LOB.
<li>LOB files where not deleted when the table was truncated or dropped. This is now done.
</ul>
<b>New Functionality (but currently disabled):</b>
These features are currently disabled. They will be enabled after release 1.0.
See the history for more details.
<li>Optimization for MIN and MAX: Queries such as SELECT MIN(ID), MAX(ID)+1, COUNT(*) FROM TEST can now use an index if one is available.
<li>When large strings or byte arrays where inserted into a LOB (CLOB or BLOB), or if the data was stored using
    PreparedStatement.setBytes or setString, the data was stored in-place (no separate files where created).
    Now distinct files are created if the size is larger than MAX_LENGTH_INPLACE_LOB.
</ul>
<b>Bugfixes:</b>
<ul>
<li>Outer join: Some incompatibilities with PostgreSQL and MySQL with more complex outer joins have been fixed.
<li>Subquery optimization: Constant subqueries are now only evaluated once (like this was before).
<li>DATEDIFF on seconds, minutes, hours did return different results in certain timezones (half-hour timezones) in certain situations. Fixed.
</ul>
For details see also the history. Version 1.0 is planned for 2006-08-29.
The plans for version 1.0 are:
<ul>
<li>Write more tests, bugfixes.
<li>For plans after release 1.0, see the new ''Roadmap'' page on the web site.
</ul>
');

INSERT INTO ITEM VALUES(8,
'New proposed license', '2006-08-25 12:00:00',
'A new proposed license is available at
<a href="http://www.h2database.com/html/proposed_license.html">h2database.com/html/proposed_license.html</a>.
This is still open for discussion at the <a href="http://www.h2database.com/ipowerb">license forum</a>.
');

INSERT INTO ITEM VALUES(7,
'New version available: 0.9 RC 1 / 2006-08-23', '2006-08-23 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Changes and new functionality:</b>
<ul>
<li>Date and time constants outside the valid range (February 31 and so on) are no longer accepted.
<li>Improvements in the autocomplete feature. Thanks a lot to James Devenish for his very valuable feedback and testing!
</ul>
<b>New Functionality (but currently disabled):</b>
These features are currently disabled. They will be enabled after release 1.0.
See the history for more details.
<li>Optimization for IN(value list) and IN(subquery).
<li>Very large transactions are now supported.
<li>Arithmetic overflows in can now be detected for integer types.
</ul>
<b>Bugfixes:</b>
<ul>
<li>Bugfix for an outer join problem (too many rows where returned for a combined inner join / outer join).
<li>Local temporary tables where not included in the meta data.
<li>Database opening: Sometimes opening a database was very slow because indexes were re-created.
<li>Referential integrity: Fixed a stack overflow problem when using cascade delete.
<li>LIKE: If collation was set (SET COLLATION ...), it was ignored when using LIKE.
</ul>
For details see also the history. Release 1.0 is planned for 2006-08-28.
There will be another release candidate (RC 2) before.
The plans for the next release (RC 2) are:
<ul>
<li>Change the license to MPL.
<li>Write more tests, bugfixes.
<li>For plans after release 1.0, see the new ''Roadmap'' page on the web site.
</ul>
');

INSERT INTO ITEM VALUES(6,
'New version available: 0.9 Beta / 2006-08-14', '2006-08-14 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Changes and new functionality:</b>
<ul>
<li>Autocomplete support in the H2 Console.
    Three levels: Off, Normal (default; table and column names), Full (complete SQL grammar).
    Schemas and quoted identifiers are not yet supported.
    There are some browser issues, but everything should work in Firefox.
    Please report any incompatibilities, problems and usability issues.
<li>Source code to support H2 in Resin is included.
    For the complete patch, see http://forum.caucho.com/node/61
<li>Space is better re-used after deleting many records.
<li>Umlauts and chinese characters are now supported in identifier names.
<li>Performance optimization for outer join with comparison with constants in the where clause.
</ul>
<b>Bugfixes:</b>
<ul>
<li>SET LOG 0 didn''t work. Fixed.
<li>Fixed a problem when comparing BIGINT values with constants.
<li>Fixed NULL handling where there is a NULL value in the list.
<li>It is now possible to cancel a select statement with a (temporary) view.
</ul>
For details see also the history. The next release is planned for 2006-08-28.
If everything goes fine this will be 1.0 final (there might be a release candidate or two before this date).
The plans for the next release are:
<ul>
<li>Bugfixes, write more tests, more bugfixes, more tests.
<li>Proposal for changed license.
<li>For other plans, see the new ''Roadmap'' part on the web site.
</ul>
');

INSERT INTO ITEM VALUES(5,
'New version available: 0.9 Beta / 2006-07-29', '2006-07-30 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Changes and new functionality:</b>
<ul>
<li>ParameterMetaData is now implemented
<li>Experimental auto-complete functionality in the H2 Console.
  Does not yet work for all cases.
  Press [Ctrl]+[Space] to activate, and [Esc] to deactivate it.
<li>1.0/3.0 is now 0.33333... and not 0.3 as before.
  The scale of a DECIMAL division is adjusted automatically (up to current scale + 25).
<li>''SELECT * FROM TEST'' can now be written as ''FROM TEST SELECT *''
<li>New parameter schemaName in Trigger.init.
<li>New method DatabaseEventListener.init to pass the database URL.
<li>    Opening a database that was not closed previously is now faster
    (specially if using a database URL of the form jdbc:h2:test;LOG=2)
<li>Improved performance for Statement.getGeneratedKeys
</ul>
<b>Bugfixes:</b>
<ul>
<li>SCRIPT: The system generated indexes are not any more included in the script file.
    Also, the drop statements for generated sequences are not included in the script any longer.
<li>Bugfix: IN(NULL) didn''t return NULL in every case. Fixed.
<li>Bugfix: DATEDIFF didn''t work correctly for hour, minute and second if one of the dates was before 1970.
<li>SELECT EXCEPT (or MINUS) did not work for some cases. Fixed.
<li>DATEDIFF now returns a BIGINT and not an INT
<li>DATEADD didn''t work for milliseconds.
<li>Could not connect to a database that was closing at the same time.
<li>C-style block comments /* */ are not parsed correctly when they contain * or /
</ul>
For details see also the history. The plans for the next release are:
<ul>
<li>Bugfixes, write more tests, more bugfixes, more tests.
<li>Proposal for changed license.
<li>For other plans, see the new ''Roadmap'' part on the web site.
</ul>
');

INSERT INTO ITEM VALUES(4,
'New version available: 0.9 Beta / 2006-07-14', '2006-07-14 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Changes and new functionality:</b>
<ul>
<li>The cache size is now measured in blocks and no longer in rows.
    Manually setting the cache size is no longer necessary in most cases.
<li>CREATE VIEW now supports a column list: CREATE VIEW TESTV(A, B) AS ...
<li>New column IS_GENERATED in the metadata tables SEQUENCES and INDEXES.
<li>ResultSetMetaData.isNullable is now implemented.
<li>Optimization: data conversion of constants was not optimized.
<li>Optimization: deterministic subqueries are evaluated only once.
<li>Compatibility: ''T'', ''Y'', ''YES'', ''F'', ''N'', ''NO'' (case insensitive) can now also be converted to boolean.
<li>Compatibility: SUBSTRING(string FROM start FOR length).
<li>Compatibility: TRIM(whitespace FROM string).
<li>LIKE ... ESCAPE: The escape character may now also be an expression.
<li>IF EXISTS / IF NOT EXISTS implemented for the remaining CREATE / DROP statements.
<li>An exception was thrown if a scalar subquery returned no rows. Now NULL is returned.
<li>Objects of unknown type are no longer serialized to a byte array.
<li>Reduced jar file size: The regression tests are no longer included in the jar file.
</ul>
<b>Bugfixes:</b>
<ul>
<li>Issue #123: The connection to the server is lost if an abnormal exception occurs.
<li>Issue #124: Adding a column didn''t work when the table contains a referential integrity check.
<li>Issue #125: Foreign key constraints of local temporary tables are not dropped when the table is dropped.
<li>Issue #126: It is possible to create multiple primary keys for the same table.
<li>A few bugs in the CSV tool have been fixed.
</ul>
For details see also the history. The plans for the next release are:
<ul>
<li>Bugfixes, write more tests, more bugfixes, more tests.
<li>Proposal for changed license.
<li>For other plans, see the new ''Roadmap'' part on the web site.
</ul>
');

INSERT INTO ITEM VALUES(3,
'New version available: 0.9 Beta / 2006-07-01', '2006-07-01 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Changes and new functionality:</b>
<ul>
<li>Reduced memory usage: implemented a String cache and improved the Value cache.
    Now uses a weak reference to avoid OutOfMemory.
    (however the row cache could still cause OutOfMemory).
<li>New functions: MEMORY_FREE() and MEMORY_USED().
<li>Newsfeed sample application (used to create the newsfeed and newsletter).
<li>Server: changed the public API to allow an application to deal easier with start problems.
</ul>
<b>Bugfixes:</b>
<ul>
<li>Issue #116: Server: reduces memory usage. Reduced number of cached objects per connection.
<li>Issue #117: Server.start...Server sometimes returned before the server was started. Solved.
<li>Issue #118: ALTER TABLE RENAME COLUMN didn''t work correctly.
<li>Issue #119: If a table with autoincrement column is created in another schema,
        it was not possible to connect to the database again.
<li>Issue #120: Some ALTER TABLE statements didn''t work when the table was in another than the main schema. Fixed.
<li>Issue #121: Using a quoted table or alias name in front of a column name (SELECT "TEST".ID FROM TEST) didn''t work.
<li>Database names are no longer case sensitive for the Windows operating system,
    because there the files names are not case sensitive.
<li>Issue #122: Using OFFSET in big result sets (disk buffered result sets) did not work. Fixed.
<li>Outer joins did not always use an index even if this was possible.
</ul>
For details see also the history. The plans for the next release are:
<ul>
<li>Bugfixes, write more tests, more bugfixes, more tests
<li>Proposal for changed license (still pending...)
<li>For other plans, see the new ''Roadmap'' part on the web site
</ul>
');

INSERT INTO ITEM VALUES(2,
'New version available: 0.9 Beta / 2006-06-16', '2006-06-16 12:00:00',
'A new version of H2 is available for <a href="http://www.h2database.com">download</a>.
<br/>
<b>Changes and new functionality:</b>
<ul>
<li>It''s now Beta version; don''t expect much added functionality until 1.0
<li>New XML encoding functions (XMLTEXT, XMLATTR, XMLNODE,...).
<li>New functions FORMATDATETIME and PARSEDATETIME
<li>Performance: improved opening of a large databases
<li>Performance: improved creating and opening encrypted databases
<li>Blob.getLength() and Clob.getLength() are now fast operations
<li>Documented ALTER TABLE DROP COLUMN
<li>Implemented DROP TRIGGER
<li>Calling Server.start...Server now waits until the server is ready
<li>If a connection is closed while there is still an operation running, this operation is stopped
    Implemented distributing lob files into directories, and only keep up to 255 files in one directory.
    However this is disabled by default; it will be enabled the next time the file format changes
    (maybe not before 1.1). It can be enabled by the application by setting
    Constants.LOB_FILES_IN_DIRECTORIES = true
</ul>
<b>Bugfixes:</b>
<ul>
<li>Issue #110: PreparedStatement.setCharacterStream
<li>Issue #111: The catalog name was not uppercase
<li>Issue #112: Two threads could not open the same database at the same time
<li>Issue #113: Drop is now restricted
<li>Issue #114: Support large index data size
<li>Issue #115: Lock timeout for three or more threads
</ul>
For details see also the history. The plans for the next release are:
<ul>
<li>Bugfixes, write more tests, more bugfixes, more tests
<li>Proposal for changed license (still pending...)
<li>For other plans, see the new ''Roadmap'' part on the web site
</ul>
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