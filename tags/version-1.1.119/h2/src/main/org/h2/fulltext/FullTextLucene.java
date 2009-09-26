/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.fulltext;

//## Java 1.4 begin ##
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexModifier;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.h2.api.CloseListener;
import org.h2.api.Trigger;
import org.h2.command.Parser;
import org.h2.engine.Session;
import org.h2.expression.ExpressionColumn;
import org.h2.jdbc.JdbcConnection;
import org.h2.store.fs.FileSystem;
import org.h2.tools.SimpleResultSet;
import org.h2.util.JdbcUtils;
import org.h2.util.New;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
//## Java 1.4 end ##

/**
 * This class implements the full text search based on Apache Lucene.
 * Most methods can be called using SQL statements as well.
 */
public class FullTextLucene extends FullText {

    /**
     * Whether the text content should be stored in the Lucene index.
     */
    static final boolean STORE_DOCUMENT_TEXT_IN_INDEX = Boolean.getBoolean("h2.storeDocumentTextInIndex");

    //## Java 1.4 begin ##
    private static final HashMap<String, IndexModifier> INDEX_MODIFIERS = New.hashMap();
    private static final String TRIGGER_PREFIX = "FTL_";
    private static final String SCHEMA = "FTL";
    private static final String FIELD_DATA = "DATA";
    private static final String FIELD_COLUMN_PREFIX = "_";
    private static final String FIELD_QUERY = "QUERY";
    //## Java 1.4 end ##

    /**
     * Initializes full text search functionality for this database. This adds
     * the following Java functions to the database:
     * <ul>
     * <li>FTL_CREATE_INDEX(schemaNameString, tableNameString,
     * columnListString)</li>
     * <li>FTL_SEARCH(queryString, limitInt, offsetInt): result set</li>
     * <li>FTL_REINDEX()</li>
     * <li>FTL_DROP_ALL()</li>
     * </ul>
     * It also adds a schema FTL to the database where bookkeeping information
     * is stored. This function may be called from a Java application, or by
     * using the SQL statements:
     *
     * <pre>
     * CREATE ALIAS IF NOT EXISTS FTL_INIT FOR
     *      &quot;org.h2.fulltext.FullTextLucene.init&quot;;
     * CALL FTL_INIT();
     * </pre>
     *
     * @param conn the connection
     */
    //## Java 1.4 begin ##
    public static void init(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
        stat.execute("CREATE TABLE IF NOT EXISTS " + SCHEMA
                        + ".INDEXES(SCHEMA VARCHAR, TABLE VARCHAR, COLUMNS VARCHAR, PRIMARY KEY(SCHEMA, TABLE))");
        stat.execute("CREATE ALIAS IF NOT EXISTS FTL_CREATE_INDEX FOR \"" + FullTextLucene.class.getName() + ".createIndex\"");
        stat.execute("CREATE ALIAS IF NOT EXISTS FTL_SEARCH FOR \"" + FullTextLucene.class.getName() + ".search\"");
        stat.execute("CREATE ALIAS IF NOT EXISTS FTL_SEARCH_DATA FOR \"" + FullTextLucene.class.getName() + ".searchData\"");
        stat.execute("CREATE ALIAS IF NOT EXISTS FTL_REINDEX FOR \"" + FullTextLucene.class.getName() + ".reindex\"");
        stat.execute("CREATE ALIAS IF NOT EXISTS FTL_DROP_ALL FOR \"" + FullTextLucene.class.getName() + ".dropAll\"");
        try {
            getIndexModifier(conn);
        } catch (Exception e) {
            throw convertException(e);
        }
    }
    //## Java 1.4 end ##

    /**
     * Create a new full text index for a table and column list. Each table may
     * only have one index at any time.
     *
     * @param conn the connection
     * @param schema the schema name of the table (case sensitive)
     * @param table the table name (case sensitive)
     * @param columnList the column list (null for all columns)
     */
    //## Java 1.4 begin ##
    public static void createIndex(Connection conn, String schema, String table, String columnList) throws SQLException {
        init(conn);
        PreparedStatement prep = conn.prepareStatement("INSERT INTO " + SCHEMA
                + ".INDEXES(SCHEMA, TABLE, COLUMNS) VALUES(?, ?, ?)");
        prep.setString(1, schema);
        prep.setString(2, table);
        prep.setString(3, columnList);
        prep.execute();
        createTrigger(conn, schema, table);
        indexExistingRows(conn, schema, table);
    }
    //## Java 1.4 end ##

    /**
     * Re-creates the full text index for this database.
     *
     * @param conn the connection
     */
    //## Java 1.4 begin ##
    public static void reindex(Connection conn) throws SQLException {
        init(conn);
        removeAllTriggers(conn, TRIGGER_PREFIX);
        removeIndexFiles(conn);
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM "+SCHEMA+".INDEXES");
        while (rs.next()) {
            String schema = rs.getString("SCHEMA");
            String table = rs.getString("TABLE");
            createTrigger(conn, schema, table);
            indexExistingRows(conn, schema, table);
        }
    }
    //## Java 1.4 end ##

    /**
     * Drops all full text indexes from the database.
     *
     * @param conn the connection
     */
    //## Java 1.4 begin ##
    public static void dropAll(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("DROP SCHEMA IF EXISTS " + SCHEMA);
        removeAllTriggers(conn, TRIGGER_PREFIX);
        removeIndexFiles(conn);
    }
    //## Java 1.4 end ##

    /**
     * Searches from the full text index for this database.
     * The returned result set has the following column:
     * <ul><li>QUERY (varchar): the query to use to get the data.
     * The query does not include 'SELECT * FROM '. Example:
     * PUBLIC.TEST WHERE ID = 1
     * </li></ul>
     *
     * @param conn the connection
     * @param text the search query
     * @param limit the maximum number of rows or 0 for no limit
     * @param offset the offset or 0 for no offset
     * @return the result set
     */
    //## Java 1.4 begin ##
    public static ResultSet search(Connection conn, String text, int limit, int offset) throws SQLException {
        return search(conn, text, limit, offset, false);
    }
    //## Java 1.4 end ##

    /**
     * Searches from the full text index for this database. The result contains
     * the primary key data as an array. The returned result set has the
     * following columns:
     * <ul>
     * <li>SCHEMA (varchar): the schema name. Example: PUBLIC </li>
     * <li>TABLE (varchar): the table name. Example: TEST </li>
     * <li>COLUMNS (array of varchar): comma separated list of quoted column
     * names. The column names are quoted if necessary. Example: (ID) </li>
     * <li>KEYS (array of values): comma separated list of values. Example: (1)
     * </li>
     * </ul>
     *
     * @param conn the connection
     * @param text the search query
     * @param limit the maximum number of rows or 0 for no limit
     * @param offset the offset or 0 for no offset
     * @return the result set
     */
    //## Java 1.4 begin ##
    public static ResultSet searchData(Connection conn, String text, int limit, int offset) throws SQLException {
        return search(conn, text, limit, offset, true);
    }

    /**
     * Convert an exception to a fulltext exception.
     *
     * @param e the original exception
     * @return the converted SQL exception
     */
    static SQLException convertException(Exception e) {
        SQLException e2 = new SQLException("FULLTEXT", "Error while indexing document");
        e2.initCause(e);
        return e2;
    }

    private static void createTrigger(Connection conn, String schema, String table) throws SQLException {
        Statement stat = conn.createStatement();
        String trigger = StringUtils.quoteIdentifier(schema) + "." + StringUtils.quoteIdentifier(TRIGGER_PREFIX + table);
        stat.execute("DROP TRIGGER IF EXISTS " + trigger);
        StringBuilder buff = new StringBuilder("CREATE TRIGGER IF NOT EXISTS ");
        buff.append(trigger).
            append(" AFTER INSERT, UPDATE, DELETE ON ").
            append(StringUtils.quoteIdentifier(schema)).
            append('.').
            append(StringUtils.quoteIdentifier(table)).
            append(" FOR EACH ROW CALL \"").
            append(FullTextLucene.FullTextTrigger.class.getName()).
            append("\"");
        stat.execute(buff.toString());
    }

    /**
     * Get the index modifier for the given connection.
     *
     * @param conn the connection
     * @return the index modifier
     */
    static IndexModifier getIndexModifier(Connection conn) throws SQLException {
        String path = getIndexPath(conn);
        IndexModifier indexer;
        synchronized (INDEX_MODIFIERS) {
            indexer = INDEX_MODIFIERS.get(path);
            if (indexer == null) {
                try {
                    boolean recreate = !IndexReader.indexExists(path);
                    Analyzer analyzer = new StandardAnalyzer();
                    indexer = new IndexModifier(path, analyzer, recreate);
                } catch (IOException e) {
                    throw convertException(e);
                }
                INDEX_MODIFIERS.put(path, indexer);
            }
        }
        return indexer;
    }

    /**
     * Get the path of the Lucene index for this database.
     *
     * @param conn the database connection
     * @return the path
     */
    static String getIndexPath(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("CALL DATABASE_PATH()");
        rs.next();
        String path = rs.getString(1);
        if (path == null) {
            throw new SQLException("FULLTEXT", "Fulltext search for in-memory databases is not supported.");
        }
        rs.close();
        return path;
    }

    private static void indexExistingRows(Connection conn, String schema, String table) throws SQLException {
        FullTextLucene.FullTextTrigger existing = new FullTextLucene.FullTextTrigger();
        existing.init(conn, schema, null, table, false, Trigger.INSERT);
        String sql = "SELECT * FROM " + StringUtils.quoteIdentifier(schema) + "." + StringUtils.quoteIdentifier(table);
        ResultSet rs = conn.createStatement().executeQuery(sql);
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            Object[] row = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                row[i] = rs.getObject(i + 1);
            }
            existing.fire(conn, null, row);
        }
    }

    private static void removeIndexFiles(Connection conn) throws SQLException {
        String path = getIndexPath(conn);
        IndexModifier index = INDEX_MODIFIERS.get(path);
        if (index != null) {
            removeIndexModifier(index, path);
        }
        FileSystem.getInstance(path).deleteRecursive(path, false);
    }

    /**
     * Close the index modifier and remove it from the index modifier set.
     *
     * @param indexModifier the index modifier
     * @param indexPath the index path
     */
    static void removeIndexModifier(IndexModifier indexModifier, String indexPath) throws SQLException {
        synchronized (INDEX_MODIFIERS) {
            try {
                INDEX_MODIFIERS.remove(indexPath);
                indexModifier.flush();
                indexModifier.close();
            } catch (Exception e) {
                throw convertException(e);
            }
        }
    }

    private static ResultSet search(Connection conn, String text, int limit, int offset, boolean data) throws SQLException {
        SimpleResultSet result = createResultSet(data);
        if (conn.getMetaData().getURL().startsWith("jdbc:columnlist:")) {
            // this is just to query the result set columns
            return result;
        }
        if (text == null || text.trim().length() == 0) {
            return result;
        }
        String path = getIndexPath(conn);
        try {
            IndexModifier indexer = getIndexModifier(conn);
            indexer.flush();
            IndexReader reader = IndexReader.open(path);
            Analyzer analyzer = new StandardAnalyzer();
            Searcher searcher = new IndexSearcher(reader);
            QueryParser parser = new QueryParser(FIELD_DATA, analyzer);
            Query query = parser.parse(text);
            Hits hits = searcher.search(query);
            int max = hits.length();
            if (limit == 0) {
                limit = max;
            }
            for (int i = 0; i < limit && i + offset < max; i++) {
                Document doc = hits.doc(i + offset);
                String q = doc.get(FIELD_QUERY);
                if (data) {
                    int idx = q.indexOf(" WHERE ");
                    JdbcConnection c = (JdbcConnection) conn;
                    Session session = (Session) c.getSession();
                    Parser p = new Parser(session);
                    String tab = q.substring(0, idx);
                    ExpressionColumn expr = (ExpressionColumn) p.parseExpression(tab);
                    String schemaName = expr.getOriginalTableAliasName();
                    String tableName = expr.getColumnName();
                    q = q.substring(idx + " WHERE ".length());
                    Object[][] columnData = parseKey(conn, q);
                    result.addRow(
                            schemaName,
                            tableName,
                            columnData[0],
                            columnData[1]);
                } else {
                    result.addRow(q);
                }
            }
            // TODO keep it open if possible
            reader.close();
        } catch (Exception e) {
            throw convertException(e);
        }
        return result;
    }
    //## Java 1.4 end ##

    /**
     * Trigger updates the index when a inserting, updating, or deleting a row.
     */
    public static class FullTextTrigger
    //## Java 1.4 begin ##
    implements Trigger, CloseListener
    //## Java 1.4 end ##
    {

        //## Java 1.4 begin ##
        private String schema;
        private String table;
        private int[] keys;
        private int[] indexColumns;
        private String[] columns;
        private int[] columnTypes;
        private String indexPath;
        private IndexModifier indexModifier;
        //## Java 1.4 end ##

        /**
         * INTERNAL
         */
        //## Java 1.4 begin ##
        public void init(Connection conn, String schemaName, String triggerName,
                String tableName, boolean before, int type) throws SQLException {
            this.schema = schemaName;
            this.table = tableName;
            this.indexPath = getIndexPath(conn);
            this.indexModifier = getIndexModifier(conn);
            ArrayList<String> keyList = New.arrayList();
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getColumns(null,
                    JdbcUtils.escapeMetaDataPattern(schemaName),
                    JdbcUtils.escapeMetaDataPattern(tableName),
                    null);
            ArrayList<String> columnList = New.arrayList();
            while (rs.next()) {
                columnList.add(rs.getString("COLUMN_NAME"));
            }
            columnTypes = new int[columnList.size()];
            columns = new String[columnList.size()];
            columnList.toArray(columns);
            rs = meta.getColumns(null,
                    JdbcUtils.escapeMetaDataPattern(schemaName),
                    JdbcUtils.escapeMetaDataPattern(tableName),
                    null);
            for (int i = 0; rs.next(); i++) {
                columnTypes[i] = rs.getInt("DATA_TYPE");
            }
            if (keyList.size() == 0) {
                rs = meta.getPrimaryKeys(null,
                        JdbcUtils.escapeMetaDataPattern(schemaName),
                        tableName);
                while (rs.next()) {
                    keyList.add(rs.getString("COLUMN_NAME"));
                }
            }
            if (keyList.size() == 0) {
                throw new SQLException("No primary key for table " + tableName);
            }
            ArrayList<String> indexList = New.arrayList();
            PreparedStatement prep = conn.prepareStatement(
                    "SELECT COLUMNS FROM " + SCHEMA
                    + ".INDEXES WHERE SCHEMA=? AND TABLE=?");
            prep.setString(1, schemaName);
            prep.setString(2, tableName);
            rs = prep.executeQuery();
            if (rs.next()) {
                String columns = rs.getString(1);
                if (columns != null) {
                    for (String s : StringUtils.arraySplit(columns, ',', true)) {
                        indexList.add(s);
                    }
                }
            }
            if (indexList.size() == 0) {
                indexList.addAll(columnList);
            }
            keys = new int[keyList.size()];
            setColumns(keys, keyList, columnList);
            indexColumns = new int[indexList.size()];
            setColumns(indexColumns, indexList, columnList);
        }
        //## Java 1.4 end ##

        /**
         * INTERNAL
         */
        //## Java 1.4 begin ##
        public void fire(Connection conn, Object[] oldRow, Object[] newRow)
                throws SQLException {
            if (oldRow != null) {
                if (newRow != null) {
                    // update
                    if (hasChanged(oldRow, newRow, indexColumns)) {
                        delete(oldRow);
                        insert(newRow);
                    }
                } else {
                    // delete
                    delete(oldRow);
                }
            } else if (newRow != null) {
                // insert
                insert(newRow);
            }
        }
        //## Java 1.4 end ##

        /**
         * INTERNAL
         */
        //## Java 1.4 begin ##
        public void close() throws SQLException {
            if (indexModifier != null) {
                removeIndexModifier(indexModifier, indexPath);
                indexModifier = null;
            }
        }
        //## Java 1.4 end ##

        /**
         * INTERNAL
         */
        public void remove() {
            // ignore
        }

        private void insert(Object[] row) throws SQLException {
            String query = getQuery(row);
            Document doc = new Document();
            doc.add(new Field(FIELD_QUERY, query, Field.Store.YES, Field.Index.UN_TOKENIZED));
            long time = System.currentTimeMillis();
            doc.add(new Field("modified", DateTools.timeToString(time, DateTools.Resolution.SECOND), Field.Store.YES, Field.Index.UN_TOKENIZED));
            StatementBuilder buff = new StatementBuilder();
            for (int index : indexColumns) {
                String columnName = columns[index];
                String data = asString(row[index], columnTypes[index]);
                doc.add(new Field(FIELD_COLUMN_PREFIX + columnName, data, Field.Store.NO, Field.Index.TOKENIZED));
                buff.appendExceptFirst(" ");
                buff.append(data);
            }
            Field.Store storeText = STORE_DOCUMENT_TEXT_IN_INDEX ? Field.Store.YES : Field.Store.NO;
            doc.add(new Field(FIELD_DATA, buff.toString(), storeText,
                    Field.Index.TOKENIZED));
            try {
                indexModifier.addDocument(doc);
            } catch (IOException e) {
                throw convertException(e);
            }
        }

        private void delete(Object[] row) throws SQLException {
            String query = getQuery(row);
            try {
                Term term = new Term(FIELD_QUERY, query);
                indexModifier.deleteDocuments(term);
            } catch (IOException e) {
                throw convertException(e);
            }
        }

        private String getQuery(Object[] row) throws SQLException {
            StatementBuilder buff = new StatementBuilder();
            if (schema != null) {
                buff.append(StringUtils.quoteIdentifier(schema)).append('.');
            }
            buff.append(StringUtils.quoteIdentifier(table)).append(" WHERE ");
            for (int columnIndex : keys) {
                buff.appendExceptFirst(" AND ");
                buff.append(StringUtils.quoteIdentifier(columns[columnIndex]));
                Object o = row[columnIndex];
                if (o == null) {
                    buff.append(" IS NULL");
                } else {
                    buff.append('=').append(FullText.quoteSQL(o, columnTypes[columnIndex]));
                }
            }
            return buff.toString();
        }
    }

}
