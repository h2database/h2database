/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.fulltext;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.h2.api.Trigger;
import org.h2.tools.SimpleResultSet;
import org.h2.util.ByteUtils;
import org.h2.util.StringUtils;

public class FullText implements Trigger {

   private static final String TRIGGER_PREFIX = "FT_";
   private static final String SCHEMA = "FT";
   private static final String FIELD_QUERY = "query";
   private IndexInfo index;
   private int[] dataTypes;
   private PreparedStatement prepInsertWord, prepInsertRow, prepInsertMap;
   private PreparedStatement prepDeleteRow, prepDeleteMap;
   private PreparedStatement prepSelectRow;

   /**
    * Create a new full text index for a table and column list. Each table may only have one index at any time.
    *
    * @param conn the connection
    * @param name the name of the index (must be unique)
    * @param schema the schema name of the table
    * @param table the table name
    * @param columnList the column list (null for all columns)
    */
   public static void createIndex(Connection conn, String schema, String table, String columnList) throws SQLException {
       init(conn);
       PreparedStatement prep = conn.prepareStatement("INSERT INTO "+SCHEMA+".INDEXES(SCHEMA, TABLE, COLUMNS) VALUES(?, ?, ?)");
       prep.setString(1, schema);
       prep.setString(2, table);
       prep.setString(3, columnList);
       prep.execute();
       createTrigger(conn, schema, table);
       indexExistingRows(conn, schema, table);
   }

   private static void createTrigger(Connection conn, String schema, String table) throws SQLException {
       Statement stat = conn.createStatement();
       String trigger = StringUtils.quoteIdentifier(schema) + "." + StringUtils.quoteIdentifier(TRIGGER_PREFIX + table);
       stat.execute("DROP TRIGGER IF EXISTS " + trigger);
       StringBuffer buff = new StringBuffer("CREATE TRIGGER IF NOT EXISTS ");
       buff.append(trigger);
       buff.append(" AFTER INSERT, UPDATE, DELETE ON ");
       buff.append(StringUtils.quoteIdentifier(schema) + "." + StringUtils.quoteIdentifier(table));
       buff.append(" FOR EACH ROW CALL \"");
       buff.append(FullText.class.getName());
       buff.append("\"");
       stat.execute(buff.toString());
   }

   private static void indexExistingRows(Connection conn, String schema, String table) throws SQLException {
       FullText existing = new FullText();
       existing.init(conn, schema, null, table);
       StringBuffer buff = new StringBuffer("SELECT * FROM ");
       buff.append(StringUtils.quoteIdentifier(schema) + "." + StringUtils.quoteIdentifier(table));
       ResultSet rs = conn.createStatement().executeQuery(buff.toString());
       int columnCount = rs.getMetaData().getColumnCount();
       while(rs.next()) {
           Object[] row = new Object[columnCount];
           for(int i=0; i<columnCount; i++) {
               row[i] = rs.getObject(i+1);
           }
           existing.fire(conn, null, row);
       }
   }

   /**
    * Re-creates the full text index for this database
    *
    * @param conn the connection
    */
   public static void reindex(Connection conn) throws SQLException {
       init(conn);
       removeAllTriggers(conn);
       FullTextSettings setting = FullTextSettings.getInstance(conn);
       setting.getWordList().clear();
       Statement stat = conn.createStatement();
       stat.execute("TRUNCATE TABLE "+SCHEMA+".WORDS");
       stat.execute("TRUNCATE TABLE "+SCHEMA+".ROWS");
       stat.execute("TRUNCATE TABLE "+SCHEMA+".MAP");
       ResultSet rs = stat.executeQuery("SELECT * FROM "+SCHEMA+".INDEXES");
       while(rs.next()) {
           String schema = rs.getString("SCHEMA");
           String table = rs.getString("TABLE");
           createTrigger(conn, schema, table);
           indexExistingRows(conn, schema, table);
       }
   }

   /**
    * Change the ignore list. The ignore list is a comma separated list of common words that must
    * not be indexed. The default ignore list is empty. If indexes already exist at the time this list is changed,
    * reindex must be called.
    * 
    * @param conn the connection
    * @param commaSeparatedList the list
    */
   public static void setIgnoreList(Connection conn, String commaSeparatedList) throws SQLException {
       init(conn);
       FullTextSettings setting = FullTextSettings.getInstance(conn);
       setIgnoreList(setting, commaSeparatedList);
       Statement stat = conn.createStatement();
       stat.execute("TRUNCATE TABLE "+SCHEMA+".IGNORELIST");
       PreparedStatement prep = conn.prepareStatement("INSERT INTO "+SCHEMA+".IGNORELIST VALUES(?)");
       prep.setString(1, commaSeparatedList);
       prep.execute();
   }

   private static void setIgnoreList(FullTextSettings setting, String commaSeparatedList) {
       String[] list = StringUtils.arraySplit(commaSeparatedList, ',', true);
       HashSet set = setting.getIgnoreList();
       for(int i=0; i<list.length; i++) {
           String word = list[i];
           word = setting.convertWord(word);
           if(word != null) {
               set.add(list[i]);
           }
       }
   }

   private static void removeAllTriggers(Connection conn) throws SQLException {
       Statement stat = conn.createStatement();
       ResultSet rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.TRIGGERS");
       Statement stat2 = conn.createStatement();
       while(rs.next()) {
           String schema = rs.getString("TRIGGER_SCHEMA");
           String name = rs.getString("TRIGGER_NAME");
           if(name.startsWith(TRIGGER_PREFIX)) {
               name = StringUtils.quoteIdentifier(schema) + "." + StringUtils.quoteIdentifier(name);
               stat2.execute("DROP TRIGGER " + name);
           }
       }
   }

   /**
    * Drops all full text indexes from the database.
    * @param conn the connection
    */
   public static void dropAll(Connection conn) throws SQLException {
       init(conn);
       Statement stat = conn.createStatement();
       stat.execute("DROP SCHEMA IF EXISTS " + SCHEMA);
       removeAllTriggers(conn);
       FullTextSettings setting = FullTextSettings.getInstance(conn);
       setting.getIgnoreList().clear();
       setting.getWordList().clear();
   }

   /**
    * Initializes full text search functionality for this database.
    * This adds the following Java functions to the database:
    * <ul>
    * <li>FT_CREATE_INDEX(schemaNameString, tableNameString, columnListString)
    * <li>FT_SEARCH(queryString, limitInt, offsetInt): result set
    * <li>FT_REINDEX()
    * <li>FT_DROP_ALL()
    * </ul>
    * It also adds a schema FULLTEXT to the database where bookkeeping information is stored.
    * This function may be called from a Java application, or by using the SQL statements:
    * <pre>
    * CREATE ALIAS IF NOT EXISTS FULLTEXT_INIT FOR "org.h2.fulltext.FullText.init";
    * CALL FULLTEXT_INIT();
    * </pre>
    *
    * @param conn
    */
   public static void init(Connection conn) throws SQLException {
       Statement stat = conn.createStatement();
       stat.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
       stat.execute("CREATE TABLE IF NOT EXISTS "+SCHEMA+".INDEXES(ID INT AUTO_INCREMENT PRIMARY KEY, SCHEMA VARCHAR, TABLE VARCHAR, COLUMNS VARCHAR, UNIQUE(SCHEMA, TABLE))");
       stat.execute("CREATE MEMORY TABLE IF NOT EXISTS "+SCHEMA+".WORDS(ID INT AUTO_INCREMENT PRIMARY KEY, NAME VARCHAR, UNIQUE(NAME))");
       stat.execute("CREATE TABLE IF NOT EXISTS "+SCHEMA+".ROWS(ID IDENTITY, HASH INT, INDEXID INT, KEY VARCHAR, UNIQUE(HASH, INDEXID, KEY))");
              
       // 3391, 1484
//       stat.execute("CREATE TABLE IF NOT EXISTS "+SCHEMA+".MAP(ROWID INT, WORDID INT, UNIQUE(ROWID, WORDID), UNIQUE(WORDID, ROWID))");
       
       // 3063, 1484
       stat.execute("CREATE TABLE IF NOT EXISTS "+SCHEMA+".MAP(ROWID INT, WORDID INT, PRIMARY KEY(WORDID, ROWID))");
       
       stat.execute("CREATE TABLE IF NOT EXISTS "+SCHEMA+".IGNORELIST(LIST VARCHAR)");
       stat.execute("CREATE ALIAS IF NOT EXISTS FT_CREATE_INDEX FOR \"" + FullText.class.getName()+".createIndex\"");
       stat.execute("CREATE ALIAS IF NOT EXISTS FT_SEARCH FOR \"" + FullText.class.getName()+".search\"");
       stat.execute("CREATE ALIAS IF NOT EXISTS FT_REINDEX FOR \"" + FullText.class.getName()+".reindex\"");
       stat.execute("CREATE ALIAS IF NOT EXISTS FT_DROP_ALL FOR \"" + FullText.class.getName()+".dropAll\"");
       FullTextSettings setting = FullTextSettings.getInstance(conn);
       ResultSet rs = stat.executeQuery("SELECT * FROM " + SCHEMA+".IGNORELIST");
       while(rs.next()) {
           String commaSeparatedList = rs.getString(1);
           setIgnoreList(setting, commaSeparatedList);
       }
       rs = stat.executeQuery("SELECT * FROM " + SCHEMA+".WORDS");
       HashMap map = setting.getWordList();
       while(rs.next()) {
           String word = rs.getString("NAME");
           long id = rs.getLong("ID");
           word = setting.convertWord(word);
           if(word != null) {
               map.put(word, new Long(id));
           }
       }
   }

   /**
    * INTERNAL
    */
   public void init(Connection conn, String schemaName, String triggerName, String tableName) throws SQLException {
       init(conn);
       FullTextSettings setting = FullTextSettings.getInstance(conn);
       ArrayList keyList = new ArrayList();
       DatabaseMetaData meta = conn.getMetaData();
       ResultSet rs = meta.getColumns(null, schemaName, tableName, null);
       ArrayList columnList = new ArrayList();
       while(rs.next()) {
           columnList.add(rs.getString("COLUMN_NAME"));
       }
       dataTypes = new int[columnList.size()];
       index = new IndexInfo();
       index.schemaName = schemaName;
       index.tableName = tableName;
       index.columnNames = new String[columnList.size()];
       columnList.toArray(index.columnNames);
       rs = meta.getColumns(null, schemaName, tableName, null);
       for(int i=0; rs.next(); i++) {
           dataTypes[i] = rs.getInt("DATA_TYPE");
       }
       if(keyList.size() == 0) {
           rs = meta.getPrimaryKeys(null, schemaName, tableName);
           while(rs.next()) {
               keyList.add(rs.getString("COLUMN_NAME"));
           }
       }
       if(keyList.size() == 0) {
           throw new SQLException("No primary key for table " + tableName);
       }
       ArrayList indexList = new ArrayList();
       PreparedStatement prep = conn.prepareStatement(
           "SELECT ID, COLUMNS FROM "+SCHEMA+".INDEXES WHERE SCHEMA=? AND TABLE=?");
       prep.setString(1, schemaName);
       prep.setString(2, tableName);
       rs = prep.executeQuery();
       if(rs.next()) {
           index.id = rs.getInt(1);
           String columns = rs.getString(2);
           if(columns != null) {
               String[] list = StringUtils.arraySplit(columns, ',', true);
               for(int i=0; i<list.length; i++) {
                   indexList.add(list[i]);
               }
           }
       }
       if(indexList.size() == 0) {
           indexList.addAll(columnList);
       }
       index.keys = new int[keyList.size()];
       setColumns(index.keys, keyList, columnList);
       index.indexColumns = new int[indexList.size()];
       setColumns(index.indexColumns, indexList, columnList);
       setting.addIndexInfo(index);
       prepInsertWord = conn.prepareStatement("INSERT INTO "+SCHEMA+".WORDS(NAME) VALUES(?)");
       prepInsertRow = conn.prepareStatement("INSERT INTO "+SCHEMA+".ROWS(HASH, INDEXID, KEY) VALUES(?, ?, ?)");
       prepInsertMap = conn.prepareStatement("INSERT INTO "+SCHEMA+".MAP(ROWID, WORDID) VALUES(?, ?)");
       prepDeleteRow = conn.prepareStatement("DELETE FROM "+SCHEMA+".ROWS WHERE HASH=? AND INDEXID=? AND KEY=?");
       prepDeleteMap = conn.prepareStatement("DELETE FROM "+SCHEMA+".MAP WHERE ROWID=? AND WORDID=?");
       prepSelectRow = conn.prepareStatement("SELECT ID FROM "+SCHEMA+".ROWS WHERE HASH=? AND INDEXID=? AND KEY=?");

       PreparedStatement prepSelectMapByWordId = conn.prepareStatement("SELECT ROWID FROM "+SCHEMA+".MAP WHERE WORDID=?");
       PreparedStatement prepSelectRowById = conn.prepareStatement("SELECT KEY, INDEXID FROM "+SCHEMA+".ROWS WHERE ID=?");
       setting.setPrepSelectMapByWordId(prepSelectMapByWordId);
       setting.setPrepSelectRowById(prepSelectRowById);
   }

   private void setColumns(int[] index, ArrayList keys, ArrayList columns) throws SQLException {
       for(int i=0; i<keys.size(); i++) {
           String key = (String) keys.get(i);
           int found = -1;
           for(int j=0; found == -1 && j<columns.size(); j++) {
               String column = (String)columns.get(j);
               if(column.equals(key)) {
                   found = j;
               }
           }
           if(found < 0) {
               throw new SQLException("FULLTEXT", "Column not found: " + key);
           }
           index[i] = found;
       }
   }

   /**
    * INTERNAL
    */
   public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
       FullTextSettings setting = FullTextSettings.getInstance(conn);
       if(oldRow != null) {
           delete(setting, oldRow);
       }
       if(newRow != null) {
           insert(setting, newRow);
       }
   }
   
   private String getKey(Object[] row) throws SQLException {
       StringBuffer buff = new StringBuffer();
       for(int i=0; i<index.keys.length; i++) {
           if(i>0) {
               buff.append(" AND ");
           }
           int columnIndex = index.keys[i];
           buff.append(StringUtils.quoteIdentifier(index.columnNames[columnIndex]));
           Object o = row[columnIndex];
           if(o==null) {
               buff.append(" IS NULL");
           } else {
               buff.append("=");
               buff.append(quoteSQL(o, dataTypes[columnIndex]));
           }
       }
       String key = buff.toString();
       return key;
   }

   private String quoteString(String data) {
       if(data.indexOf('\'') < 0) {
           return "'" + data + "'";
       }
       StringBuffer buff = new StringBuffer(data.length()+2);
       buff.append('\'');
       for(int i=0; i < data.length(); i++) {
           char ch = data.charAt(i);
           if(ch == '\'') {
               buff.append(ch);
           }
           buff.append(ch);
       }
       buff.append('\'');
       return buff.toString();
   }

   private String quoteBinary(byte[] data) {
       return "'" + ByteUtils.convertBytesToString(data) + "'";
   }

   private String asString(Object data, int type) throws SQLException {
       if(data == null) {
           return "NULL";
       }
       switch(type) {
       case Types.BIT:
       case Types.BOOLEAN:
       case Types.INTEGER:
       case Types.BIGINT:
       case Types.DECIMAL:
       case Types.DOUBLE:
       case Types.FLOAT:
       case Types.NUMERIC:
       case Types.REAL:
       case Types.SMALLINT:
       case Types.TINYINT:
       case Types.DATE:
       case Types.TIME:
       case Types.TIMESTAMP:
       case Types.LONGVARCHAR:
       case Types.CHAR:
       case Types.VARCHAR:
           return data.toString();
       case Types.VARBINARY:
       case Types.LONGVARBINARY:
       case Types.BINARY:
       case Types.JAVA_OBJECT:
       case Types.CLOB:
       case Types.OTHER:
       case Types.BLOB:
       case Types.STRUCT:
       case Types.REF:
       case Types.NULL:
       case Types.ARRAY:
       case Types.DATALINK:
       case Types.DISTINCT:
           throw new SQLException("FULLTEXT", "Unsupported column data type: " + type);
       }
       return "";
   }

   private String quoteSQL(Object data, int type) throws SQLException {
       if(data == null) {
           return "NULL";
       }
       switch(type) {
       case Types.BIT:
       case Types.BOOLEAN:
       case Types.INTEGER:
       case Types.BIGINT:
       case Types.DECIMAL:
       case Types.DOUBLE:
       case Types.FLOAT:
       case Types.NUMERIC:
       case Types.REAL:
       case Types.SMALLINT:
       case Types.TINYINT:
           return data.toString();
       case Types.DATE:
       case Types.TIME:
       case Types.TIMESTAMP:
       case Types.LONGVARCHAR:
       case Types.CHAR:
       case Types.VARCHAR:
           return quoteString(data.toString());
       case Types.VARBINARY:
       case Types.LONGVARBINARY:
       case Types.BINARY:
           return quoteBinary((byte[])data);
       case Types.JAVA_OBJECT:
       case Types.CLOB:
       case Types.OTHER:
       case Types.BLOB:
       case Types.STRUCT:
       case Types.REF:
       case Types.NULL:
       case Types.ARRAY:
       case Types.DATALINK:
       case Types.DISTINCT:
           throw new SQLException("FULLTEXT", "Unsupported key data type: " + type);
       }
       return "";
   }

   private static void addWords(FullTextSettings setting, HashSet set, String text) {
       StringTokenizer tokenizer = new StringTokenizer(text, " \t\n\r\f+\"*%&/()=?'!,.;:-_#@|^~`{}[]");
       while(tokenizer.hasMoreTokens()) {
           String word =  tokenizer.nextToken();
           word = setting.convertWord(word);
           if(word != null) {
               set.add(word);
           }
       }
   }
   
   private int[] getWordIds(FullTextSettings setting, Object[] row) throws SQLException {
        HashSet words = new HashSet();
        for(int i=0; i<index.indexColumns.length; i++) {
            int idx = index.indexColumns[i];
            String data = asString(row[idx], dataTypes[idx]);
            addWords(setting, words, data);
        }
        HashMap allWords = setting.getWordList();
        int[] wordIds = new int[words.size()];
        Iterator it = words.iterator();
        for(int i=0; it.hasNext(); i++) {
            String word = (String) it.next();
            Integer wId = (Integer) allWords.get(word);
            int wordId;
            if(wId == null) {
                prepInsertWord.setString(1, word);
                prepInsertWord.execute();
                ResultSet rs = prepInsertWord.getGeneratedKeys();
                rs.next();
                wordId = rs.getInt(1);
                allWords.put(word, new Integer(wordId));
            } else {
                wordId = wId.intValue();
            }
            wordIds[i] = wordId;
        }
        Arrays.sort(wordIds);
        return wordIds;
    }

   private void insert(FullTextSettings setting, Object[] row) throws SQLException {
       String key = getKey(row);
       int hash = key.hashCode();
       prepInsertRow.setInt(1, hash);
       prepInsertRow.setLong(2, index.id);
       prepInsertRow.setString(3, key);
       prepInsertRow.execute();
       ResultSet rs = prepInsertRow.getGeneratedKeys();
       rs.next();
       long rowId = rs.getLong(1);
       prepInsertMap.setLong(1, rowId);
       int[] wordIds = getWordIds(setting, row);
       for(int i=0; i<wordIds.length; i++) {
           prepInsertMap.setInt(2, wordIds[i]);
           prepInsertMap.execute();
       }
   }

   private void delete(FullTextSettings setting, Object[] row) throws SQLException {
       String key = getKey(row);
       int hash = key.hashCode();
       prepSelectRow.setInt(1, hash);
       prepSelectRow.setLong(2, index.id);
       prepSelectRow.setString(3, key);
       ResultSet rs = prepSelectRow.executeQuery();
       if(rs.next()) {
           long rowId = rs.getLong(1);
           prepDeleteMap.setLong(1, rowId);
           int[] wordIds = getWordIds(setting, row);
           for(int i=0; i<wordIds.length; i++) {
               prepDeleteMap.setInt(2, wordIds[i]);
               prepDeleteMap.executeUpdate();
           }
           prepDeleteRow.setInt(1, hash);
           prepDeleteRow.setLong(2, index.id);
           prepDeleteRow.setString(3, key);
           prepDeleteRow.executeUpdate();
       }
   }

   /**
    * Re-creates the full text index for this database.
    *
    * @param conn the connection
    * @param text the search query
    * @param limit the maximum number of rows or 0 for no limit
    * @param offset the offset or 0 for no offset
    * @return the result set
    */
   public static ResultSet search(Connection conn, String text, int limit, int offset) throws SQLException {
       SimpleResultSet result = new SimpleResultSet();
       result.addColumn(FIELD_QUERY, Types.VARCHAR, 0, 0);
       if(text == null) {
           // this is just to query the result set columns
           return result;
       }
       FullTextSettings setting = FullTextSettings.getInstance(conn);
       HashSet words = new HashSet();
       addWords(setting, words, text);
       HashSet rIds = null, lastRowIds = null;
       HashMap allWords = setting.getWordList();
       PreparedStatement prepSelectMapByWordId = setting.getPrepSelectMapByWordId();
       for(Iterator it = words.iterator(); it.hasNext(); ) {
           lastRowIds = rIds;
           rIds = new HashSet();
           String word = (String) it.next();
           Integer wId = (Integer) allWords.get(word);
           if(wId == null) {
               continue;
           }
           prepSelectMapByWordId.setInt(1, wId.intValue());
           ResultSet rs = prepSelectMapByWordId.executeQuery();
           while(rs.next()) {
               Long rId = new Long(rs.getLong(1));
               if(lastRowIds == null || lastRowIds.contains(rId)) {
                   rIds.add(rId);
               }
           }
       }
       if(rIds == null || rIds.size() == 0) {
           return result;
       }
       PreparedStatement prepSelectRowById = setting.getPrepSelectRowById();
       int rowCount = 0;
       for(Iterator it = rIds.iterator(); it.hasNext(); ) {
           long rowId = ((Long)it.next()).longValue();
           prepSelectRowById.setLong(1, rowId);
           ResultSet rs = prepSelectRowById.executeQuery();
           if(!rs.next()) {
               continue;
           }
           if(offset > 0) {
               offset--;
           } else {
               String key = rs.getString(1);
               long indexId = rs.getLong(2);
               IndexInfo index = setting.getIndexInfo(indexId);
               String query = StringUtils.quoteIdentifier(index.schemaName)+"."+StringUtils.quoteIdentifier(index.tableName);
               query +=" WHERE " + key;
               result.addRow(new String[]{query});
               rowCount++;
               if(limit > 0 && rowCount >= limit) {
                   break;
               }
           }
       }
       return result;
   }

}
