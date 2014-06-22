/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.fulltext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import org.h2.util.New;
import org.h2.util.SoftHashMap;

/**
 * The global settings of a full text search.
 */
class FullTextSettings {

    /**
     * The settings of open indexes.
     */
    private static final HashMap<String, FullTextSettings> SETTINGS = New.hashMap();

    /**
     * Whether this instance has been initialized.
     */
    private boolean initialized;

    /**
     * The set of words not to index (stop words).
     */
    private final HashSet<String> ignoreList = New.hashSet();

    /**
     * The set of words / terms.
     */
    private final HashMap<String, Integer> words = New.hashMap();

    /**
     * The set of indexes in this database.
     */
    private final HashMap<Integer, IndexInfo> indexes = New.hashMap();

    /**
     * The prepared statement cache.
     */
    private final SoftHashMap<Connection,
            SoftHashMap<String, PreparedStatement>> cache =
            new SoftHashMap<Connection, SoftHashMap<String, PreparedStatement>>();

    /**
     * The whitespace characters.
     */
    private String whitespaceChars = " \t\n\r\f+\"*%&/()=?'!,.;:-_#@|^~`{}[]<>\\";

    /**
     * Create a new instance.
     */
    protected FullTextSettings() {
        // don't allow construction
    }

    /**
     * Get the ignore list.
     *
     * @return the ignore list
     */
    protected HashSet<String> getIgnoreList() {
        return ignoreList;
    }

    /**
     * Get the word list.
     *
     * @return the word list
     */
    protected HashMap<String, Integer> getWordList() {
        return words;
    }

    /**
     * Get the index information for the given index id.
     *
     * @param indexId the index id
     * @return the index info
     */
    protected IndexInfo getIndexInfo(int indexId) {
        return indexes.get(indexId);
    }

    /**
     * Add an index.
     *
     * @param index the index
     */
    protected void addIndexInfo(IndexInfo index) {
        indexes.put(index.id, index);
    }

    /**
     * Convert a word to uppercase. This method returns null if the word is in
     * the ignore list.
     *
     * @param word the word to convert and check
     * @return the uppercase version of the word or null
     */
    protected String convertWord(String word) {
        // TODO this is locale specific, document
        word = word.toUpperCase();
        if (ignoreList.contains(word)) {
            return null;
        }
        return word;
    }

    /**
     * Get or create the fulltext settings for this database.
     *
     * @param conn the connection
     * @return the settings
     */
    protected static FullTextSettings getInstance(Connection conn)
            throws SQLException {
        String path = getIndexPath(conn);
        FullTextSettings setting = SETTINGS.get(path);
        if (setting == null) {
            setting = new FullTextSettings();
            SETTINGS.put(path, setting);
        }
        return setting;
    }

    /**
     * Get the file system path.
     *
     * @param conn the connection
     * @return the file system path
     */
    protected static String getIndexPath(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(
                "CALL IFNULL(DATABASE_PATH(), 'MEM:' || DATABASE())");
        rs.next();
        String path = rs.getString(1);
        if ("MEM:UNNAMED".equals(path)) {
            throw FullText.throwException(
                    "Fulltext search for private (unnamed) " +
                    "in-memory databases is not supported.");
        }
        rs.close();
        return path;
    }

    /**
     * Prepare a statement. The statement is cached in a soft reference cache.
     *
     * @param conn the connection
     * @param sql the statement
     * @return the prepared statement
     */
    protected synchronized PreparedStatement prepare(Connection conn, String sql)
            throws SQLException {
        SoftHashMap<String, PreparedStatement> c = cache.get(conn);
        if (c == null) {
            c = new SoftHashMap<String, PreparedStatement>();
            cache.put(conn, c);
        }
        PreparedStatement prep = c.get(sql);
        if (prep != null && prep.getConnection().isClosed()) {
            prep = null;
        }
        if (prep == null) {
            prep = conn.prepareStatement(sql);
            c.put(sql, prep);
        }
        return prep;
    }

    /**
     * Remove all indexes from the settings.
     */
    protected void removeAllIndexes() {
        indexes.clear();
    }

    /**
     * Remove an index from the settings.
     *
     * @param index the index to remove
     */
    protected void removeIndexInfo(IndexInfo index) {
        indexes.remove(index.id);
    }

    /**
     * Set the initialized flag.
     *
     * @param b the new value
     */
    protected void setInitialized(boolean b) {
        this.initialized = b;
    }

    /**
     * Get the initialized flag.
     *
     * @return whether this instance is initialized
     */
    protected boolean isInitialized() {
        return initialized;
    }

    /**
     * Close all fulltext settings, freeing up memory.
     */
    protected static void closeAll() {
        SETTINGS.clear();
    }

    protected void setWhitespaceChars(String whitespaceChars) {
        this.whitespaceChars = whitespaceChars;
    }

    protected String getWhitespaceChars() {
        return whitespaceChars;
    }

}
