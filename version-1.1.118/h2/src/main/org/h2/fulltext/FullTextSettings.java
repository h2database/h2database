/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
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

/**
 * The global settings of a full text search.
 */
class FullTextSettings {

    private static final HashMap<String, FullTextSettings> SETTINGS = New.hashMap();

    private boolean initialized;
    private HashSet<String> ignoreList = New.hashSet();
    private HashMap<String, Integer> words = New.hashMap();
    private HashMap<Integer, IndexInfo> indexes = New.hashMap();
    private PreparedStatement prepSelectMapByWordId;
    private PreparedStatement prepSelectRowById;

    private FullTextSettings() {
        // don't allow construction
    }

    HashSet<String> getIgnoreList() {
        return ignoreList;
    }

    HashMap<String, Integer> getWordList() {
        return words;
    }

    /**
     * Get the index information for the given index id.
     *
     * @param indexId the index id
     * @return the index info
     */
    IndexInfo getIndexInfo(int indexId) {
        return indexes.get(indexId);
    }

    /**
     * Add an index.
     *
     * @param index the index
     */
    void addIndexInfo(IndexInfo index) {
        indexes.put(index.id, index);
    }

    /**
     * Convert a word to uppercase. This method returns null if the word is in
     * the ignore list.
     *
     * @param word the word to convert and check
     * @return the uppercase version of the word or null
     */
    String convertWord(String word) {
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
    static FullTextSettings getInstance(Connection conn) throws SQLException {
        String path = getIndexPath(conn);
        FullTextSettings setting = SETTINGS.get(path);
        if (setting == null) {
            setting = new FullTextSettings();
            SETTINGS.put(path, setting);
        }
        return setting;
    }

    private static String getIndexPath(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("CALL IFNULL(DATABASE_PATH(), 'MEM:' || DATABASE())");
        rs.next();
        String path = rs.getString(1);
        if ("MEM:UNNAMED".equals(path)) {
            throw new SQLException("FULLTEXT", "Fulltext search for private (unnamed) in-memory databases is not supported.");
        }
        rs.close();
        return path;
    }

    PreparedStatement getPrepSelectMapByWordId() {
        return prepSelectMapByWordId;
    }

    void setPrepSelectMapByWordId(PreparedStatement prepSelectMapByWordId) {
        this.prepSelectMapByWordId = prepSelectMapByWordId;
    }

    PreparedStatement getPrepSelectRowById() {
        return prepSelectRowById;
    }

    void setPrepSelectRowById(PreparedStatement prepSelectRowById) {
        this.prepSelectRowById = prepSelectRowById;
    }

    /**
     * Remove all indexes from the settings.
     */
    void removeAllIndexes() {
        indexes.clear();
    }

    /**
     * Remove an index from the settings.
     *
     * @param index the index to remove
     */
    void removeIndexInfo(IndexInfo index) {
        indexes.remove(index.id);
    }

    void setInitialized(boolean b) {
        this.initialized = b;
    }

    boolean isInitialized() {
        return initialized;
    }

}
