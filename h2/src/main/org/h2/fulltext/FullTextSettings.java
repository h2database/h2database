/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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

import org.h2.util.ObjectUtils;

public class FullTextSettings {

    private static HashMap settings = new HashMap();

    private HashSet ignoreList = new HashSet();
    private HashMap words = new HashMap();
    private HashMap indexes = new HashMap();
    private PreparedStatement prepSelectMapByWordId;
    private PreparedStatement prepSelectRowById;

    private FullTextSettings() {
    }

    HashSet getIgnoreList() {
        return ignoreList;
    }

    public HashMap getWordList() {
        return words;
    }

    IndexInfo getIndexInfo(long indexId) {
        return (IndexInfo) indexes.get(ObjectUtils.getLong(indexId));
    }

    void addIndexInfo(IndexInfo index) {
        indexes.put(ObjectUtils.getLong(index.id), index);
    }

    public String convertWord(String word) {
        // TODO this is locale specific, document
        word = word.toUpperCase();
        if(ignoreList.contains(word)) {
            return null;
        }
        return word;
    }

    static FullTextSettings getInstance(Connection conn) throws SQLException {
        String path = getIndexPath(conn);
        FullTextSettings setting = (FullTextSettings) settings.get(path);
        if(setting == null) {
            setting = new FullTextSettings();
            settings.put(path, setting);
        }
        return setting;
    }

    private static String getIndexPath(Connection conn) throws SQLException {
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

    public PreparedStatement getPrepSelectMapByWordId() {
        return prepSelectMapByWordId;
    }

    public void setPrepSelectMapByWordId(PreparedStatement prepSelectMapByWordId) {
        this.prepSelectMapByWordId = prepSelectMapByWordId;
    }

    public PreparedStatement getPrepSelectRowById() {
        return prepSelectRowById;
    }

    public void setPrepSelectRowById(PreparedStatement prepSelectRowById) {
        this.prepSelectRowById = prepSelectRowById;
    }

}
