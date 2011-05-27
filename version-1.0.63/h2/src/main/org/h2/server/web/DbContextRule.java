/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.h2.bnf.Bnf;
import org.h2.bnf.Rule;
import org.h2.bnf.Sentence;
import org.h2.command.Parser;
import org.h2.message.Message;
import org.h2.util.StringUtils;

public class DbContextRule implements Rule {
    DbContents contents;
    int type;
    static final int COLUMN = 0, TABLE = 1, TABLE_ALIAS = 2;
    public static final int NEW_TABLE_ALIAS = 3; 
    public static final int COLUMN_ALIAS = 4; 
    private static final boolean SUGGEST_TABLE_ALIAS = false;
    
    DbContextRule(DbContents contents, int type) {
        this.contents = contents;
        this.type = type;
    }

    public String name() {
        return null;
    }

    public String random(Bnf config, int level) {
        return null;
    }

    public Rule last() {
        return this;
    }

    public void setLinks(HashMap ruleMap) {
    }

    public void addNextTokenList(String query, Sentence sentence) {
        switch (type) {
        case TABLE:
            addTable(query, sentence);
            break;
        case NEW_TABLE_ALIAS:
            addNewTableAlias(query, sentence);
            break;
        case TABLE_ALIAS:
            addTableAlias(query, sentence);
            break;
        case COLUMN_ALIAS:
//            addColumnAlias(query, sentence);
//            break;
        case COLUMN:
            addColumn(query, sentence);
            break;
        default:
        }
    }
    
    private void addTableAlias(String query, Sentence sentence) {
        String q = StringUtils.toUpperEnglish(query.trim());
        HashMap map = sentence.getAliases();
        HashSet set = new HashSet();
        if (map != null) {
            for (Iterator it = map.entrySet().iterator(); it.hasNext();) {
                Map.Entry entry = (Entry) it.next();
                String alias = (String) entry.getKey();
                DbTableOrView table = (DbTableOrView) entry.getValue();
                set.add(StringUtils.toUpperEnglish(table.name));
                if (q.length() == 0 || alias.startsWith(q)) {
                    if (q.length() < alias.length()) {
                        sentence.add(alias + ".", alias.substring(q.length()) + ".", Sentence.CONTEXT);
                    }
                }
            }
        }
        HashSet tables = sentence.getTables();
        if (tables != null) {
            for (Iterator it = tables.iterator(); it.hasNext();) {
                DbTableOrView table = (DbTableOrView) it.next();
                String tableName = StringUtils.toUpperEnglish(table.name);
            //DbTableOrView[] tables = contents.defaultSchema.tables;
            //for(int i=0; i<tables.length; i++) {
            //    DbTableOrView table = tables[i];
            //    String tableName = StringUtils.toUpperEnglish(table.name);
                if (!set.contains(tableName)) {
                    if (q.length() == 0 || tableName.startsWith(q)) {
                        if (q.length() < tableName.length()) {
                            sentence.add(tableName + ".", tableName.substring(q.length()) + ".", Sentence.CONTEXT);
                        }
                    }
                }
            }
        }
    }

    private void addNewTableAlias(String query, Sentence sentence) {
        if (SUGGEST_TABLE_ALIAS) {
            // good when testing!
            if (query.length() > 3) {
                return;
            }
            String lastTableName = StringUtils.toUpperEnglish(sentence.getLastTable().name);
            if (lastTableName == null) {
                return;
            }
            HashMap map = sentence.getAliases();
            String shortName = lastTableName.substring(0, 1);
            if (map != null && map.containsKey(shortName)) {
                int result = 0;
                for (int i = 1;; i++) {
                    if (!map.containsKey(shortName + i)) {
                        result = i;
                        break;
                    }
                }
                shortName += result;
            }
            String q = StringUtils.toUpperEnglish(query.trim());
            if (q.length() == 0 || StringUtils.toUpperEnglish(shortName).startsWith(q)) {
                if (q.length() < shortName.length()) {
                    sentence.add(shortName, shortName.substring(q.length()), Sentence.CONTEXT);
                }
            }
        }
    }
    
//    private boolean startWithIgnoreCase(String a, String b) {
//        if(a.length() < b.length()) {
//            return false;
//        }
//        for(int i=0; i<b.length(); i++) {
//            if(Character.toUpperCase(a.charAt(i)) != Character.toUpperCase(b.charAt(i))) {
//                return false;
//            }
//        }
//        return true;
//    }

    private void addTable(String query, Sentence sentence) {
        DbSchema schema = contents.defaultSchema;
        String text = StringUtils.toUpperEnglish(sentence.text).trim();
        if (text.endsWith(".")) {
            for (int i = 0; i < contents.schemas.length; i++) {
                if (text.endsWith(StringUtils.toUpperEnglish(contents.schemas[i].name) + ".")) {
                    schema = contents.schemas[i];
                    break;
                }
            }
        }
        String q = StringUtils.toUpperEnglish(query.trim());
        DbTableOrView[] tables = schema.tables;
        for (int i = 0; i < tables.length; i++) {
            DbTableOrView table = tables[i];
            if (q.length() == 0 || StringUtils.toUpperEnglish(table.name).startsWith(q)) {
                if (q.length() < table.quotedName.length()) {
                    sentence.add(table.quotedName, table.quotedName.substring(q.length()), Sentence.CONTEXT);
                }
            }
        }
    }

    private void addColumn(String query, Sentence sentence) {
        String tableName = query;
        String columnPattern = "";
        if (query.trim().length() == 0) {
            tableName = null;
            if (sentence.text.trim().endsWith(".")) {
                return;
            }
        } else {
            tableName = StringUtils.toUpperEnglish(query.trim());
            if (tableName.endsWith(".")) {
                tableName = tableName.substring(0, tableName.length() - 1);
            } else {
                columnPattern = StringUtils.toUpperEnglish(query.trim());
                tableName = null;
            }
        }
        HashSet set = null;
        HashMap aliases = sentence.getAliases();
        if (tableName == null && sentence.getTables() != null) {
            set = sentence.getTables();
        }
        DbTableOrView table = null;
        if (tableName != null && aliases != null && aliases.get(tableName) != null) {
            table = (DbTableOrView) aliases.get(tableName);
            tableName = StringUtils.toUpperEnglish(table.name);
        }
        if (tableName == null) {
            if (set == null && aliases == null) {
                return;
            }
            if ((set != null && set.size() > 1) || (aliases != null && aliases.size() > 1)) {
                return;
            }
        }
        if (table == null) {
            DbTableOrView[] tables = contents.defaultSchema.tables;
            for (int i = 0; i < tables.length; i++) {
                DbTableOrView tab = tables[i];
                String t = StringUtils.toUpperEnglish(tab.name);
                if (tableName != null && !tableName.equals(t)) {
                    continue;
                }
                if (set != null && !set.contains(tab)) {
                    continue;
                }
                table = tab;
                break;
            }
        }
        if (table != null) {
            for (int j = 0; j < table.columns.length; j++) {
                String columnName = table.columns[j].name;
                if (!StringUtils.toUpperEnglish(columnName).startsWith(columnPattern)) {
                    continue;
                }
                if (columnPattern.length() < columnName.length()) {
                    sentence.add(columnName, columnName.substring(columnPattern.length()), Sentence.CONTEXT);
                }
            }
        }
    }

    public String matchRemove(String query, Sentence sentence) {
        if (query.length() == 0) {
            return null;
        }
        String s;
        switch (type) {
        case TABLE:
            s = matchTable(query, sentence);
            break;
        case NEW_TABLE_ALIAS:
            s = matchTableAlias(query, sentence, true);
            break;
        case TABLE_ALIAS:
            s = matchTableAlias(query, sentence, false);
            break;
        case COLUMN_ALIAS:
            s = matchColumnAlias(query, sentence, false);
            break;
        case COLUMN:
            s = matchColumn(query, sentence);
            break;
        default:
            throw Message.getInternalError("type=" + type);
        }
        return s;
    }

    public String matchTable(String query, Sentence sentence) {
        String up = StringUtils.toUpperEnglish(query);
        DbTableOrView[] tables = contents.defaultSchema.tables;
        String best = null;
        DbTableOrView bestTable = null;
        for (int i = 0; i < tables.length; i++) {
            DbTableOrView table = tables[i];
            String tableName = StringUtils.toUpperEnglish(table.name);
            if (up.startsWith(tableName)) {
                if (best == null || tableName.length() > best.length()) {
                    best = tableName;
                    bestTable = table;
                }
            }
        }
        if (best == null) {
            return null;
        }
        sentence.addTable(bestTable);
        query = query.substring(best.length());
        // while(query.length()>0 && Character.isWhitespace(query.charAt(0))) {
        // query = query.substring(1);
        // }
        return query;
    }

    public String matchColumnAlias(String query, Sentence sentence, boolean add) {
        String up = StringUtils.toUpperEnglish(query);
        int i = 0;
        if (query.indexOf(' ') < 0) {
            return null;
        }
        for (; i < up.length(); i++) {
            char ch = up.charAt(i);
            if (ch != '_' && !Character.isLetterOrDigit(ch)) {
                break;
            }
        }
        if (i == 0) {
            return null;
        }
        String alias = up.substring(0, i);
        if (Parser.isKeyword(alias)) {
            return null;
        }
        return query.substring(alias.length());
    }

    public String matchTableAlias(String query, Sentence sentence, boolean add) {
        String up = StringUtils.toUpperEnglish(query);
        int i = 0;
        if (query.indexOf(' ') < 0) {
            return null;
        }
        for (; i < up.length(); i++) {
            char ch = up.charAt(i);
            if (ch != '_' && !Character.isLetterOrDigit(ch)) {
                break;
            }
        }
        if (i == 0) {
            return null;
        }
        String alias = up.substring(0, i);
        if (Parser.isKeyword(alias)) {
            return null;
        }
        if (add) {
            sentence.addAlias(alias, sentence.getLastTable());
        }
        HashMap map = sentence.getAliases();
        if ((map != null && map.containsKey(alias)) || (sentence.getLastTable() == null)) {
            if (add && query.length() == alias.length()) {
                return query;
            }
            query = query.substring(alias.length());
            return query;
        } else {
            HashSet tables = sentence.getTables();
            if (tables != null) {
                String best = null;
                for (Iterator it = tables.iterator(); it.hasNext();) {
                    DbTableOrView table = (DbTableOrView) it.next();
                    String tableName = StringUtils.toUpperEnglish(table.name);
                //DbTableOrView[] tables = contents.defaultSchema.tables;
                //for(int i=0; i<tables.length; i++) {
                //    DbTableOrView table = tables[i];
                //    String tableName = StringUtils.toUpperEnglish(table.name);
                    if (alias.startsWith(tableName) && (best == null || tableName.length() > best.length())) {
                        best = tableName;
                    }
                }
                if (best != null) {
                    query = query.substring(best.length());
                    return query;
                }
            }
            return null;
        }
    }

    public String matchColumn(String query, Sentence sentence) {
        String up = StringUtils.toUpperEnglish(query);
        HashSet set = sentence.getTables();
        DbTableOrView[] tables = contents.defaultSchema.tables;
        String best = null;
        for (int i = 0; i < tables.length; i++) {
            DbTableOrView table = tables[i];
            if (set != null && !set.contains(table)) {
                continue;
            }
            for (int j = 0; j < table.columns.length; j++) {
                String name = StringUtils.toUpperEnglish(table.columns[j].name);
                if (up.startsWith(name)) {
                    String b = query.substring(name.length());
                    if (best == null || b.length() < best.length()) {
                        best = b;
                    }
                }
            }
        }
        return best;
    }
}
