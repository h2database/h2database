/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.h2.bnf.Bnf;
import org.h2.bnf.Rule;
import org.h2.bnf.RuleHead;
import org.h2.bnf.Sentence;
import org.h2.command.Parser;
import org.h2.message.Message;
import org.h2.util.New;
import org.h2.util.StringUtils;

/**
 * A BNF terminal rule that is linked to the database context information.
 * This class is used by the H2 Console, to support auto-complete.
 */
public class DbContextRule implements Rule {

    static final int COLUMN = 0, TABLE = 1, TABLE_ALIAS = 2;
    static final int NEW_TABLE_ALIAS = 3;
    static final int COLUMN_ALIAS = 4, SCHEMA = 5;
    private static final boolean SUGGEST_TABLE_ALIAS = false;

    private DbContents contents;
    private int type;

    DbContextRule(DbContents contents, int type) {
        this.contents = contents;
        this.type = type;
    }

    public String toString() {
        switch (type) {
        case SCHEMA:
            return "schema";
        case TABLE:
            return "table";
        case NEW_TABLE_ALIAS:
            return "nt";
        case TABLE_ALIAS:
            return "t";
        case COLUMN_ALIAS:
            return "c";
        case COLUMN:
            return "column";
        default:
            return "?";
        }
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

    public void setLinks(HashMap<String, RuleHead> ruleMap) {
        // nothing to do
    }

    public void addNextTokenList(Sentence sentence) {
        switch (type) {
        case SCHEMA:
            addSchema(sentence);
            break;
        case TABLE:
            addTable(sentence);
            break;
        case NEW_TABLE_ALIAS:
            addNewTableAlias(sentence);
            break;
        case TABLE_ALIAS:
            addTableAlias(sentence);
            break;
        case COLUMN_ALIAS:
//            addColumnAlias(query, sentence);
//            break;
        case COLUMN:
            addColumn(sentence);
            break;
        default:
        }
    }

    private void addTableAlias(Sentence sentence) {
        String query = sentence.getQuery();
        String q = StringUtils.toUpperEnglish(query.trim());
        HashMap<String, DbTableOrView> map = sentence.getAliases();
        HashSet<String> set = New.hashSet();
        if (map != null) {
            for (Map.Entry<String, DbTableOrView> entry : map.entrySet()) {
                String alias = entry.getKey();
                DbTableOrView table = entry.getValue();
                set.add(StringUtils.toUpperEnglish(table.name));
                if (q.length() == 0 || alias.startsWith(q)) {
                    if (q.length() < alias.length()) {
                        sentence.add(alias + ".", alias.substring(q.length()) + ".", Sentence.CONTEXT);
                    }
                }
            }
        }
        HashSet<DbTableOrView> tables = sentence.getTables();
        if (tables != null) {
            for (DbTableOrView table : tables) {
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

    private void addNewTableAlias(Sentence sentence) {
        String query = sentence.getQuery();
        if (SUGGEST_TABLE_ALIAS) {
            // good when testing!
            if (query.length() > 3) {
                return;
            }
            String lastTableName = StringUtils.toUpperEnglish(sentence.getLastTable().name);
            if (lastTableName == null) {
                return;
            }
            HashMap<String, DbTableOrView> map = sentence.getAliases();
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
//            if(Character.toUpperCase(a.charAt(i))
//                    != Character.toUpperCase(b.charAt(i))) {
//                return false;
//            }
//        }
//        return true;
//    }

    private void addSchema(Sentence sentence) {
        String query = sentence.getQuery();
        String q = StringUtils.toUpperEnglish(query);
        if (q.trim().length() == 0) {
            q = q.trim();
        }
        for (DbSchema schema : contents.schemas) {
            if (schema == contents.defaultSchema) {
                continue;
            }
            if (q.length() == 0 || StringUtils.toUpperEnglish(schema.name).startsWith(q)) {
                if (q.length() < schema.quotedName.length()) {
                    sentence.add(schema.quotedName + ".", schema.quotedName.substring(q.length()) + ".", Sentence.CONTEXT);
                }
            }
        }
    }

    private void addTable(Sentence sentence) {
        String query = sentence.getQuery();
        DbSchema schema = sentence.getLastMatchedSchema();
        if (schema == null) {
            schema = contents.defaultSchema;
        }
        String q = StringUtils.toUpperEnglish(query);
        if (q.trim().length() == 0) {
            q = q.trim();
        }
        for (DbTableOrView table : schema.tables) {
            if (q.length() == 0 || StringUtils.toUpperEnglish(table.name).startsWith(q)) {
                if (q.length() < table.quotedName.length()) {
                    sentence.add(table.quotedName, table.quotedName.substring(q.length()), Sentence.CONTEXT);
                }
            }
        }
    }

    private void addColumn(Sentence sentence) {
        String query = sentence.getQuery();
        String tableName = query;
        String columnPattern = "";
        if (query.trim().length() == 0) {
            tableName = null;
        } else {
            tableName = StringUtils.toUpperEnglish(query.trim());
            if (tableName.endsWith(".")) {
                tableName = tableName.substring(0, tableName.length() - 1);
            } else {
                columnPattern = StringUtils.toUpperEnglish(query.trim());
                tableName = null;
            }
        }
        HashSet<DbTableOrView> set = null;
        HashMap<String, DbTableOrView> aliases = sentence.getAliases();
        if (tableName == null && sentence.getTables() != null) {
            set = sentence.getTables();
        }
        DbTableOrView table = null;
        if (tableName != null && aliases != null && aliases.get(tableName) != null) {
            table = aliases.get(tableName);
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
            for (DbTableOrView tab : contents.defaultSchema.tables) {
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
        if (table != null && table.columns != null) {
            for (DbColumn column : table.columns) {
                String columnName = column.name;
                if (!StringUtils.toUpperEnglish(columnName).startsWith(columnPattern)) {
                    continue;
                }
                if (columnPattern.length() < columnName.length()) {
                    sentence.add(columnName, columnName.substring(columnPattern.length()), Sentence.CONTEXT);
                }
            }
        }
    }

    public boolean matchRemove(Sentence sentence) {
        if (sentence.getQuery().length() == 0) {
            return false;
        }
        String s;
        switch (type) {
        case SCHEMA:
            s = matchSchema(sentence);
            break;
        case TABLE:
            s = matchTable(sentence);
            break;
        case NEW_TABLE_ALIAS:
            s = matchTableAlias(sentence, true);
            break;
        case TABLE_ALIAS:
            s = matchTableAlias(sentence, false);
            break;
        case COLUMN_ALIAS:
            s = matchColumnAlias(sentence);
            break;
        case COLUMN:
            s = matchColumn(sentence);
            break;
        default:
            throw Message.throwInternalError("type=" + type);
        }
        if (s == null) {
            return false;
        }
        sentence.setQuery(s);
        return true;
    }

    private String matchSchema(Sentence sentence) {
        String query = sentence.getQuery();
        String up = sentence.getQueryUpper();
        DbSchema[] schemas = contents.schemas;
        String best = null;
        DbSchema bestSchema = null;
        for (DbSchema schema: schemas) {
            String schemaName = StringUtils.toUpperEnglish(schema.name);
            if (up.startsWith(schemaName)) {
                if (best == null || schemaName.length() > best.length()) {
                    best = schemaName;
                    bestSchema = schema;
                }
            }
        }
        sentence.setLastMatchedSchema(bestSchema);
        if (best == null) {
            return null;
        }
        query = query.substring(best.length());
        // while(query.length()>0 && Character.isWhitespace(query.charAt(0))) {
        // query = query.substring(1);
        // }
        return query;
    }

    private String matchTable(Sentence sentence) {
        String query = sentence.getQuery();
        String up = sentence.getQueryUpper();
        DbSchema schema = sentence.getLastMatchedSchema();
        if (schema == null) {
            schema = contents.defaultSchema;
        }
        DbTableOrView[] tables = schema.tables;
        String best = null;
        DbTableOrView bestTable = null;
        for (DbTableOrView table : tables) {
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

    private String matchColumnAlias(Sentence sentence) {
        String query = sentence.getQuery();
        String up = sentence.getQueryUpper();
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
        if (Parser.isKeyword(alias, true)) {
            return null;
        }
        return query.substring(alias.length());
    }

    private String matchTableAlias(Sentence sentence, boolean add) {
        String query = sentence.getQuery();
        String up = sentence.getQueryUpper();
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
        if (Parser.isKeyword(alias, true)) {
            return null;
        }
        if (add) {
            sentence.addAlias(alias, sentence.getLastTable());
        }
        HashMap<String, DbTableOrView> map = sentence.getAliases();
        if ((map != null && map.containsKey(alias)) || (sentence.getLastTable() == null)) {
            if (add && query.length() == alias.length()) {
                return query;
            }
            query = query.substring(alias.length());
            return query;
        }
        HashSet<DbTableOrView> tables = sentence.getTables();
        if (tables != null) {
            String best = null;
            for (DbTableOrView table : tables) {
                String tableName = StringUtils.toUpperEnglish(table.name);
                //DbTableOrView[] tables = contents.defaultSchema.tables;
                //for(int i=0; i<tables.length; i++) {
                //    DbTableOrView table = tables[i];
                //    String tableName = StringUtils.toUpperEnglish(table.name);
                if (alias.startsWith(tableName) && (best == null || tableName.length() > best.length())) {
                    sentence.setLastMatchedTable(table);
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

    private String matchColumn(Sentence sentence) {
        String query = sentence.getQuery();
        String up = sentence.getQueryUpper();
        HashSet<DbTableOrView> set = sentence.getTables();
        String best = null;
        DbTableOrView last = sentence.getLastMatchedTable();
        if (last != null && last.columns != null) {
            for (DbColumn column : last.columns) {
                String name = StringUtils.toUpperEnglish(column.name);
                if (up.startsWith(name)) {
                    String b = query.substring(name.length());
                    if (best == null || b.length() < best.length()) {
                        best = b;
                    }
                }
            }
        }
        for (DbTableOrView table : contents.defaultSchema.tables) {
            if (table != last && set != null && !set.contains(table)) {
                continue;
            }
            if (table == null || table.columns == null) {
                continue;
            }
            for (DbColumn column : table.columns) {
                String name = StringUtils.toUpperEnglish(column.name);
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
