/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.util.HashMap;
import java.util.HashSet;
import org.h2.bnf.BnfVisitor;
import org.h2.bnf.Rule;
import org.h2.bnf.RuleHead;
import org.h2.bnf.Sentence;
import org.h2.command.Parser;
import org.h2.message.DbException;
import org.h2.util.StringUtils;

/**
 * A BNF terminal rule that is linked to the database context information.
 * This class is used by the H2 Console, to support auto-complete.
 */
public class DbContextRule implements Rule {

    static final int COLUMN = 0, TABLE = 1, TABLE_ALIAS = 2;
    static final int NEW_TABLE_ALIAS = 3;
    static final int COLUMN_ALIAS = 4, SCHEMA = 5;

    private final DbContents contents;
    private final int type;

    DbContextRule(DbContents contents, int type) {
        this.contents = contents;
        this.type = type;
    }

    @Override
    public void setLinks(HashMap<String, RuleHead> ruleMap) {
        // nothing to do
    }

    @Override
    public void accept(BnfVisitor visitor) {
        // nothing to do
    }

    @Override
    public boolean autoComplete(Sentence sentence) {
        String query = sentence.getQuery(), s = query;
        String up = sentence.getQueryUpper();
        switch (type) {
        case SCHEMA: {
            DbSchema[] schemas = contents.schemas;
            String best = null;
            DbSchema bestSchema = null;
            for (DbSchema schema: schemas) {
                String name = StringUtils.toUpperEnglish(schema.name);
                if (up.startsWith(name)) {
                    if (best == null || name.length() > best.length()) {
                        best = name;
                        bestSchema = schema;
                    }
                } else if (s.length() == 0 || name.startsWith(up)) {
                    if (s.length() < name.length()) {
                        sentence.add(name, name.substring(s.length()), type);
                        sentence.add(schema.quotedName + ".", schema.quotedName.substring(s.length()) + ".", Sentence.CONTEXT);
                    }
                }
            }
            if (best != null) {
                sentence.setLastMatchedSchema(bestSchema);
                s = s.substring(best.length());
            }
            break;
        }
        case TABLE: {
            DbSchema schema = sentence.getLastMatchedSchema();
            if (schema == null) {
                schema = contents.defaultSchema;
            }
            DbTableOrView[] tables = schema.tables;
            String best = null;
            DbTableOrView bestTable = null;
            for (DbTableOrView table : tables) {
                String compare = up;
                String name = StringUtils.toUpperEnglish(table.name);
                if (table.quotedName.length() > name.length()) {
                    name = table.quotedName;
                    compare = query;
                }
                if (compare.startsWith(name)) {
                    if (best == null || name.length() > best.length()) {
                        best = name;
                        bestTable = table;
                    }
                } else if (s.length() == 0 || name.startsWith(compare)) {
                    if (s.length() < name.length()) {
                        sentence.add(table.quotedName, table.quotedName.substring(s.length()), Sentence.CONTEXT);
                    }
                }
            }
            if (best != null) {
                sentence.setLastMatchedTable(bestTable);
                sentence.addTable(bestTable);
                s = s.substring(best.length());
            }
            break;
        }
        case NEW_TABLE_ALIAS:
            s = autoCompleteTableAlias(sentence, true);
            break;
        case TABLE_ALIAS:
            s = autoCompleteTableAlias(sentence, false);
            break;
        case COLUMN_ALIAS: {
            int i = 0;
            if (query.indexOf(' ') < 0) {
                break;
            }
            for (; i < up.length(); i++) {
                char ch = up.charAt(i);
                if (ch != '_' && !Character.isLetterOrDigit(ch)) {
                    break;
                }
            }
            if (i == 0) {
                break;
            }
            String alias = up.substring(0, i);
            if (Parser.isKeyword(alias, true)) {
                break;
            }
            s = s.substring(alias.length());
            break;
        }
        case COLUMN: {
            HashSet<DbTableOrView> set = sentence.getTables();
            String best = null;
            DbTableOrView last = sentence.getLastMatchedTable();
            if (last != null && last.columns != null) {
                for (DbColumn column : last.columns) {
                    String compare = up;
                    String name = StringUtils.toUpperEnglish(column.name);
                    if (column.quotedName.length() > name.length()) {
                        name = column.quotedName;
                        compare = query;
                    }
                    if (compare.startsWith(name)) {
                        String b = s.substring(name.length());
                        if (best == null || b.length() < best.length()) {
                            best = b;
                        } else if (s.length() == 0 || name.startsWith(compare)) {
                            if (s.length() < name.length()) {
                                sentence.add(column.name, column.name.substring(s.length()), Sentence.CONTEXT);
                            }
                        }
                    }
                }
            }
            for (DbSchema schema : contents.schemas) {
                for (DbTableOrView table : schema.tables) {
                    if (table != last && set != null && !set.contains(table)) {
                        continue;
                    }
                    if (table == null || table.columns == null) {
                        continue;
                    }
                    for (DbColumn column : table.columns) {
                        String name = StringUtils.toUpperEnglish(column.name);
                        if (up.startsWith(name)) {
                            String b = s.substring(name.length());
                            if (best == null || b.length() < best.length()) {
                                best = b;
                            }
                        } else if (s.length() == 0 || name.startsWith(up)) {
                            if (s.length() < name.length()) {
                                sentence.add(column.name, column.name.substring(s.length()), Sentence.CONTEXT);
                            }
                        }
                    }
                }
            }
            if (best != null) {
                s = best;
            }
            break;
        }
        default:
            throw DbException.throwInternalError("type=" + type);
        }
        if (!s.equals(query)) {
            while (s.length() > 0 && Character.isSpaceChar(s.charAt(0))) {
                s = s.substring(1);
            }
            sentence.setQuery(s);
            return true;
        }
        return false;
    }

    private static String autoCompleteTableAlias(Sentence sentence, boolean newAlias) {
        String s = sentence.getQuery();
        String up = sentence.getQueryUpper();
        int i = 0;
        for (; i < up.length(); i++) {
            char ch = up.charAt(i);
            if (ch != '_' && !Character.isLetterOrDigit(ch)) {
                break;
            }
        }
        if (i == 0) {
            return s;
        }
        String alias = up.substring(0, i);
        if ("SET".equals(alias) || Parser.isKeyword(alias, true)) {
            return s;
        }
        if (newAlias) {
            sentence.addAlias(alias, sentence.getLastTable());
        }
        HashMap<String, DbTableOrView> map = sentence.getAliases();
        if ((map != null && map.containsKey(alias)) || (sentence.getLastTable() == null)) {
            if (newAlias && s.length() == alias.length()) {
                return s;
            }
            s = s.substring(alias.length());
            if (s.length() == 0) {
                sentence.add(alias + ".", ".", Sentence.CONTEXT);
            }
            return s;
        }
        HashSet<DbTableOrView> tables = sentence.getTables();
        if (tables != null) {
            String best = null;
            for (DbTableOrView table : tables) {
                String tableName = StringUtils.toUpperEnglish(table.name);
                if (alias.startsWith(tableName) && (best == null || tableName.length() > best.length())) {
                    sentence.setLastMatchedTable(table);
                    best = tableName;
                } else if (s.length() == 0 || tableName.startsWith(alias)) {
                    sentence.add(tableName + ".", tableName.substring(s.length()) + ".", Sentence.CONTEXT);
                }
            }
            if (best != null) {
                s = s.substring(best.length());
                if (s.length() == 0) {
                    sentence.add(alias + ".", ".", Sentence.CONTEXT);
                }
                return s;
            }
        }
        return s;
    }

}
