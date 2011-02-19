/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.jaqu;

import java.text.MessageFormat;
import org.h2.jaqu.TableDefinition.IndexDefinition;
import org.h2.jaqu.util.StatementBuilder;
import org.h2.jaqu.util.StringUtils;

/**
 * Interface that defines points where JaQu can build different statements for
 * DB-specific SQL. 
 *
 */
public interface SQLDialect {
    
    public String tableName(String schema, String table);
    
    public String createIndex(String schema, String table, IndexDefinition index);
    
    public void appendLimit(SQLStatement stat, long limit);
    
    public void appendOffset(SQLStatement stat, long offset);
    
    /**
     *  Default implementation of an SQL dialect.
     *  Designed for an H2 database.  May be suitable for others.
     */
    public static class DefaultSQLDialect implements SQLDialect {
        
        @Override
        public String tableName(String schema, String table) {            
            if (StringUtils.isNullOrEmpty(schema))
                return table;
            return schema + "." + table;
        }
        
        @Override
        public String createIndex(String schema, String table, IndexDefinition index) {
            StatementBuilder cols = new StatementBuilder();
            for (String col:index.columnNames) {
                cols.appendExceptFirst(", ");
                cols.append(col);
            }
            String type;
            switch(index.type) {
            case UNIQUE:
                type = " UNIQUE ";
                break;
            case HASH:
                type = " HASH ";
                break;
            case UNIQUE_HASH:
                type = " UNIQUE HASH ";
                break;
            case STANDARD:
            default:
                type = " ";
                break;
            }
                
            return MessageFormat.format("CREATE{0}INDEX IF NOT EXISTS {1} ON {2}({3})",
                    type, index.indexName, table, cols);
        }
        
        @Override
        public void appendLimit(SQLStatement stat, long limit) {
            stat.appendSQL(" LIMIT " + limit);
        }
        
        @Override
        public void appendOffset(SQLStatement stat, long offset) {
            stat.appendSQL(" OFFSET " + offset);
        }
    }   
}
