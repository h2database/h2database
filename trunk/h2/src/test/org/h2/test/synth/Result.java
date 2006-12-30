/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.util.*;

import org.h2.test.TestBase;

class Result implements Comparable {
    static final int SUCCESS=0, BOOLEAN=1, INT=2, EXCEPTION=3, RESULTSET=4;
    private int type;
    private boolean bool;
    private int intValue;
    private SQLException exception;
    private ArrayList rows;
    private ArrayList header;
    String sql;
    
    Result(String sql) {
        this.sql = sql;
        type = SUCCESS;  
    }
    
    Result(String sql, SQLException e) {
        this.sql = sql;
        type = EXCEPTION;
        exception = e;
    }
    
    Result(String sql, boolean b) {
        this.sql = sql;
        type = BOOLEAN;
        this.bool = b; 
    }
    
    Result(String sql, int i) {
        this.sql = sql;
        type = INT;
        this.intValue = i;    
    }
    
    Result(TestSynth config, String sql, ResultSet rs) {
        this.sql = sql;
        type = RESULTSET;
        try {
            rows = new ArrayList();
            header = new ArrayList();
            ResultSetMetaData meta = rs.getMetaData();
            int len = meta.getColumnCount();
            Column[] cols = new Column[len];
            for(int i=0; i<len; i++) {
                cols[i] = new Column(meta, i+1);
            }
            while(rs.next()) {
                Row row = new Row(config, rs, len);
                rows.add(row);
            }
            Collections.sort(rows);
        } catch(SQLException e) {
//            type = EXCEPTION;
//            exception = e;
            TestBase.logError("error reading result set", e);
        }
    }

    public String toString() {
        switch(type) {
        case SUCCESS:
            return "success";
        case BOOLEAN:
            return "boolean: " + this.bool;
        case INT:
            return "int: " + this.intValue;
        case EXCEPTION: {
            StringWriter w = new StringWriter();
            exception.printStackTrace(new PrintWriter(w));
            return "exception: "+exception.getSQLState()+": "+exception.getMessage() + "\r\n"+w.toString();
        }
        case RESULTSET:
            String result = "ResultSet { // size=" + rows.size() + "\r\n  ";
            for(int i=0; i<header.size(); i++) {
                Column column = (Column)header.get(i);
                result += column.toString() + "; ";
            }
            result += "} = {\r\n";
            for(int i=0; i<rows.size(); i++) {
                Row row = (Row)rows.get(i);
                result += "  { " + row.toString() + "};\r\n";
            }
            return result + "}";
        default:
            throw new Error("internal");
        }
    }
    
    public int compareTo(Object o) {
        Result r = (Result)o;
        switch(type) {
        case EXCEPTION:
            if(r.type != EXCEPTION) {
                return 1;
            }
            return 0;
//            return exception.getSQLState().compareTo(r.exception.getSQLState());
        case BOOLEAN:
        case INT:
        case SUCCESS:
        case RESULTSET:
            return toString().compareTo(r.toString());
        default:
            throw new Error("internal");
        }
    }
    
    public void log() {
        switch(type) {
        case SUCCESS:
            System.out.println("> ok");        
            break;
        case EXCEPTION:
            System.out.println("> exception");        
            break;
        case INT:
            if(intValue==0) {
                System.out.println("> ok");                
            } else {
                System.out.println("> update count: "+intValue);                
            }
            break;
        case RESULTSET:
            System.out.println("> rs "+rows.size());    
            break;
        }
        System.out.println();
    }

}
