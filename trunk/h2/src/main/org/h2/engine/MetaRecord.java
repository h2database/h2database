/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;
import java.util.Comparator;

import org.h2.api.DatabaseEventListener;
import org.h2.command.Prepared;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.SearchRow;
import org.h2.util.ObjectArray;
import org.h2.value.ValueInt;
import org.h2.value.ValueString;

public class MetaRecord {
    private int id;
    private int objectType;
    private int headPos;
    private String sql;
    
    public MetaRecord(SearchRow r) throws SQLException {
        id = r.getValue(0).getInt();
        headPos = r.getValue(1).getInt();
        objectType = r.getValue(2).getInt();
        sql = r.getValue(3).getString();         
    }
    
    public static void sort(ObjectArray records) {
        records.sort(new Comparator() {
            public int compare(Object o1, Object o2) {
                MetaRecord m1 = (MetaRecord)o1;
                MetaRecord m2 = (MetaRecord)o2;
                int c1 = DbObject.getCreateOrder(m1.getObjectType());
                int c2 = DbObject.getCreateOrder(m2.getObjectType());
                if(c1 != c2) {
                    return c1 - c2;
                }
                return m1.getId() - m2.getId();
            }
        });
    }
    
    public void setRecord(SearchRow r) {
        r.setValue(0, ValueInt.get(id));
        r.setValue(1, ValueInt.get(headPos));
        r.setValue(2, ValueInt.get(objectType));
        r.setValue(3, ValueString.get(sql));
    }    
    
    public MetaRecord(DbObject obj) {
        id = obj.getId();
        objectType = obj.getType();
        headPos = obj.getHeadPos();
        sql = obj.getCreateSQL();
    }

    void execute(Database db, Session systemSession, DatabaseEventListener listener) throws SQLException {
        try {
            Prepared command = systemSession.prepare(sql);
            command.setObjectId(id);
            command.setHeadPos(headPos);
            command.update();
        } catch(Throwable e) {
            SQLException s = Message.addSQL(Message.convert(e), sql);
            db.getTrace(Trace.DATABASE).error(sql, s);
            if(listener != null) {
                listener.exceptionThrown(s, sql);
                // continue startup in this case
            } else {
                throw s;
            }
        }
    }

    public int getHeadPos() {
        return headPos;
    }

    public void setHeadPos(int headPos) {
        this.headPos = headPos;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getObjectType() {
        return objectType;
    }

    public void setObjectType(int objectType) {
        this.objectType = objectType;
    }

    public String getSQL() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

}
