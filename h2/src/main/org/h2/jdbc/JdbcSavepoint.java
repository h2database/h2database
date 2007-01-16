/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.SQLException;
//#ifdef JDK14
import java.sql.Savepoint;
//#endif

import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceObject;
import org.h2.util.StringUtils;

/**
 * A savepoint is a point inside a transaction to where a transaction can be rolled back. 
 * The tasks that where done before the savepoint are not rolled back in this case.
 */
public class JdbcSavepoint extends TraceObject 
//#ifdef JDK14
implements Savepoint
//#endif
{

    static final String SYSTEM_SAVEPOINT_PREFIX = "SYSTEM_SAVEPOINT_";

    private int savepointId;
    private String name;
    private JdbcConnection conn;

    JdbcSavepoint(JdbcConnection conn, int savepointId, String name, Trace trace, int id) {
        setTrace(trace, TraceObject.SAVEPOINT, id);
        this.conn = conn;
        this.savepointId = savepointId;
        this.name = name;
    }

    void release() {
        this.conn = null;
    }

    static String getName(String name, int id) {
        if (name != null) {
            return StringUtils.quoteJavaString(name);
        } else {
            return SYSTEM_SAVEPOINT_PREFIX + id;
        }
    }

    void rollback() throws SQLException {
        checkValid();
        conn.prepareCommand("ROLLBACK TO SAVEPOINT " + getName(name, savepointId)).executeUpdate();
    }

    private void checkValid() throws SQLException {
        if (conn == null) {
            throw Message.getSQLException(Message.SAVEPOINT_IS_INVALID_1, getName(name, savepointId));
        }
    }

    /**
     * Get the generated id of this savepoint.
     * @return the id
     */
    public int getSavepointId() throws SQLException {
        try {
            debugCodeCall("getSavepointId");
            checkValid();
            if (name != null) {
                throw Message.getSQLException(Message.SAVEPOINT_IS_NAMED);
            }
            return savepointId;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }
    
    /**
     * Get the name of this savepoint.
     * @return the name
     */    
    public String getSavepointName() throws SQLException {
        try {
            debugCodeCall("getSavepointName");
            checkValid();
            if (name == null) {
                throw Message.getSQLException(Message.SAVEPOINT_IS_UNNAMED);
            }
            return name;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

}
