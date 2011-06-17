/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.SQLException;
//## Java 1.4 begin ##
import java.sql.Savepoint;
//## Java 1.4 end ##

import org.h2.constant.ErrorCode;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceObject;
import org.h2.util.StringUtils;

/**
 * A savepoint is a point inside a transaction to where a transaction can be
 * rolled back. The tasks that where done before the savepoint are not rolled
 * back in this case.
 */
public class JdbcSavepoint extends TraceObject
//## Java 1.4 begin ##
implements Savepoint
//## Java 1.4 end ##
{

    private static final String SYSTEM_SAVEPOINT_PREFIX = "SYSTEM_SAVEPOINT_";

    private int savepointId;
    private String name;
    private JdbcConnection conn;

    JdbcSavepoint(JdbcConnection conn, int savepointId, String name, Trace trace, int id) {
        setTrace(trace, TraceObject.SAVEPOINT, id);
        this.conn = conn;
        this.savepointId = savepointId;
        this.name = name;
    }

    /**
     * Release this savepoint. This method only set the connection to null and
     * does not execute a statement.
     */
    void release() {
        this.conn = null;
    }

    /**
     * Get the savepoint name for this name or id.
     * If the name is null, the id is used.
     *
     * @param name the name (may be null)
     * @param id the id
     * @return the savepoint name
     */
    static String getName(String name, int id) {
        if (name != null) {
            return StringUtils.quoteJavaString(name);
        }
        return SYSTEM_SAVEPOINT_PREFIX + id;
    }

    /**
     * Roll back to this savepoint.
     */
    void rollback() throws SQLException {
        checkValid();
        conn.prepareCommand("ROLLBACK TO SAVEPOINT " + getName(name, savepointId), Integer.MAX_VALUE).executeUpdate();
    }

    private void checkValid() throws SQLException {
        if (conn == null) {
            throw Message.getSQLException(ErrorCode.SAVEPOINT_IS_INVALID_1, getName(name, savepointId));
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
                throw Message.getSQLException(ErrorCode.SAVEPOINT_IS_NAMED);
            }
            return savepointId;
        } catch (Exception e) {
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
                throw Message.getSQLException(ErrorCode.SAVEPOINT_IS_UNNAMED);
            }
            return name;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    public String toString() {
        return getTraceObjectName() + ": id=" + savepointId + " name=" + name;
    }


}
