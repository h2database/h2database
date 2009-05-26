/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbcx;

import java.sql.SQLException;
import java.util.StringTokenizer;

//## Java 1.4 begin ##
import javax.transaction.xa.Xid;
//## Java 1.4 end ##

import org.h2.constant.ErrorCode;
import org.h2.message.Message;
import org.h2.message.TraceObject;
import org.h2.util.ByteUtils;

/**
 * An object of this class represents a transaction id.
 */
public class JdbcXid extends TraceObject
//## Java 1.4 begin ##
implements Xid
//## Java 1.4 end ##
{

    private static final String PREFIX = "XID";

    private int formatId;
    private byte[] branchQualifier;
    private byte[] globalTransactionId;

    JdbcXid(JdbcDataSourceFactory factory, int id, String tid) throws SQLException {
        setTrace(factory.getTrace(), TraceObject.XID, id);
        try {
            StringTokenizer tokenizer = new StringTokenizer(tid, "_");
            String prefix = tokenizer.nextToken();
            if (!PREFIX.equals(prefix)) {
                throw Message.getSQLException(ErrorCode.WRONG_XID_FORMAT_1, tid);
            }
            formatId = Integer.parseInt(tokenizer.nextToken());
            branchQualifier = ByteUtils.convertStringToBytes(tokenizer.nextToken());
            globalTransactionId = ByteUtils.convertStringToBytes(tokenizer.nextToken());
        } catch (RuntimeException e) {
            throw Message.getSQLException(ErrorCode.WRONG_XID_FORMAT_1, tid);
        }
    }

    /**
     * INTERNAL
     */
    public String getAsString() {
        StringBuilder buff = new StringBuilder(PREFIX);
        buff.append('_').
            append(formatId).
            append('_').
            append(ByteUtils.convertBytesToString(branchQualifier)).
            append('_').
            append(ByteUtils.convertBytesToString(globalTransactionId));
        return buff.toString();
    }

    /**
     * Get the format id.
     *
     * @return the format id
     */
    public int getFormatId() {
        debugCodeCall("getFormatId");
        return formatId;
    }

    /**
     * The transaction branch identifier.
     *
     * @return the identifier
     */
    public byte[] getBranchQualifier() {
        debugCodeCall("getBranchQualifier");
        return branchQualifier;
    }

    /**
     * The global transaction identifier.
     *
     * @return the transaction id
     */
    public byte[] getGlobalTransactionId() {
        debugCodeCall("getGlobalTransactionId");
        return globalTransactionId;
    }

    /**
     * INTERNAL
     */
    public String toString() {
        return getTraceObjectName() + ": " + getAsString();
    }

}
