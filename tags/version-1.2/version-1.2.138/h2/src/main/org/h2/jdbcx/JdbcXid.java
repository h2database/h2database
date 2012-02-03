/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbcx;

import java.util.StringTokenizer;
import javax.transaction.xa.Xid;
import org.h2.constant.ErrorCode;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.util.Utils;

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

    JdbcXid(JdbcDataSourceFactory factory, int id, String tid) {
        setTrace(factory.getTrace(), TraceObject.XID, id);
        try {
            StringTokenizer tokenizer = new StringTokenizer(tid, "_");
            String prefix = tokenizer.nextToken();
            if (!PREFIX.equals(prefix)) {
                throw DbException.get(ErrorCode.WRONG_XID_FORMAT_1, tid);
            }
            formatId = Integer.parseInt(tokenizer.nextToken());
            branchQualifier = Utils.convertStringToBytes(tokenizer.nextToken());
            globalTransactionId = Utils.convertStringToBytes(tokenizer.nextToken());
        } catch (RuntimeException e) {
            throw DbException.get(ErrorCode.WRONG_XID_FORMAT_1, tid);
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
            append(Utils.convertBytesToString(branchQualifier)).
            append('_').
            append(Utils.convertBytesToString(globalTransactionId));
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
