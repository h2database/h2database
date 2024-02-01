/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbcx;

import java.util.Base64;
import javax.transaction.xa.Xid;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.message.TraceObject;

/**
 * An object of this class represents a transaction id.
 */
public final class JdbcXid extends TraceObject implements Xid {

    private static final String PREFIX = "XID";

    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

    private final int formatId;
    private final byte[] branchQualifier;
    private final byte[] globalTransactionId;

    JdbcXid(JdbcDataSourceFactory factory, int id, String tid) {
        setTrace(factory.getTrace(), TraceObject.XID, id);
        try {
            String[] splits = tid.split("\\|");
            if (splits.length == 4 && PREFIX.equals(splits[0])) {
                formatId = Integer.parseInt(splits[1]);
                Base64.Decoder decoder = Base64.getUrlDecoder();
                branchQualifier = decoder.decode(splits[2]);
                globalTransactionId = decoder.decode(splits[3]);
                return;
            }
        } catch (IllegalArgumentException e) {
        }
        throw DbException.get(ErrorCode.WRONG_XID_FORMAT_1, tid);
    }

    /**
     * INTERNAL
     * @param builder to put result into
     * @param xid to provide string representation for
     * @return provided StringBuilder
     */
    static StringBuilder toString(StringBuilder builder, Xid xid) {
        return builder.append(PREFIX).append('|').append(xid.getFormatId()) //
                .append('|').append(ENCODER.encodeToString(xid.getBranchQualifier())) //
                .append('|').append(ENCODER.encodeToString(xid.getGlobalTransactionId()));
    }

    /**
     * Get the format id.
     *
     * @return the format id
     */
    @Override
    public int getFormatId() {
        debugCodeCall("getFormatId");
        return formatId;
    }

    /**
     * The transaction branch identifier.
     *
     * @return the identifier
     */
    @Override
    public byte[] getBranchQualifier() {
        debugCodeCall("getBranchQualifier");
        return branchQualifier;
    }

    /**
     * The global transaction identifier.
     *
     * @return the transaction id
     */
    @Override
    public byte[] getGlobalTransactionId() {
        debugCodeCall("getGlobalTransactionId");
        return globalTransactionId;
    }

}
