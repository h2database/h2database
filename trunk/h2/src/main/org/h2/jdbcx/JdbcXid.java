/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbcx;

import java.sql.SQLException;
import java.util.StringTokenizer;

//#ifdef JDK14
import javax.transaction.xa.Xid;
//#endif

import org.h2.message.Message;
import org.h2.message.TraceObject;
import org.h2.util.ByteUtils;

public class JdbcXid extends TraceObject 
//#ifdef JDK14
implements Xid 
//#endif
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
            if(!PREFIX.equals(prefix)) {
                throw Message.getSQLException(Message.WRONG_XID_FORMAT_1, tid);
            }
            formatId = Integer.parseInt(tokenizer.nextToken());
            branchQualifier = ByteUtils.convertStringToBytes(tokenizer.nextToken());
            globalTransactionId = ByteUtils.convertStringToBytes(tokenizer.nextToken());
        } catch(Throwable e) {
            throw Message.getSQLException(Message.WRONG_XID_FORMAT_1, tid);
        }
    }
    
//    private JdbcXid(JdbcDataSourceFactory factory, int id, Xid xid) {
//        setTrace(factory.getTrace(), TraceObject.XID, id);        
//        this.formatId = xid.getFormatId();
//        this.branchQualifier = clone(xid.getBranchQualifier());
//        this.globalTransactionId = clone(xid.getGlobalTransactionId());
//    }
    
    public String getAsString() {
        StringBuffer buff = new StringBuffer(PREFIX);
        buff.append('_');
        buff.append(formatId);
        buff.append('_');
        buff.append(ByteUtils.convertBytesToString(branchQualifier));
        buff.append('_');
        buff.append(ByteUtils.convertBytesToString(globalTransactionId));
        return buff.toString();
    }
    
//    private byte[] clone(byte[] data) {
//        byte[] d2 = new byte[data.length];
//        System.arraycopy(data, 0, d2, 0, data.length);
//        return d2;
//    }
    
    public int getFormatId() {
        debugCodeCall("getFormatId");
        return formatId;
    }

    public byte[] getBranchQualifier() {
        debugCodeCall("getBranchQualifier");        
        return branchQualifier;
    }

    public byte[] getGlobalTransactionId() {
        debugCodeCall("getGlobalTransactionId");                
        return globalTransactionId;
    }

}
