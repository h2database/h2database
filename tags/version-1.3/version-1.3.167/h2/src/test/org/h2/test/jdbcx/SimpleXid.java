/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbcx;

import java.util.concurrent.atomic.AtomicInteger;
import javax.transaction.xa.Xid;
import org.h2.util.MathUtils;
import org.h2.util.Utils;

/**
 * A simple Xid implementation.
 */
public class SimpleXid implements Xid {

    private static AtomicInteger next = new AtomicInteger();

    private final int formatId;
    private final byte[] branchQualifier;
    private final byte[] globalTransactionId;

    private SimpleXid(int formatId, byte[] branchQualifier, byte[] globalTransactionId) {
        this.formatId = formatId;
        this.branchQualifier = branchQualifier;
        this.globalTransactionId = globalTransactionId;
    }

    /**
     * Create a new random xid.
     *
     * @return the new object
     */
    public static SimpleXid createRandom() {
        int formatId = next.getAndIncrement();
        byte[] bq = new byte[MAXBQUALSIZE];
        MathUtils.randomBytes(bq);
        byte[] gt = new byte[MAXGTRIDSIZE];
        MathUtils.randomBytes(gt);
        return new SimpleXid(formatId, bq, gt);
    }

    public byte[] getBranchQualifier() {
        return branchQualifier;
    }

    public int getFormatId() {
        return formatId;
    }

    public byte[] getGlobalTransactionId() {
        return globalTransactionId;
    }

    public int hashCode() {
        return formatId;
    }

    public boolean equals(Object other) {
        if (other instanceof Xid) {
            Xid xid = (Xid) other;
            if (xid.getFormatId() == formatId) {
                if (Utils.compareNotNull(branchQualifier, xid.getBranchQualifier()) == 0) {
                    if (Utils.compareNotNull(globalTransactionId, xid.getGlobalTransactionId()) == 0) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public String toString() {
        return "xid:" + formatId;
    }

}
