/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: James Devenish
 */
package org.h2.test.jdbcx;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.NumberFormat;
import javax.transaction.xa.Xid;

/**
 * A utility class for the basic XA test.
 */
public class TestXid implements Xid {
    private static final NumberFormat NF;

    private static int fXidCounter;
    private int fFormatId;
    private byte[] fGlobalTransactionId;
    private byte[] fBranchQualifier;
    private int fId;
    private long fCreationTime;

    static {
        NumberFormat nf = NumberFormat.getIntegerInstance();
        nf.setMaximumIntegerDigits(5);
        nf.setMinimumIntegerDigits(5);
        nf.setGroupingUsed(false);
        NF = nf;
    }

    public TestXid() {
        this(1);
    }

    public TestXid(int branch) {
        synchronized (TestXid.class) {
            fXidCounter++;
            fId = fXidCounter;
        }
        fCreationTime = System.currentTimeMillis();
        String host;
        try {
            InetAddress ia = InetAddress.getLocalHost();
            host = ia.getHostName();
        } catch (UnknownHostException e) {
            host = "localhost";
        }

        fFormatId = 0;
        fGlobalTransactionId = new byte[MAXGTRIDSIZE];
        fBranchQualifier = new byte[MAXBQUALSIZE];

        StringBuilder sb;
        byte[] ba;

        sb = new StringBuilder();
        sb.append(host);
        sb.append(":");
        sb.append(fId);
        sb.append(":");
        sb.append(fCreationTime);
        // System.out.println("global transaction id: " + sb.toString());
        ba = sb.toString().getBytes();

        for (int i = 0; i < MAXGTRIDSIZE; i++) {
            fGlobalTransactionId[i] = (byte) ' ';
        }
        for (int i = 0; i < ba.length; i++) {
            fGlobalTransactionId[i] = ba[i];
        }

        sb = new StringBuilder(NF.format(branch));
        // System.out.println("branch qualifier: " + sb.toString());
        ba = sb.toString().getBytes();
        for (int i = 0; i < MAXBQUALSIZE; i++) {
            fBranchQualifier[i] = (byte) ' ';
        }
        for (int i = 0; i < ba.length; i++) {
            fBranchQualifier[i] = ba[i];
        }
    }

    /**
     * This method is called when executing this application.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) {
        new TestXid();
    }

    public int getFormatId() {
        return fFormatId;
    }

    public byte[] getGlobalTransactionId() {
        return fGlobalTransactionId;
    }

    public byte[] getBranchQualifier() {
        return fBranchQualifier;
    }

}
