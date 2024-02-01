/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.security;

import static org.h2.util.Bits.INT_VH_LE;
import static org.h2.util.Bits.LONG_VH_LE;

import java.security.MessageDigest;
import java.util.Arrays;

/**
 * SHA-3 message digest family.
 */
public final class SHA3 extends MessageDigest {

    private static final long[] ROUND_CONSTANTS;

    static {
        long[] rc = new long[24];
        byte l = 1;
        for (int i = 0; i < 24; i++) {
            rc[i] = 0;
            for (int j = 0; j < 7; j++) {
                byte t = l;
                l = (byte) (t < 0 ? t << 1 ^ 0x71 : t << 1);
                if ((t & 1) != 0) {
                    rc[i] ^= 1L << (1 << j) - 1;
                }
            }
        }
        ROUND_CONSTANTS = rc;
    }

    /**
     * Returns a new instance of SHA3-224 message digest.
     *
     * @return SHA3-224 message digest
     */
    public static SHA3 getSha3_224() {
        return new SHA3("SHA3-224", 28);
    }

    /**
     * Returns a new instance of SHA3-256 message digest.
     *
     * @return SHA3-256 message digest
     */
    public static SHA3 getSha3_256() {
        return new SHA3("SHA3-256", 32);
    }

    /**
     * Returns a new instance of SHA3-384 message digest.
     *
     * @return SHA3-384 message digest
     */
    public static SHA3 getSha3_384() {
        return new SHA3("SHA3-384", 48);
    }

    /**
     * Returns a new instance of SHA3-512 message digest.
     *
     * @return SHA3-512 message digest
     */
    public static SHA3 getSha3_512() {
        return new SHA3("SHA3-512", 64);
    }

    private final int digestLength;

    private final int rate;

    private long state00, state01, state02, state03, state04, state05, state06, state07, state08, state09, //
            state10, state11, state12, state13, state14, state15, state16, state17, state18, state19, //
            state20, state21, state22, state23, state24;

    private final byte[] buf;

    private int bufcnt;

    private SHA3(String algorithm, int digestLength) {
        super(algorithm);
        this.digestLength = digestLength;
        buf = new byte[this.rate = 200 - digestLength * 2];
    }

    @Override
    protected byte[] engineDigest() {
        buf[bufcnt] = 0b110;
        Arrays.fill(buf, bufcnt + 1, rate, (byte) 0);
        buf[rate - 1] |= 0x80;
        absorbQueue();
        byte[] r = new byte[digestLength];
        switch (digestLength) {
        case 64:
            LONG_VH_LE.set(r, 56, state07);
            LONG_VH_LE.set(r, 48, state06);
            //$FALL-THROUGH$
        case 48:
            LONG_VH_LE.set(r, 40, state05);
            LONG_VH_LE.set(r, 32, state04);
            //$FALL-THROUGH$
        case 32:
            LONG_VH_LE.set(r, 24, state03);
            break;
        case 28:
            INT_VH_LE.set(r, 24, (int) state03);
        }
        LONG_VH_LE.set(r, 16, state02);
        LONG_VH_LE.set(r, 8, state01);
        LONG_VH_LE.set(r, 0, state00);
        engineReset();
        return r;
    }

    @Override
    protected int engineGetDigestLength() {
        return digestLength;
    }

    @Override
    protected void engineReset() {
        state24 = state23 = state22 = state21 = state20 //
                = state19 = state18 = state17 = state16 = state15 //
                = state14 = state13 = state12 = state11 = state10 //
                = state09 = state08 = state07 = state06 = state05 //
                = state04 = state03 = state02 = state01 = state00 = 0L;
        Arrays.fill(buf, (byte) 0);
        bufcnt = 0;
    }

    @Override
    protected void engineUpdate(byte input) {
        buf[bufcnt++] = input;
        if (bufcnt == rate) {
            absorbQueue();
        }
    }

    @Override
    protected void engineUpdate(byte[] input, int offset, int len) {
        while (len > 0) {
            if (bufcnt == 0 && len >= rate) {
                do {
                    absorb(input, offset);
                    offset += rate;
                    len -= rate;
                } while (len >= rate);
            } else {
                int partialBlock = Math.min(len, rate - bufcnt);
                System.arraycopy(input, offset, buf, bufcnt, partialBlock);
                bufcnt += partialBlock;
                offset += partialBlock;
                len -= partialBlock;
                if (bufcnt == rate) {
                    absorbQueue();
                }
            }
        }
    }

    private void absorbQueue() {
        absorb(buf, 0);
        bufcnt = 0;
    }

    private void absorb(byte[] data, int offset) {
        /*
         * There is no need to copy 25 state* fields into local variables,
         * because so large number of local variables only hurts performance.
         */
        switch (digestLength) {
        case 28:
            state17 ^= (long) LONG_VH_LE.get(data, offset + 136);
            //$FALL-THROUGH$
        case 32:
            state13 ^= (long) LONG_VH_LE.get(data, offset + 104);
            state14 ^= (long) LONG_VH_LE.get(data, offset + 112);
            state15 ^= (long) LONG_VH_LE.get(data, offset + 120);
            state16 ^= (long) LONG_VH_LE.get(data, offset + 128);
            //$FALL-THROUGH$
        case 48:
            state09 ^= (long) LONG_VH_LE.get(data, offset + 72);
            state10 ^= (long) LONG_VH_LE.get(data, offset + 80);
            state11 ^= (long) LONG_VH_LE.get(data, offset + 88);
            state12 ^= (long) LONG_VH_LE.get(data, offset + 96);
        }
        state00 ^= (long) LONG_VH_LE.get(data, offset);
        state01 ^= (long) LONG_VH_LE.get(data, offset + 8);
        state02 ^= (long) LONG_VH_LE.get(data, offset + 16);
        state03 ^= (long) LONG_VH_LE.get(data, offset + 24);
        state04 ^= (long) LONG_VH_LE.get(data, offset + 32);
        state05 ^= (long) LONG_VH_LE.get(data, offset + 40);
        state06 ^= (long) LONG_VH_LE.get(data, offset + 48);
        state07 ^= (long) LONG_VH_LE.get(data, offset + 56);
        state08 ^= (long) LONG_VH_LE.get(data, offset + 64);
        for (int i = 0; i < 24; i++) {
            long c0 = state00 ^ state05 ^ state10 ^ state15 ^ state20;
            long c1 = state01 ^ state06 ^ state11 ^ state16 ^ state21;
            long c2 = state02 ^ state07 ^ state12 ^ state17 ^ state22;
            long c3 = state03 ^ state08 ^ state13 ^ state18 ^ state23;
            long c4 = state04 ^ state09 ^ state14 ^ state19 ^ state24;
            long dX = (c1 << 1 | c1 >>> 63) ^ c4;
            state00 ^= dX;
            state05 ^= dX;
            state10 ^= dX;
            state15 ^= dX;
            state20 ^= dX;
            dX = (c2 << 1 | c2 >>> 63) ^ c0;
            state01 ^= dX;
            state06 ^= dX;
            state11 ^= dX;
            state16 ^= dX;
            state21 ^= dX;
            dX = (c3 << 1 | c3 >>> 63) ^ c1;
            state02 ^= dX;
            state07 ^= dX;
            state12 ^= dX;
            state17 ^= dX;
            state22 ^= dX;
            dX = (c4 << 1 | c4 >>> 63) ^ c2;
            state03 ^= dX;
            state08 ^= dX;
            state13 ^= dX;
            state18 ^= dX;
            state23 ^= dX;
            dX = (c0 << 1 | c0 >>> 63) ^ c3;
            state04 ^= dX;
            state09 ^= dX;
            state14 ^= dX;
            state19 ^= dX;
            state24 ^= dX;
            long s00 = state00;
            long s01 = state06 << 44 | state06 >>> 20;
            long s02 = state12 << 43 | state12 >>> 21;
            long s03 = state18 << 21 | state18 >>> 43;
            long s04 = state24 << 14 | state24 >>> 50;
            long s05 = state03 << 28 | state03 >>> 36;
            long s06 = state09 << 20 | state09 >>> 44;
            long s07 = state10 << 3 | state10 >>> 61;
            long s08 = state16 << 45 | state16 >>> 19;
            long s09 = state22 << 61 | state22 >>> 3;
            long s10 = state01 << 1 | state01 >>> 63;
            long s11 = state07 << 6 | state07 >>> 58;
            long s12 = state13 << 25 | state13 >>> 39;
            long s13 = state19 << 8 | state19 >>> 56;
            long s14 = state20 << 18 | state20 >>> 46;
            long s15 = state04 << 27 | state04 >>> 37;
            long s16 = state05 << 36 | state05 >>> 28;
            long s17 = state11 << 10 | state11 >>> 54;
            long s18 = state17 << 15 | state17 >>> 49;
            long s19 = state23 << 56 | state23 >>> 8;
            long s20 = state02 << 62 | state02 >>> 2;
            long s21 = state08 << 55 | state08 >>> 9;
            long s22 = state14 << 39 | state14 >>> 25;
            long s23 = state15 << 41 | state15 >>> 23;
            long s24 = state21 << 2 | state21 >>> 62;
            state00 = s00 ^ ~s01 & s02 ^ ROUND_CONSTANTS[i];
            state01 = s01 ^ ~s02 & s03;
            state02 = s02 ^ ~s03 & s04;
            state03 = s03 ^ ~s04 & s00;
            state04 = s04 ^ ~s00 & s01;
            state05 = s05 ^ ~s06 & s07;
            state06 = s06 ^ ~s07 & s08;
            state07 = s07 ^ ~s08 & s09;
            state08 = s08 ^ ~s09 & s05;
            state09 = s09 ^ ~s05 & s06;
            state10 = s10 ^ ~s11 & s12;
            state11 = s11 ^ ~s12 & s13;
            state12 = s12 ^ ~s13 & s14;
            state13 = s13 ^ ~s14 & s10;
            state14 = s14 ^ ~s10 & s11;
            state15 = s15 ^ ~s16 & s17;
            state16 = s16 ^ ~s17 & s18;
            state17 = s17 ^ ~s18 & s19;
            state18 = s18 ^ ~s19 & s15;
            state19 = s19 ^ ~s15 & s16;
            state20 = s20 ^ ~s21 & s22;
            state21 = s21 ^ ~s22 & s23;
            state22 = s22 ^ ~s23 & s24;
            state23 = s23 ^ ~s24 & s20;
            state24 = s24 ^ ~s20 & s21;
        }
    }

}
