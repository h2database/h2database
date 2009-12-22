/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Copyright (c) 2000-2005 Marc Alexander Lehmann <schmorp@schmorp.de>
 * Copyright (c) 2005 Oren J. Maurice <oymaurice@hazorea.org.il>
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   1.  Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *
 *   2.  Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *   3.  The name of the author may not be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ''AS IS'' AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO
 * EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package org.h2.compress;

import java.sql.SQLException;

/**
 * This class implements the LZF lossless data compression algorithm.
 * LZF is a Lempel-Ziv variant with byte-aligned output, and optimized for speed.
 *
 * <h2>Safety/Use Notes:</h2>
 * <ul><li> Each instance should be used by a single thread only,
 *   due to cached hashtable</li>
 *     <li> May run into problems when data buffers approach Integer.MAX_VALUE
 *       (or, say, 2^31)</li>
 *     <li> For performance reasons, safety checks on expansion omitted</li>
 *     <li> Invalid compressed data can cause ArrayIndexOutOfBoundsException</li>
 * </ul>
 * <p />
 * <h2>LZF compressed format:</h2>
 * <ul><li>2 modes: literal run, or back-reference to previous data
 *     <ul><li>Literal run: directly copy bytes from input to output</li>
 *         <li>Back-reference: copy previous data to output stream,
 *           with specified offset from location and length</li>
 *     </ul>
 * </li>
 * <li>Back-references are assumed to be at least 3 bytes,
 *   otherwise there is no gain from using a back-reference.</li>
 * </ul>
 * <h2>Binary format:</h2>
 * <ul><li>First byte -- control byte:
 *    <ul><li>highest 3 bits are back-reference length, or 0 if literal run</li>
 *     <li>lowest 5 bits are either literal run length or
 *       part of offset for back-reference</li>
 *    </ul></li>
 * <li>If literal run:
 *    <ul><li> next bytes are data to copy directly into output</li></ul>
 * </li>
 * <li>If back reference:
 *    <ul><li>If and only if back reference length is 7 (top 3 bits set),
 *      add next byte to back reference length as unsigned byte</li>
 *     <li>In either case, add next byte to offset location
 *       with lowest 5 bits of control byte</li>
 *    </ul></li>
 * </ul>
 */
public final class CompressLZF implements Compressor {

    /** Number of entries for main hash table
     * <br />Size is a trade-off between hash collisions (reduced compression)
     * and speed (amount that fits in CPU cache) */
    private static final int HASH_SIZE = 1 << 14;

    /** 32: maximum number of literals in a chunk */
    private static final int MAX_LITERAL = 1 << 5;

    /** 8192, maximum offset allowed for a back-reference */
    private static final int MAX_OFF = 1 << 13;

    /** Maximum back-reference length
     * == 256 (full byte) + 8 (top 3 bits of byte) + 1 = 264 */
    private static final int MAX_REF = (1 << 8) + (1 << 3);

    /** Hash table for matching byte sequences -- reused for performance */
    private int[] cachedHashTable;

    public void setOptions(String options) throws SQLException {
        // nothing to do
    }

    /**
     * Return byte with lower 2 bytes being byte at index, then index+1
     */
    private static int first(byte[] in, int inPos) {
        return (in[inPos] << 8) | (in[inPos + 1] & 255);
    }

    /**
     * Shift v 1 byte left, add value at index inPos+2
     */
    private static int next(int v, byte[] in, int inPos) {
        return (v << 8) | (in[inPos + 2] & 255);
    }

    /** Compute address in hash table */
    private static int hash(int h) {
        return ((h * 2777) >> 9) & (HASH_SIZE - 1);
    }

    /**
     * Compress from one buffer to another
     * @param in Input buffer
     * @param inLen Length of bytes to compress from input buffer
     * @param out Output buffer
     * @param outPos Starting position in out buffer
     * @return Number of bytes written to output buffer
     */
    public int compress(byte[] in, int inLen, byte[] out, int outPos) {
        int inPos = 0;
        if (cachedHashTable == null) {
            cachedHashTable = new int[HASH_SIZE];
        }
        int[] hashTab = cachedHashTable;
        int literals = 0;
        outPos++;
        int future = first(in, 0);
        while (inPos < inLen - 4) {
            byte p2 = in[inPos + 2];
            // next
            future = (future << 8) + (p2 & 255);
            int off = hash(future);
            int ref = hashTab[off];
            hashTab[off] = inPos;
            if (ref < inPos
                        && ref > 0
                        && (off = inPos - ref - 1) < MAX_OFF
                        && in[ref + 2] == p2
                        && in[ref + 1] == (byte) (future >> 8)
                        && in[ref] == (byte) (future >> 16)) {
                // match
                int maxLen = inLen - inPos - 2;
                if (maxLen > MAX_REF) {
                    maxLen = MAX_REF;
                }
                if (literals == 0) {
                    // back-to-back back-reference, so no control byte for literal run
                    outPos--;
                } else {
                    // set control byte at start of literal run
                    // to store number of literals
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                }
                int len = 3;
                while (len < maxLen && in[ref + len] == in[inPos + len]) {
                    len++;
                }
                len -= 2;
                if (len < 7) {
                    out[outPos++] = (byte) ((off >> 8) + (len << 5));
                } else {
                    out[outPos++] = (byte) ((off >> 8) + (7 << 5));
                    out[outPos++] = (byte) (len - 7);
                }
                out[outPos++] = (byte) off;
                // move one byte forward to allow for control byte on next literal run
                outPos++;
                inPos += len;
                // rebuild the future, and store last couple bytes to hashtable
                // storing hashes of last bytes in back-reference improves compression ratio
                // and only reduces speed *slightly*
                future = first(in, inPos);
                future = next(future, in, inPos);
                hashTab[hash(future)] = inPos++;
                future = next(future, in, inPos);
                hashTab[hash(future)] = inPos++;
            } else {
                // copy byte from input to output as part of literal
                out[outPos++] = in[inPos++];
                literals++;
                // end of this literal chunk, write length to control byte and start new chunk
                if (literals == MAX_LITERAL) {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                    // move ahead one byte to allow for control byte containing literal length
                    outPos++;
                }
            }
        }
        // writes out remaining few bytes as literals
        while (inPos < inLen) {
            out[outPos++] = in[inPos++];
            literals++;
            if (literals == MAX_LITERAL) {
                out[outPos - literals - 1] = (byte) (literals - 1);
                literals = 0;
                outPos++;
            }
        }
        // writes final literal run length to control byte
        out[outPos - literals - 1] = (byte) (literals - 1);
        if (literals == 0) {
            outPos--;
        }
        return outPos;
    }

    /**
     * Expand compressed data from one buffer to another
     * @param in Compressed data buffer
     * @param inPos Index of first byte in input data
     * @param inLen Number of compressed input bytes
     * @param out Output buffer for decompressed data
     * @param outPos Index for start of decompressed data
     * @param outLen Size of decompressed data
     */
    public void expand(byte[] in, int inPos, int inLen, byte[] out, int outPos, int outLen) {
        do {
            int ctrl = in[inPos++] & 255;
            // literal run of length = ctrl + 1,
            //  directly copy to output and move forward this many bytes
            if (ctrl < MAX_LITERAL) {
                ctrl++;
                System.arraycopy(in, inPos, out, outPos, ctrl);
                outPos += ctrl;
                inPos += ctrl;
            } else {
                // back reference
                // highest 3 bits are match length
                int len = ctrl >> 5;
                // if length is maxed add in next byte to length
                if (len == 7) {
                    len += in[inPos++] & 255;
                }
                // minimum back-reference is 3 bytes, so 2 was subtracted before storing size
                len += 2;

                // control is now offset amount for back-reference...
                // the logical AND operation removes the length bits
                ctrl = -((ctrl & 0x1f) << 8) - 1;

                // next byte augments/increases offset
                ctrl -= in[inPos++] & 255;

                // quickly copy back-reference bytes from location in output to current position
                for (int i = 0; i < len; i++) {
                    out[outPos + i] = out[outPos + ctrl + i];
                }
                outPos += len;
            }
        } while (outPos < outLen);
    }

    public int getAlgorithm() {
        return Compressor.LZF;
    }
}