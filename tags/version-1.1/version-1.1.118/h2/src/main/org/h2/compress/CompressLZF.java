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

/**
 * This class implements the LZF lossless data compression algorithm.
 * LZF is optimized for speed.
 */
public class CompressLZF implements Compressor {

    private static final int HASH_SIZE = 1 << 14;
    private static final int[] EMPTY = new int[HASH_SIZE];
    private static final int MAX_LITERAL = 1 << 5;
    private static final int MAX_OFF = 1 << 13;
    private static final int MAX_REF = (1 << 8) + (1 << 3);

    private int[] cachedHashTable;

    public void setOptions(String options) {
        // nothing to do
    }

    public int getAlgorithm() {
        return Compressor.LZF;
    }

    private int first(byte[] in, int inPos) {
        return (in[inPos] << 8) + (in[inPos + 1] & 255);
    }

    private int next(int v, byte[] in, int inPos) {
        return (v << 8) + (in[inPos + 2] & 255);
    }

    private int hash(int h) {
        // or 57321
        return ((h * 184117) >> 9) & (HASH_SIZE - 1);
    }

    public int compress(byte[] in, int inLen, byte[] out, int outPos) {
        int inPos = 0;
        if (cachedHashTable == null) {
            cachedHashTable = new int[HASH_SIZE];
        } else {
            System.arraycopy(EMPTY, 0, cachedHashTable, 0, HASH_SIZE);
        }
        int[] hashTab = cachedHashTable;
        int literals = 0;
        int hash = first(in, inPos);
        while (true) {
            if (inPos < inLen - 4) {
                hash = next(hash, in, inPos);
                int off = hash(hash);
                int ref = hashTab[off];
                hashTab[off] = inPos;
                off = inPos - ref - 1;
                if (off < MAX_OFF && ref > 0 && in[ref + 2] == in[inPos + 2] && in[ref + 1] == in[inPos + 1] && in[ref] == in[inPos]) {
                    int maxLen = inLen - inPos - 2;
                    maxLen = maxLen > MAX_REF ? MAX_REF : maxLen;
                    int len = 3;
                    while (len < maxLen && in[ref + len] == in[inPos + len]) {
                        len++;
                    }
                    len -= 2;
                    if (literals != 0) {
                        out[outPos++] = (byte) (literals - 1);
                        literals = -literals;
                        do {
                            out[outPos++] = in[inPos + literals++];
                        } while (literals != 0);
                    }
                    if (len < 7) {
                        out[outPos++] = (byte) ((off >> 8) + (len << 5));
                    } else {
                        out[outPos++] = (byte) ((off >> 8) + (7 << 5));
                        out[outPos++] = (byte) (len - 7);
                    }
                    out[outPos++] = (byte) off;
                    inPos += len;
                    hash = first(in, inPos);
                    hash = next(hash, in, inPos);
                    hashTab[hash(hash)] = inPos++;
                    hash = next(hash, in, inPos);
                    hashTab[hash(hash)] = inPos++;
                    continue;
                }
            } else if (inPos == inLen) {
                break;
            }
            inPos++;
            literals++;
            if (literals == MAX_LITERAL) {
                out[outPos++] = (byte) (literals - 1);
                literals = -literals;
                do {
                    out[outPos++] = in[inPos + literals++];
                } while (literals != 0);
            }
        }
        if (literals != 0) {
            out[outPos++] = (byte) (literals - 1);
            literals = -literals;
            do {
                out[outPos++] = in[inPos + literals++];
            } while (literals != 0);
        }
        return outPos;
    }

    public void expand(byte[] in, int inPos, int inLen, byte[] out, int outPos, int outLen) {
        do {
            int ctrl = in[inPos++] & 255;
            if (ctrl < (1 << 5)) {
                // literal run
                ctrl += inPos;
                do {
                    out[outPos++] = in[inPos];
                } while (inPos++ < ctrl);
            } else {
                // back reference
                int len = ctrl >> 5;
                ctrl = -((ctrl & 0x1f) << 8) - 1;
                if (len == 7) {
                    len += in[inPos++] & 255;
                }
                ctrl -= in[inPos++] & 255;
                len += outPos + 2;
                out[outPos] = out[outPos++ + ctrl];
                out[outPos] = out[outPos++ + ctrl];
                while (outPos < len - 8) {
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                }
                while (outPos < len) {
                    out[outPos] = out[outPos++ + ctrl];
                }
            }
        } while (outPos < outLen);
    }
}
