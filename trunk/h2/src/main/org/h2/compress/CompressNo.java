/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.compress;

public class CompressNo implements Compresser {

    public int getAlgorithm() {
        return Compresser.NO;
    }

    public void setOptions(String options) {
    }
    
    public int compress(byte[] in, int inLen, byte[] out, int outPos) {
        System.arraycopy(in, 0, out, outPos, inLen);
        return outPos + inLen;
    }

    public void expand(byte[] in, int inPos, int inLen, byte[] out, int outPos, int outLen) {
        System.arraycopy(in, inPos, out, outPos, outLen);
    }

}
