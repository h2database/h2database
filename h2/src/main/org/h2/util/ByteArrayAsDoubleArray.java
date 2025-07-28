package org.h2.util;

/**
 * A view of a {@code byte[]}, with a getter and setter as if it were a {@code double[]}.
 */
public class ByteArrayAsDoubleArray {

    private final boolean be;

    public ByteArrayAsDoubleArray(boolean be) {
        this.be = be;
    }

    public double get(byte[] array, int offset) {
        if (be) {
            return Double.longBitsToDouble(Bits.LONG_VH_BE.get(array, offset));
        } else  {
            return Double.longBitsToDouble(Bits.LONG_VH_LE.get(array, offset));
        }
    }

    public void set(byte[] array, int offset, double value) {
        if (be) {
            Bits.LONG_VH_BE.set(array, offset, Double.doubleToRawLongBits(value));
        } else {
            Bits.LONG_VH_LE.set(array, offset, Double.doubleToRawLongBits(value));
        }
    }

}
