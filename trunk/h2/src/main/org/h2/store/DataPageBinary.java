/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;

/**
 * @author Thomas
 */
public class DataPageBinary extends DataPage {
//    private final static boolean UTF8 = true;
    
    public DataPageBinary(DataHandler handler, byte[] data) {
        super(handler, data);
    }
    
    public void updateChecksum() {
        if (CHECKSUM) {
            int x = handler.getChecksum(data, 0, pos - 2);
            data[pos - 2] = (byte) x;
        }
    }

    public void check(int len) throws SQLException {
        if (CHECKSUM) {
            int x = handler.getChecksum(data, 0, len - 2);
            if (data[len - 2] == (byte) x) {
                return;
            }
            handler.handleInvalidChecksum();
        }
    }

    public int getFillerLength() {
        return 2;
    }

    public void writeByte(byte x) {
        data[pos++] = x;
    }

    public int readByte() {
        return data[pos++];
    }

    public void writeInt(int x) {
        byte[] buff = data;
        buff[pos++] = (byte) (x >> 24);
        buff[pos++] = (byte) (x >> 16);
        buff[pos++] = (byte) (x >> 8);
        buff[pos++] = (byte) x;
    }

    public void setInt(int pos, int x) {
        byte[] buff = data;
        buff[pos] = (byte) (x >> 24);
        buff[pos + 1] = (byte) (x >> 16);
        buff[pos + 2] = (byte) (x >> 8);
        buff[pos + 3] = (byte) x;
    }

    public int readInt() {
        byte[] buff = data;
        return (buff[pos++] << 24) + ((buff[pos++] & 0xff) << 16) + ((buff[pos++] & 0xff) << 8) + (buff[pos++] & 0xff);
    }

//    private static int getStringLenChar(String s) {
//        return 4 + s.length() * 2;
//    }

//    private String readStringChar() {
//        int len = ((data[pos++] & 0xff) << 24) + ((data[pos++] & 0xff) << 16) + ((data[pos++] & 0xff) << 8) + (data[pos++] & 0xff);
//        char[] chars = new char[len];
//        for(int i=0; i<len; i++) {
//            chars[i] = (char)(((data[pos++] & 0xff) << 8) + (data[pos++] & 0xff));
//        }
//        return new String(chars);
//    }

//    private void writeStringChar(String s) {
//        checkCapacity(s.length()*2+4);
//        int len = s.length();
//        data[pos++] = (byte) ((len >> 24) & 0xff);
//        data[pos++] = (byte) ((len >> 16) & 0xff);
//        data[pos++] = (byte) ((len >> 8) & 0xff);
//        data[pos++] = (byte) (len & 0xff);
//        for(int i=0; i<s.length(); i++) {
//            int c = s.charAt(i);
//            data[pos++] = (byte)(c >> 8);
//            data[pos++] = (byte)c;
//        }
//    }
    
    private static int getStringLenUTF8(String s) {
        int plus = 4, len = s.length();
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (c >= 0x800) {
                plus += 2;
            } else if (c == 0 || c >= 0x80) {
                plus++;
            }
        }
        return len + plus;
    }
    
    private void writeStringUTF8(String s) {
        int len = s.length();
        checkCapacity(len * 3 + 4);
        int p = pos;
        byte[] buff = data;
        buff[p++] = (byte) (len >> 24);
        buff[p++] = (byte) (len >> 16);
        buff[p++] = (byte) (len >> 8);
        buff[p++] = (byte) len;
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            if (c > 0 && c < 0x80) {
                buff[p++] = (byte) c;
            } else if (c >= 0x800) {
                buff[p++] = (byte) (0xe0 | (c >> 12));
                buff[p++] = (byte) (0x80 | ((c >> 6) & 0x3f));
                buff[p++] = (byte) (0x80 | (c & 0x3f));
            } else {
                buff[p++] = (byte) (0xc0 | (c >> 6));
                buff[p++] = (byte) (0x80 | (c & 0x3f));
            }
        }
        pos = p;
    }

    private String readStringUTF8() {
        byte[] buff = data;
        int p = pos;
        int len = ((buff[p++] & 0xff) << 24) + ((buff[p++] & 0xff) << 16) + ((buff[p++] & 0xff) << 8)
                + (buff[p++] & 0xff);
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            int x = buff[p++] & 0xff;
            if (x < 0x80) {
                chars[i] = (char) x;
            } else if (x >= 0xe0) {
                chars[i] = (char) (((x & 0xf) << 12) + ((buff[p++] & 0x3f) << 6) + (buff[p++] & 0x3f));
            } else {
                chars[i] = (char) (((x & 0x1f) << 6) + (buff[p++] & 0x3f));
            }
        }
        pos = p;
        return new String(chars);
    }

    public void writeString(String s) {
//        if(UTF8) {
            writeStringUTF8(s);
//        } else {
//            writeStringChar(s);
//        }
    }

    public String readString() {
//        if(UTF8) {
            return readStringUTF8();
//        }
//        return readStringChar();
    }

    public int getIntLen() {
        return 4;
    }

    public int getLongLen(long x) {
        return 8;
    }

    public int getStringLen(String s) {
//        if(UTF8) {
            return getStringLenUTF8(s);
//        }
//        return getStringLenChar(s);
    }

    public void fill(int len) {
        if (pos > len) {
            pos = len;
        }
        checkCapacity(len - pos);
        pos = len;
    }

}
