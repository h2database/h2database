/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.message.Message;

/**
 * This class represents a byte buffer that is human readable up to some point.
 * Number are stored in hex format.
 * It is mainly used to debug storage problems.
 */
public class DataPageText extends DataPage {

    public DataPageText(DataHandler handler, byte[] data) {
        super(handler, data);
    }
    
    public void setInt(int pos, int x) {
        for (int i = 7; i >= 0; i--, x >>>= 4) {
            data[i] = (byte) Character.forDigit(x & 0xf, 16);
        }
    }

    public void updateChecksum() {
        if (CHECKSUM) {
            int x = handler.getChecksum(data, 0, pos - 2);
            data[pos - 2] = (byte) ('a' + (((x ^ (x >> 4)) & 0xf)));
        }
    }

    public void check(int len) throws SQLException {
        if (CHECKSUM) {
            int x = handler.getChecksum(data, 0, len - 2);
            if (data[len - 2] == (byte) ('a' + (((x ^ (x >> 4)) & 0xf)))) {
                return;
            }
            handler.handleInvalidChecksum();
        }
    }

    public int getFillerLength() {
        return 2;
    }

    public void writeInt(int x) {
        if (SysProperties.CHECK) {
            checkCapacity(8);
        }
        for (int i = 7; i >= 0; i--, x >>>= 4) {
            data[pos + i] = (byte) Character.forDigit(x & 0xf, 16);
        }
        pos += 8;
    }

    public int readInt() {
        int x = 0;
        if (data[pos] == ' ') {
            pos += 8;
            return 0;
        }
        for (int i = 8, c; i > 0; i--) {
            x <<= 4;
            x |= (c = data[pos++]) >= 'a' ? (c - 'a' + 10) : (c - '0');
        }
        return x;
    }

    public int getIntLen() {
        return 8;
    }

    public int getLongLen(long x) {
        return 16;
    }

    public int getStringLen(String s) {
        int len = 2 + s.length() + 1;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
            case '\b':
            case '\n':
            case '\r':
            case '\t':
            case '\f':
            case '"':
            case '\\':
                len++;
                break;
            default:
                int ch = (c & 0xffff);
                if ((ch >= ' ') && (ch <= 0xff)) {
                    // 0
                } else {
                    len += 5;
                }
            }
        }
        return len;
    }

    public String readString() {
        StringBuffer buff = new StringBuffer(32);
        if (SysProperties.CHECK && data[pos] != '"') {
            throw Message.getInternalError("\" expected");
        }
        pos++;
        while (true) {
            char x = (char) (data[pos++] & 0xff);
            if (x == '"') {
                break;
            } else if (x == '\\') {
                x = (char) data[pos++];
                switch (x) {
                case 't':
                    buff.append('\t');
                    break;
                case 'r':
                    buff.append('\r');
                    break;
                case 'n':
                    buff.append('\n');
                    break;
                case 'b':
                    buff.append('\b');
                    break;
                case 'f':
                    buff.append('\f');
                    break;
                case '"':
                    buff.append('"');
                    break;
                case '\\':
                    buff.append('\\');
                    break;
                case 'u': {
                    x = 0;
                    for (int i = 3, c; i >= 0; i--) {
                        x <<= 4;
                        x |= (c = data[pos++]) >= 'a' ? (c - 'a' + 10) : (c - '0');
                    }
                    buff.append(x);
                    break;
                }
                default:
                    throw Message.getInternalError("unexpected " + x);
                }
            } else {
                buff.append(x);
            }
        }
        pos++;
        return buff.toString();
    }

    public void writeString(String s) {
        checkCapacity(s.length() * 6 + 2);
        data[pos++] = '\"';
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
            case '\t':
                data[pos++] = '\\';
                data[pos++] = 't';
                break;
            case '\r':
                data[pos++] = '\\';
                data[pos++] = 'r';
                break;
            case '\n':
                data[pos++] = '\\';
                data[pos++] = 'n';
                break;
            case '\b':
                data[pos++] = '\\';
                data[pos++] = 'b';
                break;
            case '\f':
                data[pos++] = '\\';
                data[pos++] = 'f';
                break;
            case '"':
                data[pos++] = '\\';
                data[pos++] = '\"';
                break;
            case '\\':
                data[pos++] = '\\';
                data[pos++] = '\\';
                break;
            default:
                int ch = (c & 0xffff);
                if ((ch >= ' ') && (ch <= 0xff)) {
                    data[pos++] = (byte) ch;
                } else {
                    data[pos++] = '\\';
                    data[pos++] = 'u';
                    for (int j = 3; j >= 0; j--, ch >>>= 4) {
                        data[pos + j] = (byte) Character.forDigit(ch & 0xf, 16);
                    }
                    pos += 4;
                }
            }
        }
        data[pos++] = '\"';
        data[pos++] = ' ';
    }

    public void fill(int len) {
        if (pos > len) {
            pos = len;
        }
        checkCapacity(len - pos);
        while (pos < len) {
            data[pos++] = ' ';
        }
        data[pos - 1] = '\n';
    }
}
