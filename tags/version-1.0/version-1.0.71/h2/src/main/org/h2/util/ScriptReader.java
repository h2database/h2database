/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;

import org.h2.message.Message;

/**
 * This class can split SQL scripts to single SQL statements.
 * Each SQL statement ends with the character ';', however it is ignored
 * in comments and quotes.
 */
public class ScriptReader {
    private Reader reader;
    private boolean end;
    private boolean insideRemark;
    private boolean blockRemark;
    private boolean skipRemarks;

    public ScriptReader(Reader reader) {
        this.reader = reader;
    }

    public void close() throws SQLException {
        try {
            reader.close();
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    private int read() throws SQLException {
        try {
            return reader.read();
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    public String readStatement() throws SQLException {
        if (end) {
            return null;
        }
        StringBuffer buff = new StringBuffer(200);
        int c = read();
        while (true) {
            if (c < 0) {
                end = true;
                return buff.length() == 0 ? null : buff.toString();
            } else if (c == ';') {
                break;
            }
            switch (c) {
            case '\'':
                buff.append((char) c);
                while (true) {
                    c = read();
                    if (c < 0) {
                        break;
                    }
                    buff.append((char) c);
                    if (c == '\'') {
                        break;
                    }
                }
                c = read();
                break;
            case '"':
                buff.append((char) c);
                while (true) {
                    c = read();
                    if (c < 0) {
                        break;
                    }
                    buff.append((char) c);
                    if (c == '\"') {
                        break;
                    }
                }
                c = read();
                break;
            case '/': {
                int last = c;
                c = read();
                if (c == '*') {
                    // block comment
                    insideRemark = true;
                    blockRemark = true;
                    if (!skipRemarks) {
                        buff.append((char) last);
                        buff.append((char) c);
                    }
                    while (true) {
                        c = read();
                        if (c < 0) {
                            break;
                        }
                        if (!skipRemarks) {
                            buff.append((char) c);
                        }
                        if (c == '*') {
                            c = read();
                            if (c < 0) {
                                break;
                            }
                            if (!skipRemarks) {
                                buff.append((char) c);
                            }
                            if (c == '/') {
                                insideRemark = false;
                                break;
                            }
                        }
                    }
                    c = read();
                } else if (c == '/') {
                    // single line comment
                    insideRemark = true;
                    blockRemark = false;
                    if (!skipRemarks) {
                        buff.append((char) last);
                        buff.append((char) c);
                    }
                    while (true) {
                        c = read();
                        if (c < 0) {
                            break;
                        }
                        if (!skipRemarks) {
                            buff.append((char) c);
                        }
                        if (c == '\r' || c == '\n') {
                            insideRemark = false;
                            break;
                        }
                    }
                    c = read();
                } else {
                    buff.append((char) last);
                }
                break;
            }
            case '-': {
                int last = c;
                c = read();
                if (c == '-') {
                    // single line comment
                    insideRemark = true;
                    blockRemark = false;
                    if (!skipRemarks) {
                        buff.append((char) last);
                        buff.append((char) c);
                    }
                    while (true) {
                        c = read();
                        if (c < 0) {
                            break;
                        }
                        if (!skipRemarks) {
                            buff.append((char) c);
                        }
                        if (c == '\r' || c == '\n') {
                            insideRemark = false;
                            break;
                        }
                    }
                    c = read();
                } else {
                    buff.append((char) last);
                }
                break;
            }
            default:
                buff.append((char) c);
                c = read();
            }
        }
        return buff.toString();
    }

    public boolean isInsideRemark() {
        return insideRemark;
    }

    public boolean isBlockRemark() {
        return blockRemark;
    }

    public void setSkipRemarks(boolean skipRemarks) {
        this.skipRemarks = skipRemarks;
    }
}
