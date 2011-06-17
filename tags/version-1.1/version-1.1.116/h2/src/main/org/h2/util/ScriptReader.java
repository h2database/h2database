/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;

import org.h2.constant.SysProperties;
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

    /**
     * Create a new SQL script reader from the given reader
     *
     * @param reader the reader
     */
    public ScriptReader(Reader reader) {
        this.reader = reader;
    }

    /**
     * Close the underlying reader.
     */
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

    /**
     * Read a statement from the reader. This method returns null if the end has
     * been reached.
     *
     * @return the SQL statement or null
     */
    public String readStatement() throws SQLException {
        if (end) {
            return null;
        }
        StringBuilder buff = new StringBuilder(200);
        int previous = 0;
        int c = read();
        while (true) {
            if (c < 0) {
                end = true;
                return buff.length() == 0 ? null : buff.toString();
            } else if (c == ';') {
                break;
            }
            switch (c) {
            case '$': {
                buff.append((char) c);
                c = read();
                if (c == '$' && SysProperties.DOLLAR_QUOTING && previous <= ' ') {
                    // dollar quoted string
                    buff.append((char) c);
                    while (true) {
                        c = read();
                        if (c < 0) {
                            break;
                        }
                        buff.append((char) c);
                        if (c == '$') {
                            c = read();
                            if (c < 0) {
                                break;
                            }
                            buff.append((char) c);
                            if (c == '$') {
                                break;
                            }
                        }
                    }
                    previous = c;
                    c = read();
                }
                break;
            }
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
                previous = c;
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
                previous = c;
                c = read();
                break;
            case '/': {
                int last = c;
                previous = c;
                c = read();
                if (c == '*') {
                    // block comment
                    insideRemark = true;
                    blockRemark = true;
                    if (!skipRemarks) {
                        buff.append((char) last).append((char) c);
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
                    previous = c;
                    c = read();
                } else if (c == '/') {
                    // single line comment
                    insideRemark = true;
                    blockRemark = false;
                    if (!skipRemarks) {
                        buff.append((char) last).append((char) c);
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
                    previous = c;
                    c = read();
                } else {
                    buff.append((char) last);
                }
                break;
            }
            case '-': {
                previous = c;
                int last = c;
                c = read();
                if (c == '-') {
                    // single line comment
                    insideRemark = true;
                    blockRemark = false;
                    if (!skipRemarks) {
                        buff.append((char) last).append((char) c);
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
                    previous = c;
                    c = read();
                } else {
                    buff.append((char) last);
                }
                break;
            }
            default: {
                buff.append((char) c);
                previous = c;
                c = read();
            }
            }
        }
        return buff.toString();
    }

    /**
     * Check if this is the last statement, and if the single line or block
     * comment is not finished yet.
     *
     * @return true if the current position is inside a remark
     */
    public boolean isInsideRemark() {
        return insideRemark;
    }

    /**
     * If currently inside a remark, this method tells if it is a block comment
     * (true) or single line comment (false)
     *
     * @return true if inside a block comment
     */
    public boolean isBlockRemark() {
        return blockRemark;
    }

    /**
     * If comments should be skipped completely by this reader.
     *
     * @param skipRemarks true if comments should be skipped
     */
    public void setSkipRemarks(boolean skipRemarks) {
        this.skipRemarks = skipRemarks;
    }

}
