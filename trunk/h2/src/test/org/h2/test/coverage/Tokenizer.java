/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.coverage;

import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;

/**
 * Helper class for the java file parser.
 */
public class Tokenizer {
    private StringBuffer buffer;

    private Reader reader;

    private char[] buf = new char[20];
    private int peekc;
    private int line = 1;

    private byte[] charTypes = new byte[256];
    private static final byte WHITESPACE = 1;
    private static final byte ALPHA = 4;
    private static final byte QUOTE = 8;

    private int type = TYPE_NOTHING;
    public static final int TYPE_EOF = -1;
    public static final int TYPE_WORD = -2;
    private static final int TYPE_NOTHING = -3;
    private String value;

    private Tokenizer() {
        wordChars('a', 'z');
        wordChars('A', 'Z');
        wordChars('0', '9');
        wordChars('.', '.');
        wordChars('+', '+');
        wordChars('-', '-');
        wordChars('_', '_');
        wordChars(128 + 32, 255);
        whitespaceChars(0, ' ');
        charTypes['"'] = QUOTE;
        charTypes['\''] = QUOTE;
    }

    public Tokenizer(Reader r) {
        this();
        reader = r;
    }

    public String getString() {
        return value;
    }

    private void wordChars(int low, int hi) {
        while (low <= hi) {
            charTypes[low++] |= ALPHA;
        }
    }

    private void whitespaceChars(int low, int hi) {
        while (low <= hi) {
            charTypes[low++] = WHITESPACE;
        }
    }

    private int read() throws IOException {
        int i = reader.read();
        if (i != -1) {
            append(i);
        }
        return i;
    }

    public void initToken() {
        buffer = new StringBuffer();
    }

    public String getToken() {
        buffer.setLength(buffer.length() - 1);
        return buffer.toString();
    }

    private void append(int i) {
        buffer.append((char) i);
    }

    public int nextToken() throws IOException {
        byte[] ct = charTypes;
        int c;
        value = null;

        if (type == TYPE_NOTHING) {
            c = read();
            if (c >= 0) {
                type = c;
            }
        } else {
            c = peekc;
            if (c < 0) {
                try {
                    c = read();
                    if (c >= 0) {
                        type = c;
                    }
                } catch (EOFException e) {
                    c = -1;
                }
            }
        }

        if (c < 0) {
            return type = TYPE_EOF;
        }
        int ctype = c < 256 ? ct[c] : ALPHA;
        while ((ctype & WHITESPACE) != 0) {
            if (c == '\r') {
                line++;
                c = read();
                if (c == '\n') {
                    c = read();
                }
            } else {
                if (c == '\n') {
                    line++;
                }
                c = read();
            }
            if (c < 0) {
                return type = TYPE_EOF;
            }
            ctype = c < 256 ? ct[c] : ALPHA;
        }
        if ((ctype & ALPHA) != 0) {
            initToken();
            append(c);
            int i = 0;
            do {
                if (i >= buf.length) {
                    char[] nb = new char[buf.length * 2];
                    System.arraycopy(buf, 0, nb, 0, buf.length);
                    buf = nb;
                }
                buf[i++] = (char) c;
                c = read();
                ctype = c < 0 ? WHITESPACE : c < 256 ? ct[c] : ALPHA;
            } while ((ctype & ALPHA) != 0);
            peekc = c;
            value = String.copyValueOf(buf, 0, i);
            return type = TYPE_WORD;
        }
        if ((ctype & QUOTE) != 0) {
            initToken();
            append(c);
            type = c;
            int i = 0;
            // \octal needs a lookahead
            peekc = read();
            while (peekc >= 0 && peekc != type && peekc != '\n'
                    && peekc != '\r') {
                if (peekc == '\\') {
                    c = read();
                    int first = c; // to allow \377, but not \477
                    if (c >= '0' && c <= '7') {
                        c = c - '0';
                        int c2 = read();
                        if ('0' <= c2 && c2 <= '7') {
                            c = (c << 3) + (c2 - '0');
                            c2 = read();
                            if ('0' <= c2 && c2 <= '7' && first <= '3') {
                                c = (c << 3) + (c2 - '0');
                                peekc = read();
                            } else {
                                peekc = c2;
                            }
                        } else {
                            peekc = c2;
                        }
                    } else {
                        switch (c) {
                        case 'b':
                            c = '\b';
                            break;
                        case 'f':
                            c = '\f';
                            break;
                        case 'n':
                            c = '\n';
                            break;
                        case 'r':
                            c = '\r';
                            break;
                        case 't':
                            c = '\t';
                            break;
                        }
                        peekc = read();
                    }
                } else {
                    c = peekc;
                    peekc = read();
                }

                if (i >= buf.length) {
                    char[] nb = new char[buf.length * 2];
                    System.arraycopy(buf, 0, nb, 0, buf.length);
                    buf = nb;
                }
                buf[i++] = (char) c;
            }
            if (peekc == type) {
                // keep \n or \r intact in peekc
                peekc = read();
            }
            value = String.copyValueOf(buf, 0, i);
            return type;
        }
        if (c == '/') {
            c = read();
            if (c == '*') {
                int prevc = 0;
                while ((c = read()) != '/' || prevc != '*') {
                    if (c == '\r') {
                        line++;
                        c = read();
                        if (c == '\n') {
                            c = read();
                        }
                    } else {
                        if (c == '\n') {
                            line++;
                            c = read();
                        }
                    }
                    if (c < 0) {
                        return type = TYPE_EOF;
                    }
                    prevc = c;
                }
                peekc = read();
                return nextToken();
            } else if (c == '/') {
                while ((c = read()) != '\n' && c != '\r' && c >= 0) {
                    // nothing
                }
                peekc = c;
                return nextToken();
            } else {
                peekc = c;
                return type = '/';
            }
        }
        peekc = read();
        return type = c;
    }

    public int getLine() {
        return line;
    }
}

