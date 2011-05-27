/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (license2)
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

/**
 * This class implements a simple XML pull parser.
 * Only a subset of the XML pull parser API is implemented.
 */
public class XMLParser {

    public static final int ERROR = 0;
    public static final int START_ELEMENT = 1;
    public static final int END_ELEMENT = 2;
    public static final int PROCESSING_INSTRUCTION = 3;
    public static final int CHARACTERS = 4;
    public static final int COMMENT = 5;
    public static final int SPACE = 6;
    public static final int START_DOCUMENT = 7;
    public static final int END_DOCUMENT = 8;
    public static final int ENTITY_REFERENCE = 9;
    public static final int ATTRIBUTE = 10;
    public static final int DTD = 11;
    public static final int CDATA = 12;
    public static final int NAMESPACE = 13;
    public static final int NOTATION_DECLARATION = 14;
    public static final int ENTITY_DECLARATION = 15;

    private String xml;
    private int index;
    private int eventType;
    private String currentText;
    private String currentToken;
    private String prefix, localName;
    private String[] attributeValues = new String[3];
    private int currentAttribute;
    private boolean endElement;
    private boolean html;

    public XMLParser(String xml) {
        this.xml = xml;
        eventType = START_DOCUMENT;
    }
    
    public void setHTML(boolean html) {
        this.html = html;
    }

    void addAttributeName(String prefix, String localName) {
        if (attributeValues.length <= currentAttribute) {
            String[] temp = new String[attributeValues.length * 2];
            System.arraycopy(attributeValues, 0, temp, 0, attributeValues.length);
            attributeValues = temp;
        }
        attributeValues[currentAttribute++] = prefix;
        attributeValues[currentAttribute++] = localName;
    }

    void addAttributeValue(String v) {
        attributeValues[currentAttribute++] = v;
    }

    private int readChar() {
        if (index >= xml.length()) {
            return -1;
        }
        return xml.charAt(index++);
    }

    private void back() {
        index--;
    }

    private void error(String expected) {
        throw new Error("expected: " + expected + " got: " + xml.substring(index));
    }

    private void read(String chars) {
        for (int i = 0; i < chars.length(); i++) {
            if (readChar() != chars.charAt(i)) {
                error(chars);
            }
        }
    }

    private void skipSpaces() {
        while (index < xml.length() && xml.charAt(index) <= ' ') {
            index++;
        }
    }

    private void read() {
        currentText = null;
        currentAttribute = 0;
        int tokenStart = index, currentStart = index;
        int ch = readChar();
        if (ch < 0) {
            eventType = END_DOCUMENT;
        } else if (ch == '<') {
            currentStart = index;
            ch = readChar();
            if (ch < 0) {
                eventType = ERROR;
            } else if (ch == '?') {
                eventType = PROCESSING_INSTRUCTION;
                currentStart = index;
                while (true) {
                    ch = readChar();
                    if (ch < 0) {
                        error("?>");
                    }
                    if (ch == '?' && readChar() == '>') {
                        break;
                    }
                }
                if (xml.substring(currentStart).startsWith("xml")) {
                    int back = tokenStart;
                    read();
                    tokenStart = back;
                } else {
                    currentText = xml.substring(currentStart, index - 1);
                }
            } else if (ch == '!') {
                ch = readChar();
                if (ch == '-') {
                    eventType = COMMENT;
                    if (readChar() != '-') {
                        error("-");
                    }
                    currentStart = index;
                    while (true) {
                        ch = readChar();
                        if (ch < 0) {
                            error("-->");
                        }
                        if (ch == '-' && readChar() == '-') {
                            read(">");
                            break;
                        }
                    }
                    currentText = xml.substring(currentStart, index - 1);
                } else if (ch == 'D') {
                    read("OCTYPE");
                    eventType = DTD;
                    while (true) {
                        ch = readChar();
                        if (ch < 0) {
                            break;
                        }
                        if (ch == '>') {
                            break;
                        }
                    }
                } else if (ch == '[') {
                    read("CDATA[");
                    currentStart = index;
                    eventType = CHARACTERS;
                    while (true) {
                        ch = readChar();
                        if (ch < 0) {
                            error("]]>");
                        } else if (ch != ']') {
                            continue;
                        }
                        ch = readChar();
                        if (ch < 0) {
                            error("]]>");
                        } else if (ch == ']') {
                            do {
                                ch = readChar();
                                if (ch < 0) {
                                    error("]]>");
                                }
                            } while (ch == ']');
                            if (ch == '>') {
                                currentText = xml.substring(currentStart, index - 3);
                                break;
                            }
                        }
                    }
                }
            } else if (ch == '/') {
                currentStart = index;
                prefix = null;
                eventType = END_ELEMENT;
                while (true) {
                    ch = readChar();
                    if (ch < 0) {
                        error(">");
                    } else if (ch == ':') {
                        prefix = xml.substring(currentStart, index - 1);
                        currentStart = index + 1;
                    } else if (ch == '>') {
                        localName = xml.substring(currentStart, index - 1);
                        break;
                    } else if (ch <= ' ') {
                        localName = xml.substring(currentStart, index - 1);
                        skipSpaces();
                        read(">");
                        break;
                    }
                }
            } else {
                prefix = null;
                localName = null;
                eventType = START_ELEMENT;
                while (true) {
                    ch = readChar();
                    if (ch < 0) {
                        error(">");
                    } else if (ch == ':') {
                        prefix = xml.substring(currentStart, index - 1);
                        currentStart = index + 1;
                    } else if (ch <= ' ') {
                        localName = xml.substring(currentStart, index - 1);
                        readAttributeValues();
                        ch = readChar();
                    }
                    if (ch == '/') {
                        if (localName == null) {
                            localName = xml.substring(currentStart, index - 1);
                        }
                        read(">");
                        endElement = true;
                        break;
                    } else if (ch == '>') {
                        if (localName == null) {
                            localName = xml.substring(currentStart, index - 1);
                        }
                        break;
                    }
                }
            }
        } else {
            // TODO need to replace &#xx;?
            eventType = CHARACTERS;
            while (true) {
                ch = readChar();
                if (ch < 0) {
                    break;
                } else if (ch == '<') {
                    back();
                    break;
                }
            }
            currentText = xml.substring(currentStart, index);
        }
        currentToken = xml.substring(tokenStart, index);
    }

    private void readAttributeValues() {
        while (true) {
            int start = index;
            int ch = readChar();
            if (ch < 0) {
                error(">");
            } else if (ch <= ' ') {
                continue;
            } else if (ch == '/' || ch == '>') {
                back();
                return;
            }
            int end;
            int localNameStart = start;
            boolean noValue = false;
            while (true) {
                end = index;
                ch = readChar();
                if (ch < 0) {
                    error("=");
                } else if (ch <= ' ') {
                    skipSpaces();
                    ch = readChar();
                    if (ch != '=') {
                        if (html) {
                            back();
                            noValue = true;
                        } else {
                            error("=");
                        }
                    }
                    break;
                } else if (ch == '=') {
                    break;
                } else if (ch == ':') {
                    localNameStart = index;
                } else if (ch == '/' || ch == '>') {
                    if (html) {
                        back();
                        noValue = true;
                        break;
                    } else {
                        error("=");
                    }
                }
            }
            if (localNameStart == start) {
                addAttributeName("", xml.substring(localNameStart, end));
            } else {
                addAttributeName(xml.substring(start, localNameStart - 1), xml.substring(localNameStart, end));
            }
            if (noValue) {
                noValue = false;
            } else {
                skipSpaces();
                ch = readChar();
                if (ch != '\"') {
                    error("\"");
                }
                start = index;
                while (true) {
                    end = index;
                    ch = readChar();
                    if (ch < 0) {
                        error("\"");
                    } else if (ch == '\"') {
                        break;
                    }
                }
            }
            addAttributeValue(xml.substring(start, end));
        }
    }

    public boolean hasNext() {
        return index < xml.length();
    }

    public int next() {
        if (endElement) {
            endElement = false;
            eventType = END_ELEMENT;
            currentToken = "";
        } else {
            read();
        }
        return eventType;
    }

    public int nextTag() {
        while (true) {
            int type = next();
            if (type != COMMENT && type != DTD && type != PROCESSING_INSTRUCTION) {
                return type;
            }
        }
    }

    public int getEventType() {
        return eventType;
    }

    public String getText() {
        return currentText;
    }

    public String getToken() {
        return currentToken;
    }

    public int getAttributeCount() {
        return currentAttribute / 3;
    }

    public String getAttributePrefix(int index) {
        return attributeValues[index * 3];
    }

    public String getAttributeLocalName(int index) {
        return attributeValues[index * 3 + 1];
    }

    public String getAttributeName(int index) {
        String prefix = getAttributePrefix(index);
        String localName = getAttributeLocalName(index);
        return prefix == null || prefix.length() == 0 ? localName : prefix + ":" + localName;
    }

    public String getAttributeValue(int index) {
        return attributeValues[index * 3 + 2];
    }

    public String getAttributeValue(String namespaceURI, String localName) {
        int len = getAttributeCount();
        for (int i = 0; i < len; i++) {
            if (getAttributeLocalName(i).equals(localName)) {
                return getAttributeValue(i);
            }
        }
        return null;
    }

    public String getName() {
        return prefix == null || prefix.length() == 0 ? localName : prefix + ":" + localName;
    }

    public String getLocalName() {
        return localName;
    }

    public String getPrefix() {
        return prefix;
    }

    public boolean isWhiteSpace() {
        return getText().trim().length() == 0;
    }

    public String getRemaining() {
        return xml.substring(index);
    }

    public int getPos() {
        return index;
    }

}
