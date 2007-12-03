/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.h2.test.trace;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;

import org.h2.util.StringUtils;

/**
 * Some String manipulations / formatting functions used by this tool.
 */
public class StringTools {

    static String[] arraySplit(String s, char separatorChar) {
        if (s == null) {
            return null;
        }
        if (s.length() == 0) {
            return new String[0];
        }
        ArrayList list = new ArrayList();
        StringBuffer buff = new StringBuffer(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == separatorChar) {
                list.add(buff.toString());
                buff.setLength(0);
            } else if (c == '\\' && i < s.length() - 1) {
                buff.append(s.charAt(++i));
            } else {
                buff.append(c);
            }
        }
        list.add(buff.toString());
        String[] array = new String[list.size()];
        list.toArray(array);
        return array;
    }

    private static String addAsterisk(String s, int index) {
        if (s != null && index < s.length()) {
            s = s.substring(0, index) + "[*]" + s.substring(index);
        }
        return s;
    }

    private static IllegalArgumentException getFormatException(String s, int i) {
        return new IllegalArgumentException("String format error: "
                + addAsterisk(s, i));
    }

    /**
     * Parse a a Java string.
     *
     * @param s the string formatted as a Java string
     * @return the parsed string
     */
    public static String javaDecode(String s) {
        StringBuffer buff = new StringBuffer(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                break;
            } else if (c == '\\') {
                if (i >= s.length()) {
                    throw getFormatException(s, s.length() - 1);
                }
                c = s.charAt(++i);
                switch (c) {
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
                    try {
                        c = (char) (Integer.parseInt(s.substring(i + 1, i + 5),
                                16));
                    } catch (NumberFormatException e) {
                        throw getFormatException(s, i);
                    }
                    i += 4;
                    buff.append(c);
                    break;
                }
                default:
                    if (c >= '0' && c <= '9') {
                        try {
                            c = (char) (Integer.parseInt(s.substring(i, i + 3),
                                    8));
                        } catch (NumberFormatException e) {
                            throw getFormatException(s, i);
                        }
                        i += 2;
                        buff.append(c);
                    } else {
                        throw getFormatException(s, i);
                    }
                }
            } else {
                buff.append(c);
            }
        }
        return buff.toString();
    }

    static String convertBytesToString(byte[] value) {
        StringBuffer buff = new StringBuffer(value.length * 2);
        for (int i = 0; value != null && i < value.length; i++) {
            int c = value[i] & 0xff;
            buff.append(Integer.toHexString((c >> 4) & 0xf));
            buff.append(Integer.toHexString(c & 0xf));
        }
        return buff.toString();
    }

    static byte[] convertStringToBytes(String s) throws IllegalArgumentException, NumberFormatException {
        int len = s.length();
        if (len % 2 == 1) {
            throw new IllegalArgumentException("Hex String with odd number of characters: " + s);
        }
        len /= 2;
        byte[] buff = new byte[len];
        for (int i = 0; i < len; i++) {
            String t = s.substring(i + i, i + i + 2);
            buff[i] = (byte) Integer.parseInt(t, 16);
        }
        return buff;
    }

    static String serializeToString(Object obj) throws IOException {
        byte[] bytes = serialize(obj);
        return convertBytesToString(bytes);
    }

    private static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        return out.toByteArray();
    }

    /**
     * Format an object as Java source code.
     *
     * @param o the object
     * @return the formatted string
     */
    public static String quote(Class clazz, Object value) {
        if (value == null) {
            return null;
        } else if (clazz == String.class) {
            return StringUtils.quoteJavaString(value.toString());
        } else if (clazz == BigDecimal.class) {
            return "new BigDecimal(\"" + value.toString() + "\")";
        } else if (clazz.isArray()) {
            if (clazz == String[].class) {
                return StringUtils.quoteJavaStringArray((String[]) value);
            } else if (clazz == int[].class) {
                return StringUtils.quoteJavaIntArray((int[]) value);
            }
        }
        return value.toString();
    }
    
}
