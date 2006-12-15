/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.h2.engine.Constants;
import org.h2.message.Message;

/**
 * @author Thomas
 */

public class StringUtils {

    // TODO hack for gcj
    //#GCJHACK private static final Class[] gcjClasses =  {
    //#GCJHACK gnu.gcj.convert.Input_ASCII.class,
    //#GCJHACK gnu.gcj.convert.Input_UTF8.class,
    //#GCJHACK gnu.gcj.convert.Input_8859_1.class,
    //#GCJHACK gnu.gcj.convert.Output_ASCII.class,
    //#GCJHACK gnu.gcj.convert.Output_UTF8.class,
    //#GCJHACK gnu.gcj.convert.UnicodeToBytes.class,
    //#GCJHACK gnu.gcj.convert.BytesToUnicode.class,
    //#GCJHACK gnu.java.locale.Calendar.class,
    //#GCJHACK gnu.java.locale.LocaleInformation.class,
    //#GCJHACK gnu.java.locale.LocaleInformation_de.class,
    //#GCJHACK java.util.GregorianCalendar.class,
    //#GCJHACK };

    public static boolean equals(String a, String b) {
        if(a==null) {
            return b==null;
        }
        return a.equals(b);
    }
    
    public static String toUpperEnglish(String s) {
        return s.toUpperCase(Locale.ENGLISH);
    }
    
    public static String toLowerEnglish(String s) {
        return s.toLowerCase(Locale.ENGLISH);
    }    

    public static String getDefaultCharset() {
        return System.getProperty("file.encoding");
    }

    public static String quoteStringSQL(String s) {
        StringBuffer buff = new StringBuffer(s.length()+2);
        buff.append('\'');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'') {
                buff.append(c);
            } else if(c < ' ' || c > 127) {
                // need to start from the beginning because maybe there was a \ that was not quoted
                return "STRINGDECODE(" + quoteStringSQL(javaEncode(s)) + ")";
            }
            buff.append(c);
        }
        buff.append('\'');
        return buff.toString();
    }

    public static String javaEncode(String s) {
        StringBuffer buff = new StringBuffer(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
//            case '\b':
//                // BS backspace
//                // not supported in properties files
//                buff.append("\\b");
//                break;
            case '\t':
                // HT horizontal tab
                buff.append("\\t");
                break;
            case '\n':
                // LF linefeed
                buff.append("\\n");
                break;
            case '\f':
                // FF form feed
                buff.append("\\f");
                break;
            case '\r':
                // CR carriage return
                buff.append("\\r");
                break;
            case '"':
                // double quote
                buff.append("\\\"");
                break;
            case '\\':
                // backslash
                buff.append("\\\\");
                break;
            default:
                int ch = (c & 0xffff);
                if (ch >= ' ' && (ch < 0x80)) {
                    buff.append(c);
                // not supported in properties files
                // } else if(ch < 0xff) {
                // buff.append("\\");
                // // make sure it's three characters (0x200 is octal 1000)
                // buff.append(Integer.toOctalString(0x200 | ch).substring(1));
                } else {
                    buff.append("\\u");
                    // make sure it's four characters
                    buff.append(Integer.toHexString(0x10000 | ch).substring(1));
                }
            }
        }
        return buff.toString();
    }

    public static String addAsterix(String s, int index) {
        if (s != null && index < s.length()) {
            s = s.substring(0, index) + "[*]" + s.substring(index);
        }
        return s;
    }

    private static SQLException getFormatException(String s, int i) {
        return Message.getSQLException(Message.STRING_FORMAT_ERROR_1, addAsterix(s, i));
    }

    public static String javaDecode(String s) throws SQLException {
        StringBuffer buff = new StringBuffer(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if(c == '"') {
                break;
            } else if(c=='\\') {
                if(i >= s.length()) {
                    throw getFormatException(s, s.length()-1);
                }
                c = s.charAt(++i);
                switch(c) {
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
                        c = (char)(Integer.parseInt(s.substring(i+1, i+5), 16));
                    } catch(NumberFormatException e) {
                        throw getFormatException(s, i);
                    }
                    i += 4;
                    buff.append(c);
                    break;
                }
                default:
                    if(c >= '0' && c <= '9') {
                        try {
                            c = (char)(Integer.parseInt(s.substring(i, i+3), 8));
                        } catch(NumberFormatException e) {
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

    public static String quoteJavaString(String s) {
        if(s==null) {
            return "null";
        } else {
            return "\"" + javaEncode(s) + "\"";
        }
    }

    public static byte[] utf8Encode(String s) throws SQLException {
        try {
            // TODO UTF8: String.getBytes("UTF-8") only returns 1 byte for 0xd800-0xdfff
            return s.getBytes(Constants.UTF8);
        } catch (UnsupportedEncodingException e) {
            throw Message.convert(e);
        }
    }

    public static String utf8Decode(byte[] utf8) {
        try {
            return new String(utf8, Constants.UTF8);
        } catch (UnsupportedEncodingException e) {
            throw Message.convertToInternal(e);
        }
    }

    public static String utf8Decode(byte[] bytes, int offset, int length) {
        try {
            return new String(bytes, offset, length, Constants.UTF8);
        } catch (UnsupportedEncodingException e) {
            throw Message.convertToInternal(e);
        }
    }

    public static String quoteJavaStringArray(String[] array) {
        if(array == null) {
            return "null";
        }
        StringBuffer buff = new StringBuffer();
        buff.append("new String[]{");
        for(int i=0; i<array.length; i++) {
            if(i>0) {
                buff.append(", ");
            }
            buff.append(quoteJavaString(array[i]));
        }
        buff.append("}");
        return buff.toString();
    }

    public static String quoteJavaIntArray(int[] array) {
        if(array == null) {
            return "null";
        }
        StringBuffer buff = new StringBuffer(2*array.length);
        buff.append("new int[]{");
        for(int i=0; i<array.length; i++) {
            if(i>0) {
                buff.append(", ");
            }
            buff.append(array[i]);
        }
        buff.append("}");
        return buff.toString();
    }

    public static String enclose(String s) {
        if(s.startsWith("(")) {
            return s;
        } else {
            return "(" + s + ")";
        }
    }

    public static String unEnclose(String s) {
        if(s.startsWith("(") && s.endsWith(")")) {
            return s.substring(1, s.length()-1);
        } else {
            return s;
        }
    }

    public static String urlEncode(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return s;
        }
//        byte[] utf = utf8Encode(s);
//        StringBuffer buff = new StringBuffer(utf.length);
//        for(int i=0; i<utf.length; i++) {
//
//            buff.append()
//        }
    }

    public static String urlDecode(String encoded) throws SQLException {
        byte[] buff = new byte[encoded.length()];
        int j=0;
        for(int i=0; i<encoded.length(); i++) {
            char ch = encoded.charAt(i);
            if(ch=='+') {
                buff[j++] = ' ';
            } else if(ch=='%') {
                buff[j++] = (byte)Integer.parseInt(encoded.substring(i+1,i+3),16);
                i+=2;
            } else {
                if(Constants.CHECK && (ch > 127 || ch < ' ')) {
                    throw new IllegalArgumentException("unexpected char " + (int)ch + " decoding " + encoded);
                }
                buff[j++] = (byte)ch;
            }
        }
        String s = utf8Decode(buff, 0, j);
        return s;
    }

    public static String[] arraySplit(String s, char separatorChar, boolean trim) {
        if(s==null) {
            return null;
        }
        if(s.length()==0) {
            return new String[0];
        }
        ArrayList list = new ArrayList();
        StringBuffer buff=new StringBuffer(s.length());
        for(int i=0;i<s.length();i++) {
            char c=s.charAt(i);
            if(c==separatorChar) {
                String e = buff.toString();
                list.add(trim ? e.trim() : e);
                buff.setLength(0);
            } else if(c=='\\' && i<s.length()-1) {
                buff.append(s.charAt(++i));
            } else {
                buff.append(c);
            }
        }
        String e = buff.toString();
        list.add(trim ? e.trim() : e);
        String[] array = new String[list.size()];
        list.toArray(array);
        return array;
    }

    public static String arrayCombine(String[] list, char separatorChar) {
        StringBuffer buff=new StringBuffer();
        for(int i=0;i<list.length;i++) {
            if(i>0) {
                buff.append(separatorChar);
            }
            String s=list[i];
            if(s==null) {
                s = "";
            }
            for(int j=0;j<s.length();j++) {
                char c=s.charAt(j);
                if(c=='\\' || c==separatorChar) {
                    buff.append('\\');
                }
                buff.append(c);
            }
        }
        return buff.toString();
    }

    /**
     * Formats a date using a format string
     */
    public static String formatDateTime(Date date, String format, String locale, String timezone) throws SQLException {
        SimpleDateFormat sdf = getDateFormat(format, locale, timezone);
        return sdf.format(date);
    }

    /**
     * Parses a date using a format string
     */
    public static Date parseDateTime(String date, String format, String locale, String timezone) throws SQLException {
        SimpleDateFormat sdf = getDateFormat(format, locale, timezone);
        try {
            return sdf.parse(date);
        } catch(ParseException e) {
            throw Message.getSQLException(Message.PARSE_ERROR_1, date);
        }
    }

    private static SimpleDateFormat getDateFormat(String format, String locale, String timezone) throws SQLException {
        try {
            SimpleDateFormat df;
            if(locale == null) {
                df = new SimpleDateFormat(format);
            } else {
                Locale l = new Locale(locale);
                df = new SimpleDateFormat(format, l);
            }
            if(timezone != null) {
                df.setTimeZone(TimeZone.getTimeZone(timezone));
            }
            return df;
        } catch(Exception e) {
            throw Message.getSQLException(Message.PARSE_ERROR_1, format + "/" + locale + "/" + timezone);
        }
    }

    /**
     * Creates an XML attribute of the form name="value".
     * A single space is prepended to the name,
     * so that multiple attributes can be concatenated.
     * @param name
     * @param value
     * @return the attribute
     */
    public static String xmlAttr(String name, String value) {
        return " " + name + "=\"" + xmlText(value) + "\"";
    }

    /**
     * Create an XML node with optional attributes and content.
     * The data is indented with 4 spaces if it contains a newline character.
     *
     * @param name the element name
     * @param attributes the attributes (may be null)
     * @param content the content (may be null)
     * @return the node
     */
    public static String xmlNode(String name, String attributes, String content) {
        String start = attributes == null ? name : name + attributes;
        if(content == null) {
            return "<" + start + "/>\n";
        } else {
            if(content.indexOf('\n') >= 0) {
                content = "\n" + indent(content);
            }
            return "<" + start + ">" + content + "</" + name + ">\n";
        }
    }

    /**
     * Indents a string with 4 spaces.
     * @param s the string
     * @return the indented string
     */
    public static String indent(String s) {
        return indent(s, 4);
    }

    /**
     * Indents a string with spaces.
     * @param s the string
     * @param spaces the number of spaces
     * @return the indented string
     */
    public static String indent(String s, int spaces) {
        StringBuffer buff = new StringBuffer(s.length() + spaces);
        for(int i=0; i < s.length(); ) {
            for(int j=0; j<spaces; j++) {
                buff.append(' ');
            }
            int n = s.indexOf('\n', i);
            n = n < 0 ? s.length() : n+1;
            buff.append(s.substring(i, n));
            i = n;
        }
        if(!s.endsWith("\n")) {
            buff.append('\n');
        }
        return buff.toString();
    }

    /**
     * Escapes a comment.
     * If the data contains '--', it is converted to '- -'.
     * The data is indented with 4 spaces if it contains a newline character.
     *
     * @param data
     * @return <!-- data -->
     */
    public static String xmlComment(String data) {
        int idx = 0;
        while(true) {
            idx = data.indexOf("--", idx);
            if(idx<0) {
                break;
            }
            data = data.substring(0, idx + 1) + " " + data.substring(idx + 1);
        }
        // must have a space at the beginning and at the end,
        // otherwise the data must not contain '-' as the first/last character
        if(data.indexOf('\n') >= 0) {
            return "<!--\n" + indent(data) + "-->\n";
        } else {
            return "<!-- " + data + " -->\n";
        }
    }

    /**
     * Converts the data to a CDATA element.
     * If the data contains ']]>', it is escaped as a text element.
     * @param data
     * @return <![CDATA[data]]>
     */
    public static String xmlCData(String data) {
        if(data.indexOf("]]>") >= 0) {
            return xmlText(data);
        }
        boolean newline = data.endsWith("\n");
        data = "<![CDATA[" + data + "]]>";
        return newline ? data + "\n" : data;
    }

    /**
     * Returns <?xml version="1.0"?>
     * @return <?xml version="1.0"?>
     */
    public static String xmlStartDoc() {
        return "<?xml version=\"1.0\"?>\n";
    }

    /**
     * Escapes an XML text element.
     *
     * @param text
     * @return the escaped text
     */
    public static String xmlText(String text) {
        StringBuffer buff = new StringBuffer(text.length());
        for(int i=0; i<text.length(); i++) {
            char ch = text.charAt(i);
            switch(ch) {
            case '<':
                buff.append("&lt;");
                break;
            case '>':
                buff.append("&gt;");
                break;
            case '&':
                buff.append("&amp;");
                break;
            case '\'':
                buff.append("&apos;");
                break;
            case '\"':
                buff.append("&quot;");
                break;
            case '\r':
            case '\n':
            case '\t':
                buff.append(ch);
                break;
            default:
                if(ch < ' ' || ch > 127) {
                    buff.append("&#x");
                    buff.append(Integer.toHexString(ch));
                    buff.append(';');
                } else {
                    buff.append(ch);
                }
            }
        }
        return buff.toString();
    }

    public static String replaceAll(String s, String before, String after) {
        int index = 0;
        while(true) {
            int next = s.indexOf(before, index);
            if(next < 0) {
                return s;
            }
            s = s.substring(0, next) + after + s.substring(next+before.length());
            index = next + after.length();
        }
    }

    public static String quoteIdentifier(String s) {
        StringBuffer buff = new StringBuffer("\"");
        for(int i=0; i<s.length(); i++) {
            char c = s.charAt(i);
            if(c == '"') {
                buff.append(c);
            }
            buff.append(c);
        }
        return buff.append('\"').toString();
    }

}
