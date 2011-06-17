/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
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

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.Message;

/**
 * A few String utility functions.
 */
public class StringUtils {

    private StringUtils() {
        // utility class
    }

    /**
     * Check if two strings are equal. Here, null is equal to null.
     *
     * @param a the first value
     * @param b the second value
     * @return true if both are null or both are equal
     */
    public static boolean equals(String a, String b) {
        if (a == null) {
            return b == null;
        }
        return a.equals(b);
    }

    /**
     * Convert a string to uppercase using the English locale.
     *
     * @param s the test to convert
     * @return the uppercase text
     */
    public static String toUpperEnglish(String s) {
        return s.toUpperCase(Locale.ENGLISH);
    }

    /**
     * Convert a string to lowercase using the English locale.
     *
     * @param s the text to convert
     * @return the lowercase text
     */
    public static String toLowerEnglish(String s) {
        return s.toLowerCase(Locale.ENGLISH);
    }

    /**
     * Convert a string to a SQL literal. Null is converted to NULL. The text is
     * enclosed in single quotes. If there are any special characters, the method
     * STRINGDECODE is used.
     *
     * @param s the text to convert.
     * @return the SQL literal
     */
    public static String quoteStringSQL(String s) {
        if (s == null) {
            return "NULL";
        }
        StringBuilder buff = new StringBuilder(s.length() + 2);
        buff.append('\'');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'') {
                buff.append(c);
            } else if (c < ' ' || c > 127) {
                // need to start from the beginning because maybe there was a \
                // that was not quoted
                return "STRINGDECODE(" + quoteStringSQL(javaEncode(s)) + ")";
            }
            buff.append(c);
        }
        buff.append('\'');
        return buff.toString();
    }

    /**
     * Convert a string to the Java literal using the correct escape sequences.
     * The literal is not enclosed in double quotes. The result can be used in
     * properties files or in Java source code.
     *
     * @param s the text to convert
     * @return the Java representation
     */
    public static String javaEncode(String s) {
        StringBuilder buff = new StringBuilder(s.length());
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
                int ch = c & 0xffff;
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

    /**
     * Add an asterisk ('[*]') at the given position. This format is used to
     * show where parsing failed in a statement.
     *
     * @param s the text
     * @param index the position
     * @return the text with asterisk
     */
    public static String addAsterisk(String s, int index) {
        if (s != null && index < s.length()) {
            s = s.substring(0, index) + "[*]" + s.substring(index);
        }
        return s;
    }

    private static SQLException getFormatException(String s, int i) {
        return Message.getSQLException(ErrorCode.STRING_FORMAT_ERROR_1, addAsterisk(s, i));
    }

    /**
     * Decode a text that is encoded as a Java string literal. The Java
     * properties file format and Java source code format is supported.
     *
     * @param s the encoded string
     * @return the string
     */
    public static String javaDecode(String s) throws SQLException {
        StringBuilder buff = new StringBuilder(s.length());
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
                case '#':
                    // for properties files
                    buff.append('#');
                    break;
                case '=':
                    // for properties files
                    buff.append('=');
                    break;
                case ':':
                    // for properties files
                    buff.append(':');
                    break;
                case '"':
                    buff.append('"');
                    break;
                case '\\':
                    buff.append('\\');
                    break;
                case 'u': {
                    try {
                        c = (char) (Integer.parseInt(s.substring(i + 1, i + 5), 16));
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
                            c = (char) (Integer.parseInt(s.substring(i, i + 3), 8));
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

    /**
     * Convert a string to the Java literal and enclose it with double quotes.
     * Null will result in 'null'.
     *
     * @param s the text to convert
     * @return the Java representation
     */
    public static String quoteJavaString(String s) {
        if (s == null) {
            return "null";
        }
        return "\"" + javaEncode(s) + "\"";
    }

    /**
     * Convert the text to UTF-8 format. For the Unicode characters
     * 0xd800-0xdfff only one byte is returned.
     *
     * @param s the text
     * @return the UTF-8 representation
     */
    public static byte[] utf8Encode(String s) {
        try {
            return s.getBytes(Constants.UTF8);
        } catch (UnsupportedEncodingException e) {
            throw Message.convertToInternal(e);
        }
    }

    /**
     * Convert a UTF-8 representation of a text to the text.
     *
     * @param utf8 the UTF-8 representation
     * @return the text
     */
    public static String utf8Decode(byte[] utf8) {
        try {
            return new String(utf8, Constants.UTF8);
        } catch (UnsupportedEncodingException e) {
            throw Message.convertToInternal(e);
        }
    }

    /**
     * Convert a UTF-8 representation of a text to the text using the given
     * offset and length.
     *
     * @param bytes the UTF-8 representation
     * @param offset the offset in the bytes array
     * @param length the number of bytes
     * @return the text
     */
    private static String utf8Decode(byte[] bytes, int offset, int length) {
        try {
            return new String(bytes, offset, length, Constants.UTF8);
        } catch (UnsupportedEncodingException e) {
            throw Message.convertToInternal(e);
        }
    }

    /**
     * Convert a string array to the Java source code that represents this
     * array. Null will be converted to 'null'.
     *
     * @param array the string array
     * @return the Java source code (including new String[]{})
     */
    public static String quoteJavaStringArray(String[] array) {
        if (array == null) {
            return "null";
        }
        StatementBuilder buff = new StatementBuilder("new String[]{");
        for (String a : array) {
            buff.appendExceptFirst(", ");
            buff.append(quoteJavaString(a));
        }
        return buff.append('}').toString();
    }

    /**
     * Convert an int array to the Java source code that represents this array.
     * Null will be converted to 'null'.
     *
     * @param array the int array
     * @return the Java source code (including new int[]{})
     */
    public static String quoteJavaIntArray(int[] array) {
        if (array == null) {
            return "null";
        }
        StatementBuilder buff = new StatementBuilder("new int[]{");
        for (int a : array) {
            buff.appendExceptFirst(", ");
            buff.append(a);
        }
        return buff.append('}').toString();
    }

    /**
     * Enclose a string with '(' and ')' if this is not yet done.
     *
     * @param s the string
     * @return the enclosed string
     */
    public static String enclose(String s) {
        if (s.startsWith("(")) {
            return s;
        }
        return "(" + s + ")";
    }

    /**
     * Remove enclosing '(' and ')' if this text is enclosed.
     *
     * @param s the potentially enclosed string
     * @return the string
     */
    public static String unEnclose(String s) {
        if (s.startsWith("(") && s.endsWith(")")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    /**
     * Encode the string as an URL.
     *
     * @param s the string to encode
     * @return the encoded string
     */
    public static String urlEncode(String s) {
//## Java 1.4 begin ##
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw Message.convertToInternal(e);
        }
//## Java 1.4 end ##
/*## Java 1.3 only begin ##
/*
        return URLEncoder.encode(s);
*/
//## Java 1.4 end ##
//        byte[] utf = utf8Encode(s);
//        StringBuilder buff = new StringBuilder(utf.length);
//        for(int i=0; i<utf.length; i++) {
//
//            buff.append()
//        }
    }

    /**
     * Decode the URL to a string.
     *
     * @param encoded the encoded URL
     * @return the decoded string
     */
    public static String urlDecode(String encoded) {
        byte[] buff = new byte[encoded.length()];
        int j = 0;
        for (int i = 0; i < encoded.length(); i++) {
            char ch = encoded.charAt(i);
            if (ch == '+') {
                buff[j++] = ' ';
            } else if (ch == '%') {
                buff[j++] = (byte) Integer.parseInt(encoded.substring(i + 1, i + 3), 16);
                i += 2;
            } else {
                if (SysProperties.CHECK && (ch > 127 || ch < ' ')) {
                    throw new IllegalArgumentException("unexpected char " + (int) ch + " decoding " + encoded);
                }
                buff[j++] = (byte) ch;
            }
        }
        String s = utf8Decode(buff, 0, j);
        return s;
    }

    /**
     * Split a string into an array of strings using the given separator. A null
     * string will result in a null array, and an empty string in a zero element
     * array.
     *
     * @param s the string to split
     * @param separatorChar the separator character
     * @param trim whether each element should be trimmed
     * @return the array list
     */
    public static String[] arraySplit(String s, char separatorChar, boolean trim) {
        if (s == null) {
            return null;
        }
        if (s.length() == 0) {
            return new String[0];
        }
        ArrayList<String> list = New.arrayList();
        StringBuilder buff = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == separatorChar) {
                String e = buff.toString();
                list.add(trim ? e.trim() : e);
                buff.setLength(0);
            } else if (c == '\\' && i < s.length() - 1) {
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

    /**
     * Combine an array of strings to one array using the given separator
     * character. A backslash and the separator character and escaped using a
     * backslash.
     *
     * @param list the string array
     * @param separatorChar the separator character
     * @return the combined string
     */
    public static String arrayCombine(String[] list, char separatorChar) {
        StatementBuilder buff = new StatementBuilder();
        for (String s : list) {
            buff.appendExceptFirst(String.valueOf(separatorChar));
            if (s == null) {
                s = "";
            }
            for (int j = 0; j < s.length(); j++) {
                char c = s.charAt(j);
                if (c == '\\' || c == separatorChar) {
                    buff.append('\\');
                }
                buff.append(c);
            }
        }
        return buff.toString();
    }

    /**
     * Formats a date using a format string.
     *
     * @param date the date to format
     * @param format the format string
     * @param locale the locale
     * @param timeZone the timezone
     * @return the formatted date
     */
    public static String formatDateTime(Date date, String format, String locale, String timeZone) throws SQLException {
        SimpleDateFormat dateFormat = getDateFormat(format, locale, timeZone);
        synchronized (dateFormat) {
            return dateFormat.format(date);
        }
    }

    /**
     * Parses a date using a format string.
     *
     * @param date the date to parse
     * @param format the parsing format
     * @param locale the locale
     * @param timeZone the timeZone
     * @return the parsed date
     */
    public static Date parseDateTime(String date, String format, String locale, String timeZone) throws SQLException {
        SimpleDateFormat dateFormat = getDateFormat(format, locale, timeZone);
        try {
            synchronized (dateFormat) {
                return dateFormat.parse(date);
            }
        } catch (ParseException e) {
            throw Message.getSQLException(ErrorCode.PARSE_ERROR_1, e, date);
        }
    }

    private static SimpleDateFormat getDateFormat(String format, String locale, String timeZone) throws SQLException {
        try {
            // currently, a new instance is create for each call
            // however, could cache the last few instances
            SimpleDateFormat df;
            if (locale == null) {
                df = new SimpleDateFormat(format);
            } else {
                //## Java 1.4 begin ##
                Locale l = new Locale(locale);
                //## Java 1.4 end ##
                /*## Java 1.3 only begin ##
                Locale l = new Locale(locale, "");
                ## Java 1.3 only end ##*/
                df = new SimpleDateFormat(format, l);
            }
            if (timeZone != null) {
                df.setTimeZone(TimeZone.getTimeZone(timeZone));
            }
            return df;
        } catch (Exception e) {
            throw Message.getSQLException(ErrorCode.PARSE_ERROR_1, e, format + "/" + locale + "/" + timeZone);
        }
    }

    /**
     * Creates an XML attribute of the form name="value".
     * A single space is prepended to the name,
     * so that multiple attributes can be concatenated.
     * @param name the attribute name
     * @param value the attribute value
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
        if (content == null) {
            return "<" + start + "/>\n";
        }
        if (content.indexOf('\n') >= 0) {
            content = "\n" + indent(content);
        }
        return "<" + start + ">" + content + "</" + name + ">\n";
    }

    /**
     * Indents a string with 4 spaces.
     * @param s the string
     * @return the indented string
     */
    private static String indent(String s) {
        return indent(s, 4);
    }

    /**
     * Indents a string with spaces.
     * @param s the string
     * @param spaces the number of spaces
     * @return the indented string
     */
    private static String indent(String s, int spaces) {
        StringBuilder buff = new StringBuilder(s.length() + spaces);
        for (int i = 0; i < s.length();) {
            for (int j = 0; j < spaces; j++) {
                buff.append(' ');
            }
            int n = s.indexOf('\n', i);
            n = n < 0 ? s.length() : n + 1;
            buff.append(s.substring(i, n));
            i = n;
        }
        if (!s.endsWith("\n")) {
            buff.append('\n');
        }
        return buff.toString();
    }

    /**
     * Escapes a comment.
     * If the data contains '--', it is converted to '- -'.
     * The data is indented with 4 spaces if it contains a newline character.
     *
     * @param data the comment text
     * @return <!-- data -->
     */
    public static String xmlComment(String data) {
        int idx = 0;
        while (true) {
            idx = data.indexOf("--", idx);
            if (idx < 0) {
                break;
            }
            data = data.substring(0, idx + 1) + " " + data.substring(idx + 1);
        }
        // must have a space at the beginning and at the end,
        // otherwise the data must not contain '-' as the first/last character
        if (data.indexOf('\n') >= 0) {
            return "<!--\n" + indent(data) + "-->\n";
        }
        return "<!-- " + data + " -->\n";
    }

    /**
     * Converts the data to a CDATA element.
     * If the data contains ']]>', it is escaped as a text element.
     *
     * @param data the text data
     * @return <![CDATA[data]]>
     */
    public static String xmlCData(String data) {
        if (data.indexOf("]]>") >= 0) {
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
     * @param text the text data
     * @return the escaped text
     */
    public static String xmlText(String text) {
        StringBuilder buff = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            switch (ch) {
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
                if (ch < ' ' || ch > 127) {
                    buff.append("&#x").
                        append(Integer.toHexString(ch)).
                        append(';');
                } else {
                    buff.append(ch);
                }
            }
        }
        return buff.toString();
    }

    /**
     * Replace all occurrences of the before string with the after string.
     *
     * @param s the string
     * @param before the old text
     * @param after the new text
     * @return the string with the before string replaced
     */
    public static String replaceAll(String s, String before, String after) {
        StringBuilder buff = new StringBuilder(s.length());
        int index = 0;
        while (true) {
            int next = s.indexOf(before, index);
            if (next < 0) {
                buff.append(s.substring(index));
                break;
            }
            buff.append(s.substring(index, next)).append(after);
            index = next + before.length();
        }
        return buff.toString();
    }

    /**
     * Enclose a string with double quotes. A double quote inside the string is
     * escaped using a double quote.
     *
     * @param s the text
     * @return the double quoted text
     */
    public static String quoteIdentifier(String s) {
        StringBuilder buff = new StringBuilder(s.length() + 2);
        buff.append('\"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                buff.append(c);
            }
            buff.append(c);
        }
        return buff.append('\"').toString();
    }

    /**
     * Check if a String is null or empty (the length is null).
     *
     * @param s the string to check
     * @return true if it is null or empty
     */
    public static boolean isNullOrEmpty(String s) {
        return s == null || s.length() == 0;
    }

    /**
     * In a string, replace block comment marks with /++ .. ++/.
     *
     * @param sql the string
     * @return the resulting string
     */
    public static String quoteRemarkSQL(String sql) {
        while (true) {
            int idx = sql.indexOf("*/");
            if (idx < 0) {
                break;
            }
            sql = sql.substring(0, idx) + "++/" + sql.substring(idx + 2);
        }
        while (true) {
            int idx = sql.indexOf("/*");
            if (idx < 0) {
                break;
            }
            sql = sql.substring(0, idx) + "/++" + sql.substring(idx + 2);
        }
        return sql;
    }

    /**
     * Pad a string. This method is used for the SQL function RPAD and LPAD.
     *
     * @param string the original string
     * @param n the target length
     * @param padding the padding string
     * @param right true if the padding should be appended at the end
     * @return the padded string
     */
    public static String pad(String string, int n, String padding, boolean right) {
        if (n < 0) {
            n = 0;
        }
        if (n < string.length()) {
            return string.substring(0, n);
        } else if (n == string.length()) {
            return string;
        }
        char paddingChar;
        if (padding == null || padding.length() == 0) {
            paddingChar = ' ';
        } else {
            paddingChar = padding.charAt(0);
        }
        StringBuilder buff = new StringBuilder(n);
        n -= string.length();
        if (right) {
            buff.append(string);
        }
        for (int i = 0; i < n; i++) {
            buff.append(paddingChar);
        }
        if (!right) {
            buff.append(string);
        }
        return buff.toString();
    }

    /**
     * Create a new char array and copy all the data. If the size of the byte
     * array is zero, the same array is returned.
     *
     * @param chars the char array (may be null)
     * @return a new char array
     */
    public static char[] cloneCharArray(char[] chars) {
        if (chars == null) {
            return null;
        }
        int len = chars.length;
        if (len == 0) {
            return chars;
        }
        char[] copy = new char[len];
        System.arraycopy(chars, 0, copy, 0, len);
        return copy;
    }

    /**
     * Trim a character from a string.
     *
     * @param s the string
     * @param leading if leading characters should be removed
     * @param trailing if trailing characters should be removed
     * @param sp what to remove (only the first character is used)
     *      or null for a space
     * @return the trimmed string
     */
    public static String trim(String s, boolean leading, boolean trailing, String sp) {
        char space = (sp == null || sp.length() < 1) ? ' ' : sp.charAt(0);
        if (leading) {
            int len = s.length(), i = 0;
            while (i < len && s.charAt(i) == space) {
                i++;
            }
            s = (i == 0) ? s : s.substring(i);
        }
        if (trailing) {
            int endIndex = s.length() - 1;
            int i = endIndex;
            while (i >= 0 && s.charAt(i) == space) {
                i--;
            }
            s = i == endIndex ? s : s.substring(0, i + 1);
        }
        return s;
    }

    /**
     * Check if a string only contains numbers.
     *
     * @param s the string
     * @return true if it only contains digits 0 - 9
     */
    public static boolean isNumber(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

}
