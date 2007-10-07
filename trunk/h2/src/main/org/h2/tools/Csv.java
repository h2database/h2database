/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;

/**
 * A facility to read from and write to CSV (comma separated values) files.
 */
public class Csv implements SimpleRowSource {

    private String charset = StringUtils.getDefaultCharset();
    private int bufferSize = 8 * 1024;
    private String[] columnNames;
    private char fieldSeparatorRead = ',';
    private char commentLineStart = '#';
    private String fieldSeparatorWrite = ",";
    private String rowSeparatorWrite;
    private char fieldDelimiter = '\"';
    private char escapeCharacter = '\"';
    private String fileName;
    private Reader reader;
    private PrintWriter writer;
    private int back;
    private boolean endOfLine, endOfFile;

    /**
     * Get a new object of this class.
     * 
     * @return the new instance
     */
    public static Csv getInstance() {
        return new Csv();
    }

    private int writeResultSet(ResultSet rs) throws SQLException {
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int rows = 0;
            int columnCount = meta.getColumnCount();
            String[] row = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                row[i] = meta.getColumnLabel(i + 1);
            }
            writeRow(row);
            while (rs.next()) {
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rs.getString(i + 1);
                }
                writeRow(row);
                rows++;
            }
            return rows;
        } finally {
            close();
            JdbcUtils.closeSilently(rs);
        }
    }

    /**
     * Writes the result set to a file in the CSV format.
     * 
     * @param writer
     *            the writer
     * @param rs
     *            the result set
     * @return the number of rows written
     * @throws SQLException,
     *             IOException
     */
    public int write(Writer writer, ResultSet rs) throws SQLException, IOException {
        this.writer = new PrintWriter(writer);
        return writeResultSet(rs);
    }

    /**
     * Writes the result set to a file in the CSV format.
     * 
     * @param fileName
     *            the name of the csv file
     * @param rs
     *            the result set
     * @param charset
     *            the charset or null to use UTF-8
     * @return the number of rows written
     * @throws SQLException
     */
    public int write(String fileName, ResultSet rs, String charset) throws SQLException {
        init(fileName, charset);
        try {
            initWrite();
            return writeResultSet(rs);
        } catch (IOException e) {
            throw convertException("IOException writing " + fileName, e);
        }
    }

    /**
     * Writes the result set of a query to a file in the CSV format.
     * 
     * @param conn
     *            the connection
     * @param fileName
     *            the file name
     * @param sql
     *            the query
     * @param charset
     *            the charset or null to use UTF-8
     * @return the number of rows written
     * @throws SQLException
     */
    public int write(Connection conn, String fileName, String sql, String charset) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(sql);
        int rows = write(fileName, rs, charset);
        stat.close();
        return rows;
    }

    /**
     * Reads from the CSV file and returns a result set. The rows in the result
     * set are created on demand, that means the file is kept open until all
     * rows are read or the result set is closed.
     * 
     * @param fileName the file name
     * @param colNames or null if the column names should be read from the CSV file
     * @param charset the charset or null to use UTF-8
     * @return the result set
     * @throws SQLException
     */
    public ResultSet read(String fileName, String[] colNames, String charset) throws SQLException {
        init(fileName, charset);
        try {
            return readResultSet(colNames);
        } catch (IOException e) {
            throw convertException("IOException reading " + fileName, e);
        }
    }

    /**
     * Reads CSV data from a reader and returns a result set. The rows in the
     * result set are created on demand, that means the reader is kept open
     * until all rows are read or the result set is closed.
     * 
     * @param reader the reader
     * @param colNames or null if the column names should be read from the CSV file
     * @return the result set
     * @throws SQLException, IOException
     */
    public ResultSet read(Reader reader, String[] colNames) throws SQLException, IOException {
        init(null, null);
        this.reader = reader;
        return readResultSet(colNames);
    }

    private ResultSet readResultSet(String[] colNames) throws SQLException, IOException {
        this.columnNames = colNames;
        initRead();
        SimpleResultSet result = new SimpleResultSet(this);
        makeColumnNamesUnique();
        for (int i = 0; i < columnNames.length; i++) {
            result.addColumn(columnNames[i], Types.VARCHAR, 255, 0);
        }
        return result;
    }

    private void makeColumnNamesUnique() {
        for (int i = 0; i < columnNames.length; i++) {
            String x = columnNames[i];
            if (x == null || x.length() == 0) {
                x = "C" + (i + 1);
            }
            for (int j = 0; j < i; j++) {
                String y = columnNames[j];
                if (x.equals(y)) {
                    x = x + "1";
                    j = -1;
                }
            }
            columnNames[i] = x;
        }
    }

    private Csv() {
    }

    private void init(String fileName, String charset) {
        this.fileName = fileName;
        if (charset != null) {
            this.charset = charset;
        }
    }

    private void initWrite() throws IOException {
        if (writer == null) {
            try {
                OutputStream out = new FileOutputStream(fileName);
                out = new BufferedOutputStream(out, bufferSize);
                writer = new PrintWriter(new OutputStreamWriter(out, charset));
                // TODO performance: what is faster? one, two, or both?
                // writer = new PrintWriter(new BufferedWriter(new
                // OutputStreamWriter(out, encoding), bufferSize));
            } catch (IOException e) {
                close();
                throw e;
            }
        }
    }

    private void writeRow(String[] values) {
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                if (fieldSeparatorWrite != null) {
                    writer.print(fieldSeparatorWrite);
                }
            }
            String s = values[i];
            if (s != null) {
                if (escapeCharacter != 0) {
                    if (fieldDelimiter != 0) {
                        writer.print(fieldDelimiter);
                    }
                    writer.print(escape(s));
                    if (fieldDelimiter != 0) {
                        writer.print(fieldDelimiter);
                    }
                } else {
                    writer.print(s);
                }
            }
        }
        if (rowSeparatorWrite != null) {
            writer.print(rowSeparatorWrite);
        }
        writer.println();
    }

    private String escape(String data) {
        if (data.indexOf(fieldDelimiter) < 0) {
            if (escapeCharacter == fieldDelimiter || data.indexOf(escapeCharacter) < 0) {
                return data;
            }
        }
        StringBuffer buff = new StringBuffer(data.length());
        for (int i = 0; i < data.length(); i++) {
            char ch = data.charAt(i);
            if (ch == fieldDelimiter || ch == escapeCharacter) {
                buff.append(escapeCharacter);
            }
            buff.append(ch);
        }
        return buff.toString();
    }

    private void initRead() throws IOException {
        if (reader == null) {
            try {
                InputStream in = FileUtils.openFileInputStream(fileName);
                in = new BufferedInputStream(in, bufferSize);
                reader = new InputStreamReader(in, charset);
                // TODO what is faster, 1, 2, 1+2
                // reader = new BufferedReader(new InputStreamReader(in, encoding), bufferSize);
            } catch (IOException e) {
                close();
                throw e;
            }
        }
        if (columnNames == null) {
            readHeader();
        }
    }

    private void readHeader() throws IOException {
        ArrayList list = new ArrayList();
        while (true) {
            String v = readValue();
            if (v == null) {
                if (endOfLine) {
                    if (endOfFile || list.size() > 0) {
                        break;
                    }
                } else {
                    list.add("COLUMN" + list.size());
                }
            } else {
                list.add(v);
            }
        }
        columnNames = new String[list.size()];
        list.toArray(columnNames);
    }

    private void pushBack(int ch) {
        back = ch;
    }

    private int readChar() throws IOException {
        int ch = back;
        if (ch != -1) {
            back = -1;
            return ch;
        } else if (endOfFile) {
            return -1;
        }
        ch = reader.read();
        if (ch < 0) {
            endOfFile = true;
            close();
        }
        return ch;
    }

    private String readValue() throws IOException {
        endOfLine = false;
        String value = null;
        while (true) {
            int ch = readChar();
            if (ch < 0 || ch == '\r' || ch == '\n') {
                endOfLine = true;
                break;
            } else if (ch <= ' ') {
                // ignore spaces
                continue;
            } else if (ch == fieldSeparatorRead) {
                break;
            } else if (ch == commentLineStart) {
                while (true) {
                    ch = readChar();
                    if (ch < 0 || ch == '\r' || ch == '\n') {
                        break;
                    }
                }
                endOfLine = true;
                break;
            } else if (ch == fieldDelimiter) {
                StringBuffer buff = new StringBuffer();
                boolean containsEscape = false;
                while (true) {
                    ch = readChar();
                    if (ch < 0) {
                        return buff.toString();
                    } else if (ch == fieldDelimiter) {
                        ch = readChar();
                        if (ch == fieldDelimiter) {
                            buff.append((char) ch);
                        } else {
                            pushBack(ch);
                            break;
                        }
                    } else if (ch == escapeCharacter) {
                        buff.append((char) ch);
                        ch = readChar();
                        if (ch < 0) {
                            break;
                        }
                        containsEscape = true;
                        buff.append((char) ch);
                    } else {
                        buff.append((char) ch);
                    }
                }
                value = buff.toString();
                if (containsEscape) {
                    value = unEscape(value);
                }
                while (true) {
                    ch = readChar();
                    if (ch < 0) {
                        break;
                    } else if (ch == ' ' || ch == '\t') {
                        // ignore
                    } else if (ch == fieldSeparatorRead) {
                        break;
                    } else if (ch == '\r' || ch == '\n') {
                        pushBack(ch);
                        endOfLine = true;
                        break;
                    } else {
                        pushBack(ch);
                        break;
                    }
                }
                break;
            } else {
                StringBuffer buff = new StringBuffer();
                buff.append((char) ch);
                while (true) {
                    ch = readChar();
                    if (ch == fieldSeparatorRead) {
                        break;
                    } else if (ch == '\r' || ch == '\n') {
                        pushBack(ch);
                        endOfLine = true;
                        break;
                    } else if (ch < 0) {
                        break;
                    }
                    buff.append((char) ch);
                }
                value = buff.toString().trim();
                break;
            }
        }
        return value;
    }

    private String unEscape(String s) {
        StringBuffer buff = new StringBuffer(s.length());
        int start = 0;
        while (true) {
            int idx = s.indexOf(escapeCharacter, start);
            if (idx < 0) {
                break;
            }
            buff.append(s.toCharArray(), start, idx);
            start = idx + 1;
        }
        buff.append(s.substring(start));
        return buff.toString();
    }

    /**
     * INTERNAL
     */
    public Object[] readRow() throws SQLException {
        if (reader == null) {
            return null;
        }
        String[] row = new String[columnNames.length];
        try {
            for (int i = 0;; i++) {
                String v = readValue();
                if (v == null) {
                    if (endOfFile && i == 0) {
                        return null;
                    }
                    if (endOfLine) {
                        if (i == 0) {
                            // empty line
                            i--;
                            continue;
                        }
                        break;
                    }
                }
                if (i < row.length) {
                    row[i] = v;
                }
            }
        } catch (IOException e) {
            throw convertException("IOException reading from " + fileName, e);
        }
        return row;
    }

    private SQLException convertException(String message, Exception e) {
        SQLException s = new SQLException(message, "CSV");
//#ifdef JDK14
        s.initCause(e);
//#endif
        return s;
    }

    /**
     * INTERNAL
     */
    public void close() {
        IOUtils.closeSilently(reader);
        reader = null;
        IOUtils.closeSilently(writer);
        writer = null;
    }
    
    /**
     * INTERNAL
     */
    public void reset() throws SQLException {
        throw new SQLException("Method is not supported", "CSV");
    }

    /**
     * Override the field separator for writing. The default is ",".
     * 
     * @param fieldSeparatorWrite the field separator
     */
    public void setFieldSeparatorWrite(String fieldSeparatorWrite) {
        this.fieldSeparatorWrite = fieldSeparatorWrite;
    }

    /**
     * Get the current field reparator for writing.
     * 
     * @return the field separator
     */
    public String getFieldSeparatorWrite() {
        return fieldSeparatorWrite;
    }

    /**
     * Override the field separator for reading. The default is ','.
     * 
     * @param fieldSeparatorRead the field separator
     */
    public void setFieldSeparatorRead(char fieldSeparatorRead) {
        this.fieldSeparatorRead = fieldSeparatorRead;
    }

    /**
     * Get the current field separator for reading.
     * 
     * @return the field separator
     */
    public char getFieldSeparatorRead() {
        return fieldSeparatorRead;
    }

    /**
     * Get the current row separator for writing.
     * 
     * @return the row separator
     */
    public String getRowSeparatorWrite() {
        return rowSeparatorWrite;
    }

    /**
     * Override the end-of-row marker for writing. The default is null.
     * After writing the end-of-row marker, a line feed is written (\n or \r\n depending on the system settings).
     * 
     * @param rowSeparatorWrite the row separator
     */
    public void setRowSeparatorWrite(String rowSeparatorWrite) {
        this.rowSeparatorWrite = rowSeparatorWrite;
    }

    /**
     * Set the field delimiter. The default is " (a double quote).
     * 
     * @param fieldDelimiter the field delimiter
     */
    public void setFieldDelimiter(char fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    /**
     * Get the current field delimiter.
     * 
     * @return the field delimiter
     */
    public char getFieldDelimiter() {
        return fieldDelimiter;
    }

    /**
     * Set the escape character (used to escape the field delimiter). The default is " (a double quote).
     * 
     * @param escapeCharacter the escape character
     */
    public void setEscapeCharacter(char escapeCharacter) {
        this.escapeCharacter = escapeCharacter;
    }

    /**
     * Get the current escape character.
     * 
     * @return the escape character
     */
    public char getEscapeCharacter() {
        return escapeCharacter;
    }

}
