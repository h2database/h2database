/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.New;

/**
 * A facility to read from and write to CSV (comma separated values) files.
 *
 * @author Thomas Mueller, Sylvain Cuaz
 */
public class Csv implements SimpleRowSource {

    private String streamCharset = SysProperties.FILE_ENCODING;
    private String[] columnNames;
    private char fieldSeparatorRead = ',';
    private char commentLineStart = '#';
    private String fieldSeparatorWrite = ",";
    private String rowSeparatorWrite;
    private char fieldDelimiter = '\"';
    private char escapeCharacter = '\"';
    private String lineSeparator = SysProperties.LINE_SEPARATOR;
    private String nullString = "";
    private String fileName;
    private Reader input;
    private char[] inputBuffer;
    private int inputBufferPos;
    private int inputBufferStart = -1;
    private int inputBufferEnd;
    private Writer output;
    private boolean endOfLine, endOfFile;

    private Csv() {
        // don't allow construction
    }

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
            output.close();
            return rows;
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        } finally {
            close();
            JdbcUtils.closeSilently(rs);
        }
    }

    /**
     * Writes the result set to a file in the CSV format.
     *
     * @param writer the writer
     * @param rs the result set
     * @return the number of rows written
     * @throws SQLException
     */
    public int write(Writer writer, ResultSet rs) throws SQLException {
        this.output = writer;
        return writeResultSet(rs);
    }

    /**
     * Writes the result set to a file in the CSV format. The result set is read
     * using the following loop:
     *
     * <pre>
     * while (rs.next()) {
     *     writeRow(row);
     * }
     * </pre>
     *
     * @param outputFileName the name of the csv file
     * @param rs the result set - the result set must be positioned before the
     *            first row.
     * @param charset the charset or null to use UTF-8
     * @return the number of rows written
     * @throws SQLException
     */
    public int write(String outputFileName, ResultSet rs, String charset) throws SQLException {
        init(outputFileName, charset);
        try {
            initWrite();
            return writeResultSet(rs);
        } catch (IOException e) {
            throw convertException("IOException writing " + outputFileName, e);
        }
    }

    /**
     * Writes the result set of a query to a file in the CSV format.
     *
     * @param conn the connection
     * @param outputFileName the file name
     * @param sql the query
     * @param charset the charset or null to use UTF-8
     * @return the number of rows written
     * @throws SQLException
     */
    public int write(Connection conn, String outputFileName, String sql, String charset) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(sql);
        int rows = write(outputFileName, rs, charset);
        stat.close();
        return rows;
    }

    /**
     * Reads from the CSV file and returns a result set. The rows in the result
     * set are created on demand, that means the file is kept open until all
     * rows are read or the result set is closed.
     *
     * @param inputFileName the file name
     * @param colNames or null if the column names should be read from the CSV file
     * @param charset the charset or null to use UTF-8
     * @return the result set
     * @throws SQLException
     */
    public ResultSet read(String inputFileName, String[] colNames, String charset) throws SQLException {
        init(inputFileName, charset);
        try {
            return readResultSet(colNames);
        } catch (IOException e) {
            throw convertException("IOException reading " + inputFileName, e);
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
        this.input = reader;
        return readResultSet(colNames);
    }

    private ResultSet readResultSet(String[] colNames) throws SQLException, IOException {
        this.columnNames = colNames;
        initRead();
        SimpleResultSet result = new SimpleResultSet(this);
        makeColumnNamesUnique();
        for (String columnName : columnNames) {
            result.addColumn(columnName, Types.VARCHAR, Integer.MAX_VALUE, 0);
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
                    x += "1";
                    j = -1;
                }
            }
            columnNames[i] = x;
        }
    }

    private void init(String newFileName, String charset) {
        this.fileName = newFileName;
        if (charset != null) {
            this.streamCharset = charset;
        }
    }

    private void initWrite() throws IOException {
        if (output == null) {
            try {
                OutputStream out = FileUtils.openFileOutputStream(fileName, false);
                out = new BufferedOutputStream(out, Constants.IO_BUFFER_SIZE);
                output = new BufferedWriter(new OutputStreamWriter(out, streamCharset));
            } catch (SQLException e) {
                close();
                throw Message.convertToIOException(e);
            }
        }
    }

    private void writeRow(String[] values) throws IOException {
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                if (fieldSeparatorWrite != null) {
                    output.write(fieldSeparatorWrite);
                }
            }
            String s = values[i];
            if (s != null) {
                if (escapeCharacter != 0) {
                    if (fieldDelimiter != 0) {
                        output.write(fieldDelimiter);
                    }
                    output.write(escape(s));
                    if (fieldDelimiter != 0) {
                        output.write(fieldDelimiter);
                    }
                } else {
                    output.write(s);
                }
            } else if (nullString != null && nullString.length() > 0) {
                output.write(nullString);
            }
        }
        if (rowSeparatorWrite != null) {
            output.write(rowSeparatorWrite);
        }
        output.write(lineSeparator);
    }

    private String escape(String data) {
        if (data.indexOf(fieldDelimiter) < 0) {
            if (escapeCharacter == fieldDelimiter || data.indexOf(escapeCharacter) < 0) {
                return data;
            }
        }
        StringBuilder buff = new StringBuilder(data.length());
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
        if (input == null) {
            try {
                InputStream in = FileUtils.openFileInputStream(fileName);
                in = new BufferedInputStream(in, Constants.IO_BUFFER_SIZE);
                input = new InputStreamReader(in, streamCharset);
            } catch (IOException e) {
                close();
                throw e;
            }
        }
        inputBuffer = new char[Constants.IO_BUFFER_SIZE * 2];
        if (columnNames == null) {
            readHeader();
        }
    }

    private void readHeader() throws IOException {
        ArrayList<String> list = New.arrayList();
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
                if (endOfLine) {
                    break;
                }
            }
        }
        columnNames = new String[list.size()];
        list.toArray(columnNames);
    }

    private void pushBack() {
        inputBufferPos--;
    }

    private int readChar() throws IOException {
        if (inputBufferPos >= inputBufferEnd) {
            return readBuffer();
        }
        return inputBuffer[inputBufferPos++];
    }

    private int readBuffer() throws IOException {
        if (endOfFile) {
            return -1;
        }
        int keep;
        if (inputBufferStart >= 0) {
            keep = inputBufferPos - inputBufferStart;
            if (keep > 0) {
                char[] src = inputBuffer;
                if (keep + Constants.IO_BUFFER_SIZE > src.length) {
                    inputBuffer = new char[src.length * 2];
                }
                System.arraycopy(src, inputBufferStart, inputBuffer, 0, keep);
            }
            inputBufferStart = 0;
        } else {
            keep = 0;
        }
        inputBufferPos = keep;
        int len = input.read(inputBuffer, keep, Constants.IO_BUFFER_SIZE);
        if (len == -1) {
            // ensure bufferPos > bufferEnd
            // even after pushBack
            inputBufferEnd = -1024;
            endOfFile = true;
            // ensure the right number of characters are read
            // in case the input buffer is still used
            inputBufferPos++;
            return -1;
        }
        inputBufferEnd = keep + len;
        return inputBuffer[inputBufferPos++];
    }

    private String readValue() throws IOException {
        endOfLine = false;
        inputBufferStart = inputBufferPos;
        while (true) {
            int ch = readChar();
            if (ch == fieldDelimiter) {
                // delimited value
                boolean containsEscape = false;
                inputBufferStart = inputBufferPos;
                int sep;
                while (true) {
                    ch = readChar();
                    if (ch == fieldDelimiter) {
                        ch = readChar();
                        if (ch != fieldDelimiter) {
                            sep = 2;
                            break;
                        }
                        containsEscape = true;
                    } else if (ch == escapeCharacter) {
                        ch = readChar();
                        if (ch < 0) {
                            sep = 1;
                            break;
                        }
                        containsEscape = true;
                    } else if (ch < 0) {
                        sep = 1;
                        break;
                    }
                }
                String s = new String(inputBuffer, inputBufferStart, inputBufferPos - inputBufferStart - sep);
                if (containsEscape) {
                    s = unEscape(s);
                }
                inputBufferStart = -1;
                while (true) {
                    if (ch == fieldSeparatorRead) {
                        break;
                    } else if (ch == '\n' || ch < 0 || ch == '\r') {
                        endOfLine = true;
                        break;
                    } else if (ch == ' ' || ch == '\t') {
                        // ignore
                    } else {
                        pushBack();
                        break;
                    }
                    ch = readChar();
                }
                return s;
            } else if (ch == '\n' || ch < 0 || ch == '\r') {
                endOfLine = true;
                return null;
            } else if (ch == fieldSeparatorRead) {
                // null
                return null;
            } else if (ch <= ' ') {
                // ignore spaces
                continue;
            } else if (ch == commentLineStart) {
                // comment until end of line
                inputBufferStart = -1;
                while (true) {
                    ch = readChar();
                    if (ch == '\n' || ch < 0 || ch == '\r') {
                        break;
                    }
                }
                endOfLine = true;
                return null;
            } else {
                // un-delimited value
                while (true) {
                    ch = readChar();
                    if (ch == fieldSeparatorRead) {
                        break;
                    } else if (ch == '\n' || ch < 0 || ch == '\r') {
                        endOfLine = true;
                        break;
                    }
                }
                String s = new String(inputBuffer, inputBufferStart, inputBufferPos - inputBufferStart - 1);
                inputBufferStart = -1;
                // check un-delimited value for nullString
                return readNull(s.trim());
            }
        }
    }

    private String readNull(String s) {
        return s.equals(nullString) ? null : s;
    }

    private String unEscape(String s) {
        StringBuilder buff = new StringBuilder(s.length());
        int start = 0;
        char[] chars = null;
        while (true) {
            int idx = s.indexOf(escapeCharacter, start);
            if (idx < 0) {
                idx = s.indexOf(fieldDelimiter, start);
                if (idx < 0) {
                    break;
                }
            }
            if (chars == null) {
                chars = s.toCharArray();
            }
            buff.append(chars, start, idx - start);
            if (idx == s.length() - 1) {
                start = s.length();
                break;
            }
            buff.append(chars[idx + 1]);
            start = idx + 2;
        }
        buff.append(s.substring(start));
        return buff.toString();
    }

    /**
     * INTERNAL
     */
    public Object[] readRow() throws SQLException {
        if (input == null) {
            return null;
        }
        String[] row = new String[columnNames.length];
        try {
            int i = 0;
            while (true) {
                String v = readValue();
                if (v == null) {
                    if (endOfLine) {
                        if (i == 0) {
                            if (endOfFile) {
                                return null;
                            }
                            // empty line
                            continue;
                        }
                        break;
                    }
                }
                if (i < row.length) {
                    row[i++] = v;
                }
                if (endOfLine) {
                    break;
                }
            }
        } catch (IOException e) {
            throw convertException("IOException reading from " + fileName, e);
        }
        return row;
    }

    private SQLException convertException(String message, Exception e) {
        SQLException s = new SQLException(message, "CSV");
        //## Java 1.4 begin ##
        s.initCause(e);
        //## Java 1.4 end ##
        return s;
    }

    /**
     * INTERNAL
     */
    public void close() {
        IOUtils.closeSilently(input);
        input = null;
        IOUtils.closeSilently(output);
        output = null;
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
     * Get the current field separator for writing.
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
     * Override the end-of-row marker for writing. The default is null. After
     * writing the end-of-row marker, a line feed is written (\n or \r\n
     * depending on the system settings).
     *
     * @param rowSeparatorWrite the row separator
     */
    public void setRowSeparatorWrite(String rowSeparatorWrite) {
        this.rowSeparatorWrite = rowSeparatorWrite;
    }

    /**
     * Set the field delimiter. The default is " (a double quote).
     * The value 0 means no field delimiter is used.
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
     * Set the escape character (used to escape the field delimiter). The
     * default is " (a double quote). The value 0 means no escape character is used.
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

    /**
     * Set the line separator.
     *
     * @param lineSeparator the line separator
     */
    public void setLineSeparator(String lineSeparator) {
        this.lineSeparator = lineSeparator;
    }

    /**
     * Set the value that represents NULL.
     *
     * @param nullString the null
     */
    public void setNullString(String nullString) {
        this.nullString = nullString;
    }

}
