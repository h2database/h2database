/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import org.h2.command.Parser;
import org.h2.constant.SysProperties;
import org.h2.constraint.Constraint;
import org.h2.engine.Comment;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.FunctionAlias;
import org.h2.engine.Right;
import org.h2.engine.Role;
import org.h2.engine.Session;
import org.h2.engine.Setting;
import org.h2.engine.User;
import org.h2.engine.UserAggregate;
import org.h2.engine.UserDataType;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.schema.Constant;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.table.Column;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.util.ByteUtils;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.ObjectArray;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueString;

/**
 * This class represents the statement
 * SCRIPT
 */
public class ScriptCommand extends ScriptBase {

    private boolean passwords;
    private boolean data;
    private boolean settings;
    private boolean drop;
    private boolean simple;
    private LocalResult result;
    private byte[] lineSeparator;
    private byte[] buffer;
    private boolean tempLobTableCreated;
    private int nextLobId;
    private int lobBlockSize = Constants.IO_BUFFER_SIZE;

    public ScriptCommand(Session session) {
        super(session);
    }

    public boolean isQuery() {
        return true;
    }

    // TODO lock all tables for 'script' command

    public void setData(boolean data) {
        this.data = data;
    }

    public void setPasswords(boolean passwords) {
        this.passwords = passwords;
    }

    public void setSettings(boolean settings) {
        this.settings = settings;
    }

    public void setLobBlockSize(long blockSize) {
        this.lobBlockSize = MathUtils.convertLongToInt(blockSize);
    }

    public void setDrop(boolean drop) {
        this.drop = drop;
    }

    public LocalResult queryMeta() throws SQLException {
        LocalResult r = createResult();
        r.done();
        return r;
    }

    private LocalResult createResult() {
        Expression[] expressions = new Expression[] { new ExpressionColumn(session.getDatabase(), new Column("SCRIPT",
                Value.STRING)) };
        return new LocalResult(session, expressions, 1);
    }

    public LocalResult query(int maxrows) throws SQLException {
        session.getUser().checkAdmin();
        reset();
        try {
            result = createResult();
            deleteStore();
            openOutput();
            if (out != null) {
                buffer = new byte[Constants.IO_BUFFER_SIZE];
            }
            Database db = session.getDatabase();
            if (settings) {
                for (Setting setting : db.getAllSettings()) {
                    if (setting.getName().equals(SetTypes.getTypeName(SetTypes.CREATE_BUILD))) {
                        // don't add CREATE_BUILD to the script
                        // (it is only set when creating the database)
                        continue;
                    }
                    add(setting.getCreateSQL(), false);
                }
            }
            if (out != null) {
                add("", true);
            }
            for (User user : db.getAllUsers()) {
                add(user.getCreateSQL(passwords, true), false);
            }
            for (Role role : db.getAllRoles()) {
                add(role.getCreateSQL(true), false);
            }
            for (Schema schema : db.getAllSchemas()) {
                add(schema.getCreateSQL(), false);
            }
            for (UserDataType datatype : db.getAllUserDataTypes()) {
                if (drop) {
                    add(datatype.getDropSQL(), false);
                }
                add(datatype.getCreateSQL(), false);
            }
            for (SchemaObject obj : db.getAllSchemaObjects(DbObject.CONSTANT)) {
                Constant constant = (Constant) obj;
                add(constant.getCreateSQL(), false);
            }
            for (FunctionAlias alias : db.getAllFunctionAliases()) {
                if (drop) {
                    add(alias.getDropSQL(), false);
                }
                add(alias.getCreateSQL(), false);
            }
            for (UserAggregate agg : db.getAllAggregates()) {
                if (drop) {
                    add(agg.getDropSQL(), false);
                }
                add(agg.getCreateSQL(), false);
            }
            ObjectArray<Table> tables = db.getAllTablesAndViews();
            // sort by id, so that views are after tables and views on views
            // after the base views
            tables.sort(new Comparator<Table>() {
                public int compare(Table t1, Table t2) {
                    return t1.getId() - t2.getId();
                }
            });
            for (Table table : tables) {
                table.lock(session, false, false);
                String sql = table.getCreateSQL();
                if (sql == null) {
                    // null for metadata tables
                    continue;
                }
                if (drop) {
                    add(table.getDropSQL(), false);
                }
            }
            for (SchemaObject obj : db.getAllSchemaObjects(DbObject.SEQUENCE)) {
                Sequence sequence = (Sequence) obj;
                if (drop && !sequence.getBelongsToTable()) {
                    add(sequence.getDropSQL(), false);
                }
                add(sequence.getCreateSQL(), false);
            }
            for (Table table : tables) {
                table.lock(session, false, false);
                String sql = table.getCreateSQL();
                if (sql == null) {
                    // null for metadata tables
                    continue;
                }
                String tableType = table.getTableType();
                add(sql, false);
                if (Table.TABLE.equals(tableType)) {
                    if (table.canGetRowCount()) {
                        String rowcount = "-- " + table.getRowCountApproximation() + " +/- SELECT COUNT(*) FROM "
                                + table.getSQL();
                        add(rowcount, false);
                    }
                    if (data) {
                        PlanItem plan = table.getBestPlanItem(session, null);
                        Index index = plan.getIndex();
                        Cursor cursor = index.find(session, null, null);
                        Column[] columns = table.getColumns();
                        StatementBuilder buff = new StatementBuilder("INSERT INTO ");
                        buff.append(table.getSQL()).append('(');
                        for (Column col : columns) {
                            buff.appendExceptFirst(", ");
                            buff.append(Parser.quoteIdentifier(col.getName()));
                        }
                        buff.append(") VALUES");
                        if (!simple) {
                            buff.append('\n');
                        }
                        buff.append('(');
                        String ins = buff.toString();
                        buff = null;
                        while (cursor.next()) {
                            Row row = cursor.get();
                            if (buff == null) {
                                buff = new StatementBuilder(ins);
                            } else {
                                buff.append(",\n(");
                            }
                            for (int j = 0; j < row.getColumnCount(); j++) {
                                if (j > 0) {
                                    buff.append(", ");
                                }
                                Value v = row.getValue(j);
                                if (v.getPrecision() > lobBlockSize) {
                                    int id;
                                    if (v.getType() == Value.CLOB) {
                                        id = writeLobStream((ValueLob) v);
                                        buff.append("SYSTEM_COMBINE_CLOB(" + id + ")");
                                    } else if (v.getType() == Value.BLOB) {
                                        id = writeLobStream((ValueLob) v);
                                        buff.append("SYSTEM_COMBINE_BLOB(" + id + ")");
                                    } else {
                                        buff.append(v.getSQL());
                                    }
                                } else {
                                    buff.append(v.getSQL());
                                }
                            }
                            buff.append(')');
                            if (simple || buff.length() > Constants.IO_BUFFER_SIZE) {
                                add(buff.toString(), true);
                                buff = null;
                            }
                        }
                        if (buff != null) {
                            add(buff.toString(), true);
                        }
                    }
                }
                ObjectArray<Index> indexes = table.getIndexes();
                for (int j = 0; indexes != null && j < indexes.size(); j++) {
                    Index index = indexes.get(j);
                    if (!index.getIndexType().getBelongsToConstraint()) {
                        add(index.getCreateSQL(), false);
                    }
                }
            }
            if (tempLobTableCreated) {
                add("DROP TABLE IF EXISTS SYSTEM_LOB_STREAM", true);
                add("CALL SYSTEM_COMBINE_BLOB(-1)", true);
                add("DROP ALIAS IF EXISTS SYSTEM_COMBINE_CLOB", true);
                add("DROP ALIAS IF EXISTS SYSTEM_COMBINE_BLOB", true);
                tempLobTableCreated = false;
            }
            ObjectArray<SchemaObject> constraints = db.getAllSchemaObjects(DbObject.CONSTRAINT);
            constraints.sort(new Comparator<SchemaObject>() {
                public int compare(SchemaObject c1, SchemaObject c2) {
                    return ((Constraint) c1).compareTo((Constraint) c2);
                }
            });
            for (SchemaObject obj : constraints) {
                Constraint constraint = (Constraint) obj;
                add(constraint.getCreateSQLWithoutIndexes(), false);
            }
            for (SchemaObject obj : db.getAllSchemaObjects(DbObject.TRIGGER)) {
                TriggerObject trigger = (TriggerObject) obj;
                add(trigger.getCreateSQL(), false);
            }
            for (Right right : db.getAllRights()) {
                add(right.getCreateSQL(), false);
            }
            for (Comment comment : db.getAllComments()) {
                add(comment.getCreateSQL(), false);
            }
            if (out != null) {
                out.close();
            }
        } catch (IOException e) {
            throw Message.convertIOException(e, getFileName());
        } finally {
            closeIO();
        }
        result.done();
        LocalResult r = result;
        reset();
        return r;
    }

    private int writeLobStream(ValueLob v) throws IOException, SQLException {
        if (!tempLobTableCreated) {
            add("CREATE TABLE IF NOT EXISTS SYSTEM_LOB_STREAM(ID INT, PART INT, CDATA VARCHAR, BDATA BINARY, PRIMARY KEY(ID, PART))", true);
            add("CREATE ALIAS IF NOT EXISTS SYSTEM_COMBINE_CLOB FOR \"" + this.getClass().getName() + ".combineClob\"", true);
            add("CREATE ALIAS IF NOT EXISTS SYSTEM_COMBINE_BLOB FOR \"" + this.getClass().getName() + ".combineBlob\"", true);
            tempLobTableCreated = true;
        }
        int id = nextLobId++;
        switch (v.getType()) {
        case Value.BLOB: {
            byte[] bytes = new byte[lobBlockSize];
            InputStream input = v.getInputStream();
            try {
                for (int i = 0;; i++) {
                    StringBuilder buff = new StringBuilder(lobBlockSize * 2);
                    buff.append("INSERT INTO SYSTEM_LOB_STREAM VALUES(" + id + ", " + i + ", NULL, '");
                    int len = IOUtils.readFully(input, bytes, 0, lobBlockSize);
                    if (len <= 0) {
                        break;
                    }
                    buff.append(ByteUtils.convertBytesToString(bytes, len)).append("')");
                    String sql = buff.toString();
                    add(sql, true);
                }
            } finally {
                IOUtils.closeSilently(input);
            }
            break;
        }
        case Value.CLOB: {
            char[] chars = new char[lobBlockSize];
            Reader reader = v.getReader();
            try {
                for (int i = 0;; i++) {
                    StringBuilder buff = new StringBuilder(lobBlockSize * 2);
                    buff.append("INSERT INTO SYSTEM_LOB_STREAM VALUES(" + id + ", " + i + ", ");
                    int len = IOUtils.readFully(reader, chars, lobBlockSize);
                    if (len < 0) {
                        break;
                    }
                    buff.append(StringUtils.quoteStringSQL(new String(chars, 0, len))).
                        append(", NULL)");
                    String sql = buff.toString();
                    add(sql, true);
                }
            } finally {
                IOUtils.closeSilently(reader);
            }
            break;
        }
        default:
            Message.throwInternalError("type:" + v.getType());
        }
        return id;
    }

    /**
     * Combine a BLOB.
     * This method is called from the script.
     * When calling with id -1, the file is deleted.
     *
     * @param conn a connection
     * @param id the lob id
     * @return a stream for the combined data
     */
    public static InputStream combineBlob(Connection conn, int id) throws SQLException {
        if (id < 0) {
            return null;
        }
        final ResultSet rs = getLobStream(conn, "BDATA", id);
        return new InputStream() {
            private InputStream current;
            private boolean closed;
            public int read() throws IOException {
                while (true) {
                    try {
                        if (current == null) {
                            if (closed) {
                                return -1;
                            }
                            if (!rs.next()) {
                                close();
                                return -1;
                            }
                            current = rs.getBinaryStream(1);
                            current = new BufferedInputStream(current);
                        }
                        int x = current.read();
                        if (x >= 0) {
                            return x;
                        }
                        current = null;
                    } catch (SQLException e) {
                        throw Message.convertToIOException(e);
                    }
                }
            }
            public void close() throws IOException {
                if (closed) {
                    return;
                }
                closed = true;
                try {
                    rs.close();
                } catch (SQLException e) {
                    throw Message.convertToIOException(e);
                }
            }
        };
    }

    /**
     * Combine a CLOB.
     * This method is called from the script.
     *
     * @param conn a connection
     * @param id the lob id
     * @return a reader for the combined data
     */
    public static Reader combineClob(Connection conn, int id) throws SQLException {
        if (id < 0) {
            return null;
        }
        final ResultSet rs = getLobStream(conn, "CDATA", id);
        return new Reader() {
            private Reader current;
            private boolean closed;
            public int read() throws IOException {
                while (true) {
                    try {
                        if (current == null) {
                            if (closed) {
                                return -1;
                            }
                            if (!rs.next()) {
                                close();
                                return -1;
                            }
                            current = rs.getCharacterStream(1);
                            current = new BufferedReader(current);
                        }
                        int x = current.read();
                        if (x >= 0) {
                            return x;
                        }
                        current = null;
                    } catch (SQLException e) {
                        throw Message.convertToIOException(e);
                    }
                }
            }
            public void close() throws IOException {
                if (closed) {
                    return;
                }
                closed = true;
                try {
                    rs.close();
                } catch (SQLException e) {
                    throw Message.convertToIOException(e);
                }
            }
            public int read(char[] buffer, int off, int len) throws IOException {
                if (len == 0) {
                    return 0;
                }
                int c = read();
                if (c == -1) {
                    return -1;
                }
                buffer[off] = (char) c;
                int i = 1;
                for (; i < len; i++) {
                    c = read();
                    if (c == -1) {
                        break;
                    }
                    buffer[off + i] = (char) c;
                }
                return i;
            }
        };
    }

    private static ResultSet getLobStream(Connection conn, String column, int id) throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "SELECT " + column + " FROM SYSTEM_LOB_STREAM WHERE ID=? ORDER BY PART");
        prep.setInt(1, id);
        return prep.executeQuery();
    }

    private void reset() {
        result = null;
        buffer = null;
        lineSeparator = StringUtils.utf8Encode(SysProperties.LINE_SEPARATOR);
    }

    private void add(String s, boolean insert) throws SQLException, IOException {
        if (s == null) {
            return;
        }
        s += ";";
        if (out != null) {
            byte[] buff = StringUtils.utf8Encode(s);
            int len = MathUtils.roundUp(buff.length + lineSeparator.length, Constants.FILE_BLOCK_SIZE);
            buffer = ByteUtils.copy(buff, buffer);

            if (len > buffer.length) {
                buffer = new byte[len];
            }
            System.arraycopy(buff, 0, buffer, 0, buff.length);
            for (int i = buff.length; i < len - lineSeparator.length; i++) {
                buffer[i] = ' ';
            }
            for (int j = 0, i = len - lineSeparator.length; i < len; i++, j++) {
                buffer[i] = lineSeparator[j];
            }
            out.write(buffer, 0, len);
            if (!insert) {
                Value[] row = new Value[1];
                row[0] = ValueString.get(s);
                result.addRow(row);
            }
        } else {
            Value[] row = new Value[1];
            row[0] = ValueString.get(s);
            result.addRow(row);
        }
    }

    public void setSimple(boolean simple) {
        this.simple = simple;
    }

}
