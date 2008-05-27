/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
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
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.table.Column;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.util.AutoCloseInputStream;
import org.h2.util.ByteUtils;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueString;

/**
 * This class represents the statement
 * SCRIPT
 */
public class ScriptCommand extends ScriptBase {

    private static final String TEMP_LOB_FILENAME = "system_temp_lob.db";
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
        LocalResult result = createResult();
        result.done();
        return result;
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
                ObjectArray settings = db.getAllSettings();
                for (int i = 0; i < settings.size(); i++) {
                    Setting setting = (Setting) settings.get(i);
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
            ObjectArray users = db.getAllUsers();
            for (int i = 0; i < users.size(); i++) {
                User user = (User) users.get(i);
                add(user.getCreateSQL(passwords, true), false);
            }
            ObjectArray roles = db.getAllRoles();
            for (int i = 0; i < roles.size(); i++) {
                Role role = (Role) roles.get(i);
                add(role.getCreateSQL(), false);
            }
            ObjectArray schemas = db.getAllSchemas();
            for (int i = 0; i < schemas.size(); i++) {
                Schema schema = (Schema) schemas.get(i);
                add(schema.getCreateSQL(), false);
            }
            ObjectArray datatypes = db.getAllUserDataTypes();
            for (int i = 0; i < datatypes.size(); i++) {
                UserDataType datatype = (UserDataType) datatypes.get(i);
                if (drop) {
                    add(datatype.getDropSQL(), false);
                }
                add(datatype.getCreateSQL(), false);
            }
            ObjectArray constants = db.getAllSchemaObjects(DbObject.CONSTANT);
            for (int i = 0; i < constants.size(); i++) {
                Constant constant = (Constant) constants.get(i);
                add(constant.getCreateSQL(), false);
            }
            ObjectArray functionAliases = db.getAllFunctionAliases();
            for (int i = 0; i < functionAliases.size(); i++) {
                FunctionAlias alias = (FunctionAlias) functionAliases.get(i);
                if (drop) {
                    add(alias.getDropSQL(), false);
                }
                add(alias.getCreateSQL(), false);
            }
            ObjectArray aggregates = db.getAllAggregates();
            for (int i = 0; i < aggregates.size(); i++) {
                UserAggregate agg = (UserAggregate) aggregates.get(i);
                if (drop) {
                    add(agg.getDropSQL(), false);
                }
                add(agg.getCreateSQL(), false);
            }
            ObjectArray tables = db.getAllSchemaObjects(DbObject.TABLE_OR_VIEW);
            // sort by id, so that views are after tables and views on views
            // after the base views
            tables.sort(new Comparator() {
                public int compare(Object o1, Object o2) {
                    Table t1 = (Table) o1;
                    Table t2 = (Table) o2;
                    return t1.getId() - t2.getId();
                }
            });
            for (int i = 0; i < tables.size(); i++) {
                Table table = (Table) tables.get(i);
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
            ObjectArray sequences = db.getAllSchemaObjects(DbObject.SEQUENCE);
            for (int i = 0; i < sequences.size(); i++) {
                Sequence sequence = (Sequence) sequences.get(i);
                if (drop) {
                    add(sequence.getDropSQL(), false);
                }
                add(sequence.getCreateSQL(), false);
            }
            for (int i = 0; i < tables.size(); i++) {
                Table table = (Table) tables.get(i);
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
                        String rowcount = "-- " + table.getRowCount(session) + " = SELECT COUNT(*) FROM "
                                + table.getSQL();
                        add(rowcount, false);
                    }
                    if (data) {
                        PlanItem plan = table.getBestPlanItem(session, null);
                        Index index = plan.getIndex();
                        Cursor cursor = index.find(session, null, null);
                        Column[] columns = table.getColumns();
                        StringBuffer buff = new StringBuffer();
                        buff.append("INSERT INTO ");
                        buff.append(table.getSQL());
                        buff.append('(');
                        for (int j = 0; j < columns.length; j++) {
                            if (j > 0) {
                                buff.append(", ");
                            }
                            buff.append(Parser.quoteIdentifier(columns[j].getName()));
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
                                buff = new StringBuffer(ins);
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
                            buff.append(")");
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
                ObjectArray indexes = table.getIndexes();
                for (int j = 0; indexes != null && j < indexes.size(); j++) {
                    Index index = (Index) indexes.get(j);
                    if (!index.getIndexType().belongsToConstraint()) {
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
            ObjectArray constraints = db.getAllSchemaObjects(DbObject.CONSTRAINT);
            constraints.sort(new Comparator() {
                public int compare(Object o1, Object o2) {
                    Constraint c1 = (Constraint) o1;
                    Constraint c2 = (Constraint) o2;
                    return c1.compareTo(c2);
                }
            });
            for (int i = 0; i < constraints.size(); i++) {
                Constraint constraint = (Constraint) constraints.get(i);
                add(constraint.getCreateSQLWithoutIndexes(), false);
            }
            ObjectArray triggers = db.getAllSchemaObjects(DbObject.TRIGGER);
            for (int i = 0; i < triggers.size(); i++) {
                TriggerObject trigger = (TriggerObject) triggers.get(i);
                add(trigger.getCreateSQL(), false);
            }
            ObjectArray rights = db.getAllRights();
            for (int i = 0; i < rights.size(); i++) {
                Right right = (Right) rights.get(i);
                add(right.getCreateSQL(), false);
            }
            ObjectArray comments = db.getAllComments();
            for (int i = 0; i < comments.size(); i++) {
                Comment comment = (Comment) comments.get(i);
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
            InputStream in = v.getInputStream();
            try {
                for (int i = 0;; i++) {
                    StringBuffer buff = new StringBuffer(lobBlockSize * 2);
                    buff.append("INSERT INTO SYSTEM_LOB_STREAM VALUES(" + id + ", " + i + ", NULL, '");
                    int len = IOUtils.readFully(in, bytes, 0, lobBlockSize);
                    if (len <= 0) {
                        break;
                    }
                    buff.append(ByteUtils.convertBytesToString(bytes, len));
                    buff.append("')");
                    String sql = buff.toString();
                    add(sql, true);
                }
            } finally {
                IOUtils.closeSilently(in);
            }
            break;
        }
        case Value.CLOB: {
            char[] chars = new char[lobBlockSize];
            Reader in = v.getReader();
            try {
                for (int i = 0;; i++) {
                    StringBuffer buff = new StringBuffer(lobBlockSize * 2);
                    buff.append("INSERT INTO SYSTEM_LOB_STREAM VALUES(" + id + ", " + i + ", ");
                    int len = IOUtils.readFully(in, chars, lobBlockSize);
                    if (len < 0) {
                        break;
                    }
                    buff.append(StringUtils.quoteStringSQL(new String(chars, 0, len)));
                    buff.append(", NULL)");
                    String sql = buff.toString();
                    add(sql, true);
                }
            } finally {
                IOUtils.closeSilently(in);
            }
            break;
        }
        default:
            throw Message.getInternalError("type:" + v.getType());
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
    public static InputStream combineBlob(Connection conn, int id) throws SQLException, IOException {
        if (id < 0) {
            FileUtils.delete(TEMP_LOB_FILENAME);
            return null;
        }
        ResultSet rs = getLobStream(conn, "BDATA", id);
        OutputStream out = FileUtils.openFileOutputStream(TEMP_LOB_FILENAME, false);
        while (rs.next()) {
            InputStream in = rs.getBinaryStream(1);
            IOUtils.copyAndCloseInput(in, out);
        }
        out.close();
        deleteLobStream(conn, id);
        return openAutoCloseInput();
    }

    /**
     * Combine a CLOB.
     * This method is called from the script.
     *
     * @param conn a connection
     * @param id the lob id
     * @return a reader for the combined data
     */
    public static Reader combineClob(Connection conn, int id) throws SQLException, IOException {
        ResultSet rs = getLobStream(conn, "CDATA", id);
        Writer out = FileUtils.openFileWriter(TEMP_LOB_FILENAME, false);
        while (rs.next()) {
            Reader in = new BufferedReader(rs.getCharacterStream(1));
            IOUtils.copyAndCloseInput(in, out);
        }
        out.close();
        deleteLobStream(conn, id);
        return IOUtils.getReader(openAutoCloseInput());
    }

    private static ResultSet getLobStream(Connection conn, String column, int id) throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "SELECT " + column + " FROM SYSTEM_LOB_STREAM WHERE ID=? ORDER BY PART");
        prep.setInt(1, id);
        return prep.executeQuery();
    }

    private static void deleteLobStream(Connection conn, int id) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("DELETE FROM SYSTEM_LOB_STREAM WHERE ID=?");
        prep.setInt(1, id);
        prep.execute();
    }

    private static InputStream openAutoCloseInput() throws IOException {
        InputStream in = FileUtils.openFileInputStream(TEMP_LOB_FILENAME);
        in = new BufferedInputStream(in);
        return new AutoCloseInputStream(in);
    }

    private void reset() throws SQLException {
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
