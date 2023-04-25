/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.Constraint;
import org.h2.constraint.Constraint.Type;
import org.h2.engine.Comment;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.RightOwner;
import org.h2.engine.Role;
import org.h2.engine.SessionLocal;
import org.h2.engine.Setting;
import org.h2.engine.User;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.schema.Constant;
import org.h2.schema.Domain;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.schema.UserDefinedFunction;
import org.h2.table.Column;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableType;
import org.h2.util.HasSQL;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueVarchar;

/**
 * This class represents the statement
 * SCRIPT
 */
public class ScriptCommand extends ScriptBase {

    private static final Comparator<? super DbObject> BY_NAME_COMPARATOR = (o1, o2) -> {
        if (o1 instanceof SchemaObject && o2 instanceof SchemaObject) {
            int cmp = ((SchemaObject) o1).getSchema().getName().compareTo(((SchemaObject) o2).getSchema().getName());
            if (cmp != 0) {
                return cmp;
            }
        }
        return o1.getName().compareTo(o2.getName());
    };

    private Charset charset = StandardCharsets.UTF_8;
    private Set<String> schemaNames;
    private Collection<Table> tables;
    private boolean passwords;

    // true if we're generating the INSERT..VALUES statements for row values
    private boolean data;
    private boolean settings;

    // true if we're generating the DROP statements
    private boolean drop;
    private boolean simple;
    private boolean withColumns;
    private boolean version = true;

    private LocalResult result;
    private String lineSeparatorString;
    private byte[] lineSeparator;
    private byte[] buffer;
    private boolean tempLobTableCreated;
    private int nextLobId;
    private int lobBlockSize = Constants.IO_BUFFER_SIZE;

    public ScriptCommand(SessionLocal session) {
        super(session);
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    // TODO lock all tables for 'script' command

    public void setSchemaNames(Set<String> schemaNames) {
        this.schemaNames = schemaNames;
    }

    public void setTables(Collection<Table> tables) {
        this.tables = tables;
    }

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

    @Override
    public ResultInterface queryMeta() {
        LocalResult r = createResult();
        r.done();
        return r;
    }

    private LocalResult createResult() {
        return new LocalResult(session, new Expression[] {
                new ExpressionColumn(getDatabase(), new Column("SCRIPT", TypeInfo.TYPE_VARCHAR)) }, 1, 1);
    }

    @Override
    public ResultInterface query(long maxrows) {
        session.getUser().checkAdmin();
        reset();
        Database db = getDatabase();
        if (schemaNames != null) {
            for (String schemaName : schemaNames) {
                Schema schema = db.findSchema(schemaName);
                if (schema == null) {
                    throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1,
                            schemaName);
                }
            }
        }
        try {
            result = createResult();
            deleteStore();
            openOutput();
            if (out != null) {
                buffer = new byte[Constants.IO_BUFFER_SIZE];
            }
            if (version) {
                add("-- H2 " + Constants.VERSION, true);
            }
            if (settings) {
                for (Setting setting : db.getAllSettings()) {
                    if (setting.getName().equals(SetTypes.getTypeName(
                            SetTypes.CREATE_BUILD))) {
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
            RightOwner[] rightOwners = db.getAllUsersAndRoles().toArray(new RightOwner[0]);
            // ADMIN users first, other users next, roles last
            Arrays.sort(rightOwners, (o1, o2) -> {
                boolean b = o1 instanceof User;
                if (b != o2 instanceof User) {
                    return b ? -1 : 1;
                }
                if (b) {
                    b = ((User) o1).isAdmin();
                    if (b != ((User) o2).isAdmin()) {
                        return b ? -1 : 1;
                    }
                }
                return o1.getName().compareTo(o2.getName());
            });
            for (RightOwner rightOwner : rightOwners) {
                if (rightOwner instanceof User) {
                    add(((User) rightOwner).getCreateSQL(passwords), false);
                } else {
                    add(((Role) rightOwner).getCreateSQL(true), false);
                }
            }
            ArrayList<Schema> schemas = new ArrayList<>();
            for (Schema schema : db.getAllSchemas()) {
                if (excludeSchema(schema)) {
                    continue;
                }
                schemas.add(schema);
                add(schema.getCreateSQL(), false);
            }
            dumpDomains(schemas);
            for (Schema schema : schemas) {
                for (Constant constant : sorted(schema.getAllConstants(), Constant.class)) {
                    add(constant.getCreateSQL(), false);
                }
            }

            final ArrayList<Table> tables = db.getAllTablesAndViews();
            // sort by id, so that views are after tables and views on views
            // after the base views
            tables.sort(Comparator.comparingInt(Table::getId));

            // Generate the DROP XXX  ... IF EXISTS
            for (Table table : tables) {
                if (excludeSchema(table.getSchema())) {
                    continue;
                }
                if (excludeTable(table)) {
                    continue;
                }
                if (table.isHidden()) {
                    continue;
                }
                table.lock(session, Table.READ_LOCK);
                String sql = table.getCreateSQL();
                if (sql == null) {
                    // null for metadata tables
                    continue;
                }
                if (drop) {
                    add(table.getDropSQL(), false);
                }
            }
            for (Schema schema : schemas) {
                for (UserDefinedFunction userDefinedFunction : sorted(schema.getAllFunctionsAndAggregates(),
                        UserDefinedFunction.class)) {
                    if (drop) {
                        add(userDefinedFunction.getDropSQL(), false);
                    }
                    add(userDefinedFunction.getCreateSQL(), false);
                }
            }
            for (Schema schema : schemas) {
                for (Sequence sequence : sorted(schema.getAllSequences(), Sequence.class)) {
                    if (sequence.getBelongsToTable()) {
                        continue;
                    }
                    if (drop) {
                        add(sequence.getDropSQL(), false);
                    }
                    add(sequence.getCreateSQL(), false);
                }
            }

            // Generate CREATE TABLE and INSERT...VALUES
            int count = 0;
            for (Table table : tables) {
                if (excludeSchema(table.getSchema())) {
                    continue;
                }
                if (excludeTable(table)) {
                    continue;
                }
                if (table.isHidden()) {
                    continue;
                }
                table.lock(session, Table.READ_LOCK);
                String createTableSql = table.getCreateSQL();
                if (createTableSql == null) {
                    // null for metadata tables
                    continue;
                }
                final TableType tableType = table.getTableType();
                add(createTableSql, false);
                final ArrayList<Constraint> constraints = table.getConstraints();
                if (constraints != null) {
                    for (Constraint constraint : constraints) {
                        if (Constraint.Type.PRIMARY_KEY == constraint.getConstraintType()) {
                            add(constraint.getCreateSQLWithoutIndexes(), false);
                        }
                    }
                }
                if (TableType.TABLE == tableType) {
                    if (table.canGetRowCount(session)) {
                        StringBuilder builder = new StringBuilder("-- ")
                                .append(table.getRowCountApproximation(session))
                                .append(" +/- SELECT COUNT(*) FROM ");
                        table.getSQL(builder, HasSQL.TRACE_SQL_FLAGS);
                        add(builder.toString(), false);
                    }
                    if (data) {
                        count = generateInsertValues(count, table);
                    }
                }
                final ArrayList<Index> indexes = table.getIndexes();
                for (int j = 0; indexes != null && j < indexes.size(); j++) {
                    Index index = indexes.get(j);
                    if (!index.getIndexType().getBelongsToConstraint()) {
                        add(index.getCreateSQL(), false);
                    }
                }
            }
            if (tempLobTableCreated) {
                add("DROP TABLE IF EXISTS SYSTEM_LOB_STREAM", true);
                add("DROP ALIAS IF EXISTS SYSTEM_COMBINE_CLOB", true);
                add("DROP ALIAS IF EXISTS SYSTEM_COMBINE_BLOB", true);
                tempLobTableCreated = false;
            }
            // Generate CREATE CONSTRAINT ...
            ArrayList<Constraint> constraints = new ArrayList<>();
            for (Schema schema : schemas) {
                for (Constraint constraint : schema.getAllConstraints()) {
                    if (excludeTable(constraint.getTable())) {
                        continue;
                    }
                    Type constraintType = constraint.getConstraintType();
                    if (constraintType != Type.DOMAIN && constraint.getTable().isHidden()) {
                        continue;
                    }
                    if (constraintType != Constraint.Type.PRIMARY_KEY) {
                        constraints.add(constraint);
                    }
                }
            }
            constraints.sort(null);
            for (Constraint constraint : constraints) {
                add(constraint.getCreateSQLWithoutIndexes(), false);
            }
            // Generate CREATE TRIGGER ...
            for (Schema schema : schemas) {
                for (TriggerObject trigger : schema.getAllTriggers()) {
                    if (excludeTable(trigger.getTable())) {
                        continue;
                    }
                    add(trigger.getCreateSQL(), false);
                }
            }
            // Generate GRANT ...
            dumpRights(db);
            // Generate COMMENT ON ...
            for (Comment comment : db.getAllComments()) {
                add(comment.getCreateSQL(), false);
            }
            if (out != null) {
                out.close();
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, getFileName());
        } finally {
            closeIO();
        }
        result.done();
        LocalResult r = result;
        reset();
        return r;
    }

    private void dumpDomains(ArrayList<Schema> schemas) throws IOException {
        TreeMap<Domain, TreeSet<Domain>> referencingDomains = new TreeMap<>(BY_NAME_COMPARATOR);
        TreeSet<Domain> known = new TreeSet<>(BY_NAME_COMPARATOR);
        for (Schema schema : schemas) {
            for (Domain domain : sorted(schema.getAllDomains(), Domain.class)) {
                Domain parent = domain.getDomain();
                if (parent == null) {
                    addDomain(domain);
                } else {
                    TreeSet<Domain> set = referencingDomains.get(parent);
                    if (set == null) {
                        set = new TreeSet<>(BY_NAME_COMPARATOR);
                        referencingDomains.put(parent, set);
                    }
                    set.add(domain);
                    if (parent.getDomain() == null || !schemas.contains(parent.getSchema())) {
                        known.add(parent);
                    }
                }
            }
        }
        while (!referencingDomains.isEmpty()) {
            TreeSet<Domain> known2 = new TreeSet<>(BY_NAME_COMPARATOR);
            for (Domain d : known) {
                TreeSet<Domain> set = referencingDomains.remove(d);
                if (set != null) {
                    for (Domain d2 : set) {
                        addDomain(d2);
                        known2.add(d2);
                    }
                }
            }
            known = known2;
        }
    }

    private void dumpRights(Database db) throws IOException {
        Right[] rights = db.getAllRights().toArray(new Right[0]);
        Arrays.sort(rights, (o1, o2) -> {
            Role r1 = o1.getGrantedRole(), r2 = o2.getGrantedRole();
            if ((r1 == null) != (r2 == null)) {
                return r1 == null ? -1 : 1;
            }
            if (r1 == null) {
                DbObject g1 = o1.getGrantedObject(), g2 = o2.getGrantedObject();
                if ((g1 == null) != (g2 == null)) {
                    return g1 == null ? -1 : 1;
                }
                if (g1 != null) {
                    if (g1 instanceof Schema != g2 instanceof Schema) {
                        return g1 instanceof Schema ? -1 : 1;
                    }
                    int cmp = g1.getName().compareTo(g2.getName());
                    if (cmp != 0) {
                        return cmp;
                    }
                }
            } else {
                int cmp = r1.getName().compareTo(r2.getName());
                if (cmp != 0) {
                    return cmp;
                }
            }
            return o1.getGrantee().getName().compareTo(o2.getGrantee().getName());
        });
        for (Right right : rights) {
            DbObject object = right.getGrantedObject();
            if (object != null) {
                if (object instanceof Schema) {
                    if (excludeSchema((Schema) object)) {
                        continue;
                    }
                } else if (object instanceof Table) {
                    Table table = (Table) object;
                    if (excludeSchema(table.getSchema())) {
                        continue;
                    }
                    if (excludeTable(table)) {
                        continue;
                    }
                }
            }
            add(right.getCreateSQL(), false);
        }
    }

    private void addDomain(Domain domain) throws IOException {
        if (drop) {
            add(domain.getDropSQL(), false);
        }
        add(domain.getCreateSQL(), false);
    }

    private static <T extends DbObject> T[] sorted(Collection<T> collection, Class<T> clazz) {
        @SuppressWarnings("unchecked")
        T[] array = collection.toArray((T[]) java.lang.reflect.Array.newInstance(clazz, 0));
        Arrays.sort(array, BY_NAME_COMPARATOR);
        return array;
    }

    private int generateInsertValues(int count, Table table) throws IOException {
        PlanItem plan = table.getBestPlanItem(session, null, null, -1, null, null);
        Index index = plan.getIndex();
        Cursor cursor = index.find(session, null, null);
        Column[] columns = table.getColumns();
        boolean withGenerated = false, withGeneratedAlwaysAsIdentity = false;
        for (Column c : columns) {
            if (c.isGeneratedAlways()) {
                if (c.isIdentity()) {
                    withGeneratedAlwaysAsIdentity = true;
                } else {
                    withGenerated = true;
                }
            }
        }
        StringBuilder builder = new StringBuilder("INSERT INTO ");
        table.getSQL(builder, HasSQL.DEFAULT_SQL_FLAGS);
        if (withGenerated || withGeneratedAlwaysAsIdentity || withColumns) {
            builder.append('(');
            boolean needComma = false;
            for (Column column : columns) {
                if (!column.isGenerated()) {
                    if (needComma) {
                        builder.append(", ");
                    }
                    needComma = true;
                    column.getSQL(builder, HasSQL.DEFAULT_SQL_FLAGS);
                }
            }
            builder.append(')');
            if (withGeneratedAlwaysAsIdentity) {
                builder.append(" OVERRIDING SYSTEM VALUE");
            }
        }
        builder.append(" VALUES");
        if (!simple) {
            builder.append('\n');
        }
        builder.append('(');
        String ins = builder.toString();
        builder = null;
        int columnCount = columns.length;
        while (cursor.next()) {
            Row row = cursor.get();
            if (builder == null) {
                builder = new StringBuilder(ins);
            } else {
                builder.append(",\n(");
            }
            boolean needComma = false;
            for (int i = 0; i < columnCount; i++) {
                if (columns[i].isGenerated()) {
                    continue;
                }
                if (needComma) {
                    builder.append(", ");
                }
                needComma = true;
                Value v = row.getValue(i);
                if (v.getType().getPrecision() > lobBlockSize) {
                    int id;
                    if (v.getValueType() == Value.CLOB) {
                        id = writeLobStream(v);
                        builder.append("SYSTEM_COMBINE_CLOB(").append(id).append(')');
                    } else if (v.getValueType() == Value.BLOB) {
                        id = writeLobStream(v);
                        builder.append("SYSTEM_COMBINE_BLOB(").append(id).append(')');
                    } else {
                        v.getSQL(builder, HasSQL.NO_CASTS);
                    }
                } else {
                    v.getSQL(builder, HasSQL.NO_CASTS);
                }
            }
            builder.append(')');
            count++;
            if ((count & 127) == 0) {
                checkCanceled();
            }
            if (simple || builder.length() > Constants.IO_BUFFER_SIZE) {
                add(builder.toString(), true);
                builder = null;
            }
        }
        if (builder != null) {
            add(builder.toString(), true);
        }
        return count;
    }

    private int writeLobStream(Value v) throws IOException {
        if (!tempLobTableCreated) {
            add("CREATE CACHED LOCAL TEMPORARY TABLE IF NOT EXISTS SYSTEM_LOB_STREAM" +
                    "(ID INT NOT NULL, PART INT NOT NULL, " +
                    "CDATA VARCHAR, BDATA VARBINARY)",
                    true);
            add("ALTER TABLE SYSTEM_LOB_STREAM ADD CONSTRAINT SYSTEM_LOB_STREAM_PRIMARY_KEY PRIMARY KEY(ID, PART)",
                    true);
            String className = getClass().getName();
            add("CREATE ALIAS IF NOT EXISTS " + "SYSTEM_COMBINE_CLOB FOR '" + className + ".combineClob'", true);
            add("CREATE ALIAS IF NOT EXISTS " + "SYSTEM_COMBINE_BLOB FOR '" + className + ".combineBlob'", true);
            tempLobTableCreated = true;
        }
        int id = nextLobId++;
        switch (v.getValueType()) {
        case Value.BLOB: {
            byte[] bytes = new byte[lobBlockSize];
            try (InputStream input = v.getInputStream()) {
                for (int i = 0;; i++) {
                    StringBuilder buff = new StringBuilder(lobBlockSize * 2);
                    buff.append("INSERT INTO SYSTEM_LOB_STREAM VALUES(").append(id)
                            .append(", ").append(i).append(", NULL, X'");
                    int len = IOUtils.readFully(input, bytes, lobBlockSize);
                    if (len <= 0) {
                        break;
                    }
                    StringUtils.convertBytesToHex(buff, bytes, len).append("')");
                    String sql = buff.toString();
                    add(sql, true);
                }
            }
            break;
        }
        case Value.CLOB: {
            char[] chars = new char[lobBlockSize];

            try (Reader reader = v.getReader()) {
                for (int i = 0;; i++) {
                    StringBuilder buff = new StringBuilder(lobBlockSize * 2);
                    buff.append("INSERT INTO SYSTEM_LOB_STREAM VALUES(").append(id).append(", ").append(i)
                            .append(", ");
                    int len = IOUtils.readFully(reader, chars, lobBlockSize);
                    if (len == 0) {
                        break;
                    }
                    StringUtils.quoteStringSQL(buff, new String(chars, 0, len)).
                        append(", NULL)");
                    String sql = buff.toString();
                    add(sql, true);
                }
            }
            break;
        }
        default:
            throw DbException.getInternalError("type:" + v.getValueType());
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
     * @throws SQLException on failure
     */
    public static InputStream combineBlob(Connection conn, int id)
            throws SQLException {
        if (id < 0) {
            return null;
        }
        final ResultSet rs = getLobStream(conn, "BDATA", id);
        return new InputStream() {
            private InputStream current;
            private boolean closed;
            @Override
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
                        throw DataUtils.convertToIOException(e);
                    }
                }
            }
            @Override
            public void close() throws IOException {
                if (closed) {
                    return;
                }
                closed = true;
                try {
                    rs.close();
                } catch (SQLException e) {
                    throw DataUtils.convertToIOException(e);
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
     * @throws SQLException on failure
     */
    public static Reader combineClob(Connection conn, int id) throws SQLException {
        if (id < 0) {
            return null;
        }
        final ResultSet rs = getLobStream(conn, "CDATA", id);
        return new Reader() {
            private Reader current;
            private boolean closed;
            @Override
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
                        throw DataUtils.convertToIOException(e);
                    }
                }
            }
            @Override
            public void close() throws IOException {
                if (closed) {
                    return;
                }
                closed = true;
                try {
                    rs.close();
                } catch (SQLException e) {
                    throw DataUtils.convertToIOException(e);
                }
            }
            @Override
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

    private static ResultSet getLobStream(Connection conn, String column, int id)
            throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT " + column +
                " FROM SYSTEM_LOB_STREAM WHERE ID=? ORDER BY PART");
        prep.setInt(1, id);
        return prep.executeQuery();
    }

    private void reset() {
        result = null;
        buffer = null;
        lineSeparatorString = System.lineSeparator();
        lineSeparator = lineSeparatorString.getBytes(charset);
    }

    private boolean excludeSchema(Schema schema) {
        if (schemaNames != null && !schemaNames.contains(schema.getName())) {
            return true;
        }
        if (tables != null) {
            // if filtering on specific tables, only include those schemas
            for (Table table : schema.getAllTablesAndViews(session)) {
                if (tables.contains(table)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private boolean excludeTable(Table table) {
        return tables != null && !tables.contains(table);
    }

    private void add(String s, boolean insert) throws IOException {
        if (s == null) {
            return;
        }
        if (lineSeparator.length > 1 || lineSeparator[0] != '\n') {
            s = StringUtils.replaceAll(s, "\n", lineSeparatorString);
        }
        s += ";";
        if (out != null) {
            byte[] buff = s.getBytes(charset);
            int len = MathUtils.roundUpInt(buff.length +
                    lineSeparator.length, Constants.FILE_BLOCK_SIZE);
            buffer = Utils.copy(buff, buffer);

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
                result.addRow(ValueVarchar.get(s));
            }
        } else {
            result.addRow(ValueVarchar.get(s));
        }
    }

    public void setSimple(boolean simple) {
        this.simple = simple;
    }

    public void setWithColumns(boolean withColumns) {
        this.withColumns = withColumns;
    }

    public void setVersion(boolean version) {
        this.version = version;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    @Override
    public int getType() {
        return CommandInterface.SCRIPT;
    }

}
