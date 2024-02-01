/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.MetaRecord;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreTool;
import org.h2.mvstore.StreamStore;
import org.h2.mvstore.db.LobStorageMap;
import org.h2.mvstore.db.ValueDataType;
import org.h2.mvstore.tx.TransactionMap;
import org.h2.mvstore.tx.TransactionStore;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.MetaType;
import org.h2.mvstore.type.StringDataType;
import org.h2.result.Row;
import org.h2.store.DataHandler;
import org.h2.store.FileLister;
import org.h2.store.FileStore;
import org.h2.store.LobStorageFrontend;
import org.h2.store.LobStorageInterface;
import org.h2.store.fs.FileUtils;
import org.h2.util.HasSQL;
import org.h2.util.IOUtils;
import org.h2.util.SmallLRUCache;
import org.h2.util.StringUtils;
import org.h2.util.TempFileDeleter;
import org.h2.util.Tool;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueCollectionBase;
import org.h2.value.ValueLob;
import org.h2.value.lob.LobData;
import org.h2.value.lob.LobDataDatabase;

/**
 * Helps recovering a corrupted database.
 */
public class Recover extends Tool implements DataHandler {

    private String databaseName;
    private int storageId;
    private String storageName;
    private int recordLength;
    private int valueId;
    private boolean trace;
    private ArrayList<MetaRecord> schema;
    private HashSet<Integer> objectIdSet;
    private HashMap<Integer, String> tableMap;
    private HashMap<String, String> columnTypeMap;
    private boolean lobMaps;

    /**
     * Options are case sensitive.
     * <table>
     * <caption>Supported options</caption>
     * <tr><td>[-help] or [-?]</td>
     * <td>Print the list of options</td></tr>
     * <tr><td>[-dir &lt;dir&gt;]</td>
     * <td>The directory (default: .)</td></tr>
     * <tr><td>[-db &lt;database&gt;]</td>
     * <td>The database name (all databases if not set)</td></tr>
     * <tr><td>[-trace]</td>
     * <td>Print additional trace information</td></tr>
     * <tr><td>[-transactionLog]</td>
     * <td>Print the transaction log</td></tr>
     * </table>
     * Encrypted databases need to be decrypted first.
     *
     * @param args the command line arguments
     * @throws SQLException on failure
     */
    public static void main(String... args) throws SQLException {
        new Recover().runTool(args);
    }

    /**
     * Dumps the contents of a database file to a human readable text file. This
     * text file can be used to recover most of the data. This tool does not
     * open the database and can be used even if the database files are
     * corrupted. A database can get corrupted if there is a bug in the database
     * engine or file system software, or if an application writes into the
     * database file that doesn't understand the file format, or if there is
     * a hardware problem.
     *
     * @param args the command line arguments
     * @throws SQLException on failure
     */
    @Override
    public void runTool(String... args) throws SQLException {
        String dir = ".";
        String db = null;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if ("-dir".equals(arg)) {
                dir = args[++i];
            } else if ("-db".equals(arg)) {
                db = args[++i];
            } else if ("-trace".equals(arg)) {
                trace = true;
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                showUsageAndThrowUnsupportedOption(arg);
            }
        }
        process(dir, db);
    }

    /**
     * INTERNAL
     * @param conn to use
     * @param lobId id of the LOB stream
     * @param precision not used
     * @return InputStream to read LOB content from
     * @throws SQLException on failure
     */
    public static InputStream readBlobMap(Connection conn, long lobId,
            long precision) throws SQLException {
        final PreparedStatement prep = conn.prepareStatement(
                "SELECT DATA FROM INFORMATION_SCHEMA.LOB_BLOCKS " +
                "WHERE LOB_ID = ? AND SEQ = ? AND ? > 0");
        prep.setLong(1, lobId);
        // precision is currently not really used,
        // it is just to improve readability of the script
        prep.setLong(3, precision);
        return new SequenceInputStream(
            new Enumeration<InputStream>() {

                private int seq;
                private byte[] data = fetch();

                private byte[] fetch() {
                    try {
                        prep.setInt(2, seq++);
                        ResultSet rs = prep.executeQuery();
                        if (rs.next()) {
                            return rs.getBytes(1);
                        }
                        return null;
                    } catch (SQLException e) {
                        throw DbException.convert(e);
                    }
                }

                @Override
                public boolean hasMoreElements() {
                    return data != null;
                }

                @Override
                public InputStream nextElement() {
                    ByteArrayInputStream in = new ByteArrayInputStream(data);
                    data = fetch();
                    return in;
                }
            }
        );
    }

    /**
     * INTERNAL
     * @param conn to use
     * @param lobId id of the LOB stream
     * @param precision not used
     * @return Reader to read LOB content from
     * @throws SQLException on failure
     */
    public static Reader readClobMap(Connection conn, long lobId, long precision)
            throws Exception {
        InputStream in = readBlobMap(conn, lobId, precision);
        return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    }

    private void trace(String message) {
        if (trace) {
            out.println(message);
        }
    }

    private void traceError(String message, Throwable t) {
        out.println(message + ": " + t.toString());
        if (trace) {
            t.printStackTrace(out);
        }
    }

    /**
     * Dumps the contents of a database to a SQL script file.
     *
     * @param dir the directory
     * @param db the database name (null for all databases)
     * @throws SQLException on failure
     */
    public static void execute(String dir, String db) throws SQLException {
        try {
            new Recover().process(dir, db);
        } catch (DbException e) {
            throw DbException.toSQLException(e);
        }
    }

    private void process(String dir, String db) {
        ArrayList<String> list = FileLister.getDatabaseFiles(dir, db, true);
        if (list.isEmpty()) {
            printNoDatabaseFilesFound(dir, db);
        }
        for (String fileName : list) {
            if (fileName.endsWith(Constants.SUFFIX_MV_FILE)) {
                String f = fileName.substring(0, fileName.length() -
                        Constants.SUFFIX_MV_FILE.length());
                try (PrintWriter writer = getWriter(fileName, ".txt")) {
                    MVStoreTool.dump(fileName, writer, true);
                    MVStoreTool.info(fileName, writer);
                }
                try (PrintWriter writer = getWriter(f + ".h2.db", ".sql")) {
                    dumpMVStoreFile(writer, fileName);
                }
            }
        }
    }

    private PrintWriter getWriter(String fileName, String suffix) {
        fileName = fileName.substring(0, fileName.length() - 3);
        String outputFile = fileName + suffix;
        trace("Created file: " + outputFile);
        try {
            return new PrintWriter(IOUtils.getBufferedWriter(
                    FileUtils.newOutputStream(outputFile, false)));
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private void getSQL(StringBuilder builder, String column, Value v) {
        if (v instanceof ValueLob) {
            ValueLob lob = (ValueLob) v;
            LobData lobData = lob.getLobData();
            if (lobData instanceof LobDataDatabase) {
                LobDataDatabase lobDataDatabase = (LobDataDatabase) lobData;
                int type = v.getValueType();
                long id = lobDataDatabase.getLobId();
                long precision;
                String columnType;
                if (type == Value.BLOB) {
                    precision = lob.octetLength();
                    columnType = "BLOB";
                    builder.append("READ_BLOB");
                } else {
                    precision = lob.charLength();
                    columnType = "CLOB";
                    builder.append("READ_CLOB");
                }
                if (lobMaps) {
                    builder.append("_MAP");
                } else {
                    builder.append("_DB");
                }
                columnTypeMap.put(column, columnType);
                builder.append('(').append(id).append(", ").append(precision).append(')');
                return;
            }
        }
        v.getSQL(builder, HasSQL.NO_CASTS);
    }

    private void setDatabaseName(String name) {
        databaseName = name;
    }

    private void dumpMVStoreFile(PrintWriter writer, String fileName) {
        writer.println("-- MVStore");
        String className = getClass().getName();
        writer.println("CREATE ALIAS IF NOT EXISTS READ_BLOB_MAP FOR '" + className + ".readBlobMap';");
        writer.println("CREATE ALIAS IF NOT EXISTS READ_CLOB_MAP FOR '" + className + ".readClobMap';");
        resetSchema();
        setDatabaseName(fileName.substring(0, fileName.length() -
                Constants.SUFFIX_MV_FILE.length()));
        try (MVStore mv = new MVStore.Builder().
                fileName(fileName).recoveryMode().readOnly().open()) {
            dumpLobMaps(writer, mv);
            writer.println("-- Layout");
            dumpLayout(writer, mv);
            writer.println("-- Meta");
            dumpMeta(writer, mv);
            writer.println("-- Types");
            dumpTypes(writer, mv);
            writer.println("-- Tables");
            TransactionStore store = new TransactionStore(mv, new ValueDataType());
            try {
                store.init();
            } catch (Throwable e) {
                writeError(writer, e);
            }

            // extract the metadata so we can dump the settings
            for (String mapName : mv.getMapNames()) {
                if (!mapName.startsWith("table.")) {
                    continue;
                }
                String tableId = mapName.substring("table.".length());
                if (Integer.parseInt(tableId) == 0) {
                    TransactionMap<Long, Row> dataMap = store.begin().openMap(mapName);
                    Iterator<Long> dataIt = dataMap.keyIterator(null);
                    while (dataIt.hasNext()) {
                        Long rowId = dataIt.next();
                        Row row = dataMap.get(rowId);
                        try {
                            writeMetaRow(row);
                        } catch (Throwable t) {
                            writeError(writer, t);
                        }
                    }
                }
            }
            // Have to do these before the tables because settings like COLLATION may affect
            // some of them, and we can't change settings after we have created user tables
            writeSchemaSET(writer);
            writer.println("---- Table Data ----");
            for (String mapName : mv.getMapNames()) {
                if (!mapName.startsWith("table.")) {
                    continue;
                }
                String tableId = mapName.substring("table.".length());
                if (Integer.parseInt(tableId) == 0) {
                    continue;
                }
                TransactionMap<?,?> dataMap = store.begin().openMap(mapName);
                Iterator<?> dataIt = dataMap.keyIterator(null);
                boolean init = false;
                while (dataIt.hasNext()) {
                    Object rowId = dataIt.next();
                    Object value = dataMap.get(rowId);
                    Value[] values;
                    if (value instanceof Row) {
                        values = ((Row) value).getValueList();
                        recordLength = values.length;
                    } else {
                        values = ((ValueCollectionBase) value).getList();
                        recordLength = values.length - 1;
                    }
                    if (!init) {
                        setStorage(Integer.parseInt(tableId));
                        // init the column types
                        StringBuilder builder = new StringBuilder();
                        for (valueId = 0; valueId < recordLength; valueId++) {
                            String columnName = storageName + "." + valueId;
                            builder.setLength(0);
                            getSQL(builder, columnName, values[valueId]);
                        }
                        createTemporaryTable(writer);
                        init = true;
                    }
                    StringBuilder buff = new StringBuilder();
                    buff.append("INSERT INTO O_").append(tableId)
                            .append(" VALUES(");
                    for (valueId = 0; valueId < recordLength; valueId++) {
                        if (valueId > 0) {
                            buff.append(", ");
                        }
                        String columnName = storageName + "." + valueId;
                        getSQL(buff, columnName, values[valueId]);
                    }
                    buff.append(");");
                    writer.println(buff.toString());
                }
            }
            writeSchema(writer);
            writer.println("DROP ALIAS READ_BLOB_MAP;");
            writer.println("DROP ALIAS READ_CLOB_MAP;");
            writer.println("DROP TABLE IF EXISTS INFORMATION_SCHEMA.LOB_BLOCKS;");
        } catch (Throwable e) {
            writeError(writer, e);
        }
    }

    private static void dumpLayout(PrintWriter writer, MVStore mv) {
        Map<String, String> layout = mv.getLayoutMap();
        for (Entry<String, String> e : layout.entrySet()) {
            writer.println("-- " + e.getKey() + " = " + e.getValue());
        }
    }

    private static void dumpMeta(PrintWriter writer, MVStore mv) {
        MVMap<String, String> meta = mv.getMetaMap();
        for (Entry<String, String> e : meta.entrySet()) {
            writer.println("-- " + e.getKey() + " = " + e.getValue());
        }
    }

    private static void dumpTypes(PrintWriter writer, MVStore mv) {
        MVMap.Builder<String, DataType<?>> builder = new MVMap.Builder<String, DataType<?>>()
                                                .keyType(StringDataType.INSTANCE)
                                                .valueType(new MetaType<>(null, null));
        MVMap<String,DataType<?>> map = mv.openMap("_", builder);
        for (Entry<String,?> e : map.entrySet()) {
            writer.println("-- " + e.getKey() + " = " + e.getValue());
        }
    }

    private void dumpLobMaps(PrintWriter writer, MVStore mv) {
        lobMaps = mv.hasMap("lobData");
        if (!lobMaps) {
            return;
        }
        TransactionStore txStore = new TransactionStore(mv);
        MVMap<Long, byte[]> lobData = LobStorageMap.openLobDataMap(txStore);
        StreamStore streamStore = new StreamStore(lobData);
        MVMap<Long, LobStorageMap.BlobMeta> lobMap = LobStorageMap.openLobMap(txStore);
        writer.println("-- LOB");
        writer.println("CREATE TABLE IF NOT EXISTS " +
                "INFORMATION_SCHEMA.LOB_BLOCKS(" +
                "LOB_ID BIGINT, SEQ INT, DATA VARBINARY, " +
                "PRIMARY KEY(LOB_ID, SEQ));");
        boolean hasErrors = false;
        for (Entry<Long, LobStorageMap.BlobMeta> e : lobMap.entrySet()) {
            long lobId = e.getKey();
            LobStorageMap.BlobMeta value = e.getValue();
            byte[] streamStoreId = value.streamStoreId;
            InputStream in = streamStore.get(streamStoreId);
            int len = 8 * 1024;
            byte[] block = new byte[len];
            try {
                for (int seq = 0;; seq++) {
                    int l = IOUtils.readFully(in, block, block.length);
                    if (l > 0) {
                        writer.print("INSERT INTO INFORMATION_SCHEMA.LOB_BLOCKS " +
                                "VALUES(" + lobId + ", " + seq + ", X'");
                        writer.print(StringUtils.convertBytesToHex(block, l));
                        writer.println("');");
                    }
                    if (l != len) {
                        break;
                    }
                }
            } catch (IOException ex) {
                writeError(writer, ex);
                hasErrors = true;
            }
        }
        writer.println("-- lobMap.size: " + lobMap.sizeAsLong());
        writer.println("-- lobData.size: " + lobData.sizeAsLong());

        if (hasErrors) {
            writer.println("-- lobMap");
            for (Long k : lobMap.keyList()) {
                LobStorageMap.BlobMeta value = lobMap.get(k);
                byte[] streamStoreId = value.streamStoreId;
                writer.println("--     " + k + " " + StreamStore.toString(streamStoreId));
            }
            writer.println("-- lobData");
            for (Long k : lobData.keyList()) {
                writer.println("--     " + k + " len " + lobData.get(k).length);
            }
        }
    }

    private String setStorage(int storageId) {
        this.storageId = storageId;
        this.storageName = "O_" + Integer.toString(storageId).replace('-', 'M');
        return storageName;
    }

    private void writeMetaRow(Row r) {
        MetaRecord meta = new MetaRecord(r);
        int objectType = meta.getObjectType();
        if (objectType == DbObject.INDEX && meta.getSQL().startsWith("CREATE PRIMARY KEY ")) {
            return;
        }
        schema.add(meta);
        if (objectType == DbObject.TABLE_OR_VIEW) {
            tableMap.put(meta.getId(), extractTableOrViewName(meta.getSQL()));
        }
    }

    private void resetSchema() {
        schema = new ArrayList<>();
        objectIdSet = new HashSet<>();
        tableMap = new HashMap<>();
        columnTypeMap = new HashMap<>();
    }

    private void writeSchemaSET(PrintWriter writer) {
        writer.println("---- Schema SET ----");
        for (MetaRecord m : schema) {
            if (m.getObjectType() == DbObject.SETTING) {
                String sql = m.getSQL();
                writer.println(sql + ";");
            }
        }
    }

    private void writeSchema(PrintWriter writer) {
        writer.println("---- Schema ----");
        Collections.sort(schema);
        for (MetaRecord m : schema) {
            if (m.getObjectType() != DbObject.SETTING
                    && !isSchemaObjectTypeDelayed(m)) {
                // create, but not referential integrity constraints and so on
                // because they could fail on duplicate keys
                String sql = m.getSQL();
                writer.println(sql + ";");
            }
        }
        // first, copy the lob storage (if there is any)
        // must occur before copying data,
        // otherwise the lob storage may be overwritten
        boolean deleteLobs = false;
        for (Map.Entry<Integer, String> entry : tableMap.entrySet()) {
            Integer objectId = entry.getKey();
            String name = entry.getValue();
            if (objectIdSet.contains(objectId)) {
                if (isLobTable(name)) {
                    setStorage(objectId);
                    writer.println("DELETE FROM " + name + ";");
                    writer.println("INSERT INTO " + name + " SELECT * FROM " + storageName + ";");
                    if (name.equals("INFORMATION_SCHEMA.LOBS")
                            || name.equalsIgnoreCase("\"INFORMATION_SCHEMA\".\"LOBS\"")) {
                        writer.println("UPDATE " + name + " SET `TABLE` = " +
                                LobStorageFrontend.TABLE_TEMP + ";");
                        deleteLobs = true;
                    }
                }
            }
        }
        for (Map.Entry<Integer, String> entry : tableMap.entrySet()) {
            Integer objectId = entry.getKey();
            String name = entry.getValue();
            if (objectIdSet.contains(objectId)) {
                setStorage(objectId);
                if (isLobTable(name)) {
                    continue;
                }
                writer.println("INSERT INTO " + name + " SELECT * FROM " + storageName + ";");
            }
        }
        for (Integer objectId : objectIdSet) {
            setStorage(objectId);
            writer.println("DROP TABLE " + storageName + ";");
        }
        if (deleteLobs) {
            writer.println("DELETE FROM INFORMATION_SCHEMA.LOBS WHERE `TABLE` = " +
                    LobStorageFrontend.TABLE_TEMP + ";");
        }
        ArrayList<String> referentialConstraints = new ArrayList<>();
        for (MetaRecord m : schema) {
            if (isSchemaObjectTypeDelayed(m)) {
                String sql = m.getSQL();
                // TODO parse SQL properly
                if (m.getObjectType() == DbObject.CONSTRAINT && sql.endsWith("NOCHECK")
                        && sql.contains(" FOREIGN KEY") && sql.contains("REFERENCES ")) {
                    referentialConstraints.add(sql);
                } else {
                    writer.println(sql + ';');
                }
            }
        }
        for (String sql : referentialConstraints) {
            writer.println(sql + ';');
        }
    }

    private static boolean isLobTable(String name) {
        return name.startsWith("INFORMATION_SCHEMA.LOB") || name.startsWith("\"INFORMATION_SCHEMA\".\"LOB")
                || name.startsWith("\"information_schema\".\"lob");
    }

    private static boolean isSchemaObjectTypeDelayed(MetaRecord m) {
        switch (m.getObjectType()) {
        case DbObject.INDEX:
        case DbObject.CONSTRAINT:
        case DbObject.TRIGGER:
            return true;
        }
        return false;
    }

    private void createTemporaryTable(PrintWriter writer) {
        if (!objectIdSet.contains(storageId)) {
            objectIdSet.add(storageId);
            writer.write("CREATE TABLE ");
            writer.write(storageName);
            writer.write('(');
            for (int i = 0; i < recordLength; i++) {
                if (i > 0) {
                    writer.print(", ");
                }
                writer.write('C');
                writer.print(i);
                writer.write(' ');
                String columnType = columnTypeMap.get(storageName + "." + i);
                writer.write(columnType == null ? "VARCHAR" : columnType);
            }
            writer.println(");");
            writer.flush();
        }
    }

    private static String extractTableOrViewName(String sql) {
        int indexTable = sql.indexOf(" TABLE ");
        int indexView = sql.indexOf(" VIEW ");
        if (indexTable > 0 && indexView > 0) {
            if (indexTable < indexView) {
                indexView = -1;
            } else {
                indexTable = -1;
            }
        }
        if (indexView > 0) {
            sql = sql.substring(indexView + " VIEW ".length());
        } else if (indexTable > 0) {
            sql = sql.substring(indexTable + " TABLE ".length());
        } else {
            return "UNKNOWN";
        }
        if (sql.startsWith("IF NOT EXISTS ")) {
            sql = sql.substring("IF NOT EXISTS ".length());
        }
        boolean ignore = false;
        // sql is modified in the loop
        for (int i = 0; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            if (ch == '\"') {
                ignore = !ignore;
            } else if (!ignore && (ch <= ' ' || ch == '(')) {
                sql = sql.substring(0, i);
                return sql;
            }
        }
        return "UNKNOWN";
    }


    private void writeError(PrintWriter writer, Throwable e) {
        if (writer != null) {
            writer.println("// error: " + e);
        }
        traceError("Error", e);
    }

    /**
     * INTERNAL
     */
    @Override
    public String getDatabasePath() {
        return databaseName;
    }

    /**
     * INTERNAL
     */
    @Override
    public FileStore openFile(String name, String mode, boolean mustExist) {
        return FileStore.open(this, name, "rw");
    }

    /**
     * INTERNAL
     */
    @Override
    public void checkPowerOff() {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    @Override
    public void checkWritingAllowed() {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    @Override
    public int getMaxLengthInplaceLob() {
        throw DbException.getInternalError();
    }

    /**
     * INTERNAL
     */
    @Override
    public Object getLobSyncObject() {
        return this;
    }

    /**
     * INTERNAL
     */
    @Override
    public SmallLRUCache<String, String[]> getLobFileListCache() {
        return null;
    }

    /**
     * INTERNAL
     */
    @Override
    public TempFileDeleter getTempFileDeleter() {
        return TempFileDeleter.getInstance();
    }

    /**
     * INTERNAL
     */
    @Override
    public LobStorageInterface getLobStorage() {
        return null;
    }

    /**
     * INTERNAL
     */
    @Override
    public int readLob(long lobId, byte[] hmac, long offset, byte[] buff, int off, int length) {
        throw DbException.getInternalError();
    }

    @Override
    public CompareMode getCompareMode() {
        return CompareMode.getInstance(null, 0);
    }
}
