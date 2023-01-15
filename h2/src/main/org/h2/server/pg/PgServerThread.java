/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.pg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Engine;
import org.h2.engine.SessionLocal;
import org.h2.engine.SysProperties;
import org.h2.expression.ParameterInterface;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.util.DateTimeUtils;
import org.h2.util.MathUtils;
import org.h2.util.NetUtils;
import org.h2.util.NetworkConnectionInfo;
import org.h2.util.ScriptReader;
import org.h2.util.StringUtils;
import org.h2.util.TimeZoneProvider;
import org.h2.util.Utils;
import org.h2.util.Utils10;
import org.h2.value.CaseInsensitiveMap;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBigint;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecfloat;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueReal;
import org.h2.value.ValueSmallint;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimeTimeZone;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;

/**
 * One server thread is opened for each client.
 */
public final class PgServerThread implements Runnable {

    private static final boolean INTEGER_DATE_TYPES = false;

    private static final Pattern SHOULD_QUOTE = Pattern.compile(".*[\",\\\\{}].*");

    private static String pgTimeZone(String value) {
        if (value.startsWith("GMT+")) {
            return convertTimeZone(value, "GMT-");
        } else if (value.startsWith("GMT-")) {
            return convertTimeZone(value, "GMT+");
        } else if (value.startsWith("UTC+")) {
            return convertTimeZone(value, "UTC-");
        } else if (value.startsWith("UTC-")) {
            return convertTimeZone(value, "UTC+");
        } else {
            return value;
        }
    }

    private static String convertTimeZone(String value, String prefix) {
        int length = value.length();
        return new StringBuilder(length).append(prefix).append(value, 4, length).toString();
    }

    private final PgServer server;
    private Socket socket;
    private SessionLocal session;
    private boolean stop;
    private DataInputStream dataInRaw;
    private DataInputStream dataIn;
    private OutputStream out;
    private int messageType;
    private ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    private DataOutputStream dataOut;
    private Thread thread;
    private boolean initDone;
    private String userName;
    private String databaseName;
    private int processId;
    private final int secret;
    private CommandInterface activeRequest;
    private String clientEncoding = SysProperties.PG_DEFAULT_CLIENT_ENCODING;
    private String dateStyle = "ISO, MDY";
    private TimeZoneProvider timeZone = DateTimeUtils.getTimeZone();
    private final HashMap<String, Prepared> prepared =
            new CaseInsensitiveMap<>();
    private final HashMap<String, Portal> portals =
            new CaseInsensitiveMap<>();

    PgServerThread(Socket socket, PgServer server) {
        this.server = server;
        this.socket = socket;
        this.secret = (int) MathUtils.secureRandomLong();
    }

    @Override
    public void run() {
        try {
            server.trace("Connect");
            InputStream ins = socket.getInputStream();
            out = socket.getOutputStream();
            dataInRaw = new DataInputStream(ins);
            while (!stop) {
                process();
                out.flush();
            }
        } catch (EOFException e) {
            // more or less normal disconnect
        } catch (Exception e) {
            server.traceError(e);
        } finally {
            server.trace("Disconnect");
            close();
        }
    }

    private String readString() throws IOException {
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        while (true) {
            int x = dataIn.read();
            if (x <= 0) {
                break;
            }
            buff.write(x);
        }
        return Utils10.byteArrayOutputStreamToString(buff, getEncoding());
    }

    private int readInt() throws IOException {
        return dataIn.readInt();
    }

    private short readShort() throws IOException {
        return dataIn.readShort();
    }

    private byte readByte() throws IOException {
        return dataIn.readByte();
    }

    private void readFully(byte[] buff) throws IOException {
        dataIn.readFully(buff);
    }

    private void process() throws IOException {
        int x;
        if (initDone) {
            x = dataInRaw.read();
            if (x < 0) {
                stop = true;
                return;
            }
        } else {
            x = 0;
        }
        int len = dataInRaw.readInt();
        len -= 4;
        byte[] data = Utils.newBytes(len);
        dataInRaw.readFully(data, 0, len);
        dataIn = new DataInputStream(new ByteArrayInputStream(data, 0, len));
        switch (x) {
        case 0:
            server.trace("Init");
            int version = readInt();
            if (version == 80877102) {
                server.trace("CancelRequest");
                int pid = readInt();
                int key = readInt();
                PgServerThread c = server.getThread(pid);
                if (c != null && key == c.secret) {
                    c.cancelRequest();
                } else {
                    // According to the PostgreSQL documentation, when canceling
                    // a request, if an invalid secret is provided then no
                    // exception should be sent back to the client.
                    server.trace("Invalid CancelRequest: pid=" + pid + ", key=" + key);
                }
                close();
            } else if (version == 80877103) {
                server.trace("SSLRequest");
                out.write('N');
            } else {
                server.trace("StartupMessage");
                server.trace(" version " + version +
                        " (" + (version >> 16) + "." + (version & 0xff) + ")");
                while (true) {
                    String param = readString();
                    if (param.isEmpty()) {
                        break;
                    }
                    String value = readString();
                    switch (param) {
                    case "user":
                        this.userName = value;
                        break;
                    case "database":
                        this.databaseName = server.checkKeyAndGetDatabaseName(value);
                        break;
                    case "client_encoding":
                        // node-postgres will send "'utf-8'"
                        int length = value.length();
                        if (length >= 2 && value.charAt(0) == '\''
                                && value.charAt(length - 1) == '\'') {
                            value = value.substring(1, length - 1);
                        }
                        // UTF8
                        clientEncoding = value;
                        break;
                    case "DateStyle":
                        if (value.indexOf(',') < 0) {
                            value += ", MDY";
                        }
                        dateStyle = value;
                        break;
                    case "TimeZone":
                        try {
                            timeZone = TimeZoneProvider.ofId(pgTimeZone(value));
                        } catch (Exception e) {
                            server.trace("Unknown TimeZone: " + value);
                        }
                        break;
                    }
                    // extra_float_digits 2
                    // geqo on (Genetic Query Optimization)
                    server.trace(" param " + param + "=" + value);
                }
                sendAuthenticationCleartextPassword();
                initDone = true;
            }
            break;
        case 'p': {
            server.trace("PasswordMessage");
            String password = readString();
            try {
                Properties info = new Properties();
                info.put("MODE", "PostgreSQL");
                info.put("DATABASE_TO_LOWER", "TRUE");
                info.put("DEFAULT_NULL_ORDERING", "HIGH");
                String url = "jdbc:h2:" + databaseName;
                ConnectionInfo ci = new ConnectionInfo(url, info, userName, password);
                String baseDir = server.getBaseDir();
                if (baseDir == null) {
                    baseDir = SysProperties.getBaseDir();
                }
                if (baseDir != null) {
                    ci.setBaseDir(baseDir);
                }
                if (server.getIfExists()) {
                    ci.setProperty("FORBID_CREATION", "TRUE");
                }
                ci.setNetworkConnectionInfo(new NetworkConnectionInfo( //
                        NetUtils.ipToShortForm(new StringBuilder("pg://"), //
                                socket.getLocalAddress().getAddress(), true) //
                                .append(':').append(socket.getLocalPort()).toString(), //
                        socket.getInetAddress().getAddress(), socket.getPort(), null));
                session = Engine.createSession(ci);
                initDb();
                sendAuthenticationOk();
            } catch (Exception e) {
                e.printStackTrace();
                stop = true;
            }
            break;
        }
        case 'P': {
            server.trace("Parse");
            Prepared p = new Prepared();
            p.name = readString();
            p.sql = getSQL(readString());
            int paramTypesCount = readShort();
            int[] paramTypes = null;
            if (paramTypesCount > 0) {
                paramTypes = new int[paramTypesCount];
                for (int i = 0; i < paramTypesCount; i++) {
                    paramTypes[i] = readInt();
                }
            }
            try {
                p.prep = session.prepareLocal(p.sql);
                ArrayList<? extends ParameterInterface> parameters = p.prep.getParameters();
                int count = parameters.size();
                p.paramType = new int[count];
                for (int i = 0; i < count; i++) {
                    int type;
                    if (i < paramTypesCount && paramTypes[i] != 0) {
                        type = paramTypes[i];
                        server.checkType(type);
                    } else {
                        type = PgServer.convertType(parameters.get(i).getType());
                    }
                    p.paramType[i] = type;
                }
                prepared.put(p.name, p);
                sendParseComplete();
            } catch (Exception e) {
                sendErrorResponse(e);
            }
            break;
        }
        case 'B': {
            server.trace("Bind");
            Portal portal = new Portal();
            portal.name = readString();
            String prepName = readString();
            Prepared prep = prepared.get(prepName);
            if (prep == null) {
                sendErrorResponse("Prepared not found");
                break;
            }
            portal.prep = prep;
            portals.put(portal.name, portal);
            int formatCodeCount = readShort();
            int[] formatCodes = new int[formatCodeCount];
            for (int i = 0; i < formatCodeCount; i++) {
                formatCodes[i] = readShort();
            }
            int paramCount = readShort();
            try {
                ArrayList<? extends ParameterInterface> parameters = prep.prep.getParameters();
                for (int i = 0; i < paramCount; i++) {
                    setParameter(parameters, prep.paramType[i], i, formatCodes);
                }
            } catch (Exception e) {
                sendErrorResponse(e);
                break;
            }
            int resultCodeCount = readShort();
            portal.resultColumnFormat = new int[resultCodeCount];
            for (int i = 0; i < resultCodeCount; i++) {
                portal.resultColumnFormat[i] = readShort();
            }
            sendBindComplete();
            break;
        }
        case 'C': {
            char type = (char) readByte();
            String name = readString();
            server.trace("Close");
            if (type == 'S') {
                Prepared p = prepared.remove(name);
                if (p != null) {
                    p.close();
                }
            } else if (type == 'P') {
                Portal p = portals.remove(name);
                if (p != null) {
                    p.prep.closeResult();
                }
            } else {
                server.trace("expected S or P, got " + type);
                sendErrorResponse("expected S or P");
                break;
            }
            sendCloseComplete();
            break;
        }
        case 'D': {
            char type = (char) readByte();
            String name = readString();
            server.trace("Describe");
            if (type == 'S') {
                Prepared p = prepared.get(name);
                if (p == null) {
                    sendErrorResponse("Prepared not found: " + name);
                } else {
                    try {
                        sendParameterDescription(p.prep.getParameters(), p.paramType);
                        sendRowDescription(p.prep.getMetaData(), null);
                    } catch (Exception e) {
                        sendErrorResponse(e);
                    }
                }
            } else if (type == 'P') {
                Portal p = portals.get(name);
                if (p == null) {
                    sendErrorResponse("Portal not found: " + name);
                } else {
                    CommandInterface prep = p.prep.prep;
                    try {
                        sendRowDescription(prep.getMetaData(), p.resultColumnFormat);
                    } catch (Exception e) {
                        sendErrorResponse(e);
                    }
                }
            } else {
                server.trace("expected S or P, got " + type);
                sendErrorResponse("expected S or P");
            }
            break;
        }
        case 'E': {
            String name = readString();
            server.trace("Execute");
            Portal p = portals.get(name);
            if (p == null) {
                sendErrorResponse("Portal not found: " + name);
                break;
            }
            int maxRows = readInt();
            Prepared prepared = p.prep;
            CommandInterface prep = prepared.prep;
            server.trace(prepared.sql);
            try {
                setActiveRequest(prep);
                if (prep.isQuery()) {
                    executeQuery(prepared, prep, p.resultColumnFormat, maxRows);
                } else {
                    sendCommandComplete(prep, prep.executeUpdate(null).getUpdateCount());
                }
            } catch (Exception e) {
                sendErrorOrCancelResponse(e);
            } finally {
                setActiveRequest(null);
            }
            break;
        }
        case 'S': {
            server.trace("Sync");
            sendReadyForQuery();
            break;
        }
        case 'Q': {
            server.trace("Query");
            String query = readString();
            @SuppressWarnings("resource")
            ScriptReader reader = new ScriptReader(new StringReader(query));
            while (true) {
                String s = reader.readStatement();
                if (s == null) {
                    break;
                }
                s = getSQL(s);
                try (CommandInterface command = session.prepareLocal(s)) {
                    setActiveRequest(command);
                    if (command.isQuery()) {
                        try (ResultInterface result = command.executeQuery(0, false)) {
                            sendRowDescription(result, null);
                            while (result.next()) {
                                sendDataRow(result, null);
                            }
                            sendCommandComplete(command, 0);
                        }
                    } else {
                        sendCommandComplete(command, command.executeUpdate(null).getUpdateCount());
                    }
                } catch (Exception e) {
                    sendErrorOrCancelResponse(e);
                    break;
                } finally {
                    setActiveRequest(null);
                }
            }
            sendReadyForQuery();
            break;
        }
        case 'X': {
            server.trace("Terminate");
            close();
            break;
        }
        default:
            server.trace("Unsupported: " + x + " (" + (char) x + ")");
            break;
        }
    }

    private void executeQuery(Prepared prepared, CommandInterface prep, int[] resultColumnFormat, int maxRows)
            throws Exception {
        ResultInterface result = prepared.result;
        if (result == null) {
            result = prep.executeQuery(0L, false);
        }
        try {
            // the meta-data is sent in the prior 'Describe'
            if (maxRows == 0) {
                while (result.next()) {
                    sendDataRow(result, resultColumnFormat);
                }
            } else {
                for (; maxRows > 0 && result.next(); maxRows--) {
                    sendDataRow(result, resultColumnFormat);
                }
                if (result.hasNext()) {
                    prepared.result = result;
                    sendCommandSuspended();
                    return;
                }
            }
            prepared.closeResult();
            sendCommandComplete(prep, 0);
        } catch (Exception e) {
            prepared.closeResult();
            throw e;
        }
    }

    private String getSQL(String s) {
        String lower = StringUtils.toLowerEnglish(s);
        if (lower.startsWith("show max_identifier_length")) {
            s = "CALL 63";
        } else if (lower.startsWith("set client_encoding to")) {
            s = "set DATESTYLE ISO";
        }
        // s = StringUtils.replaceAll(s, "i.indkey[ia.attnum-1]", "0");
        if (server.getTrace()) {
            server.trace(s + ";");
        }
        return s;
    }

    private void sendCommandComplete(CommandInterface command, long updateCount) throws IOException {
        startMessage('C');
        switch (command.getCommandType()) {
        case CommandInterface.INSERT:
            writeStringPart("INSERT 0 ");
            writeString(Long.toString(updateCount));
            break;
        case CommandInterface.UPDATE:
            writeStringPart("UPDATE ");
            writeString(Long.toString(updateCount));
            break;
        case CommandInterface.DELETE:
            writeStringPart("DELETE ");
            writeString(Long.toString(updateCount));
            break;
        case CommandInterface.SELECT:
        case CommandInterface.CALL:
            writeString("SELECT");
            break;
        case CommandInterface.BEGIN:
            writeString("BEGIN");
            break;
        default:
            server.trace("check CommandComplete tag for command " + command);
            writeStringPart("UPDATE ");
            writeString(Long.toString(updateCount));
        }
        sendMessage();
    }

    private void sendCommandSuspended() throws IOException {
        startMessage('s');
        sendMessage();
    }

    private void sendDataRow(ResultInterface result, int[] formatCodes) throws IOException {
        int columns = result.getVisibleColumnCount();
        startMessage('D');
        writeShort(columns);
        Value[] row = result.currentRow();
        for (int i = 0; i < columns; i++) {
            int pgType = PgServer.convertType(result.getColumnType(i));
            boolean text = formatAsText(pgType, formatCodes, i);
            writeDataColumn(row[i], pgType, text);
        }
        sendMessage();
    }

    private static long toPostgreDays(long dateValue) {
        return DateTimeUtils.absoluteDayFromDateValue(dateValue) - 10_957;
    }

    private void writeDataColumn(Value v, int pgType, boolean text) throws IOException {
        if (v == ValueNull.INSTANCE) {
            writeInt(-1);
            return;
        }
        if (text) {
            // plain text
            switch (pgType) {
            case PgServer.PG_TYPE_BOOL:
                writeInt(1);
                dataOut.writeByte(v.getBoolean() ? 't' : 'f');
                break;
            case PgServer.PG_TYPE_BYTEA: {
                byte[] bytes = v.getBytesNoCopy();
                int length = bytes.length;
                int cnt = length;
                for (int i = 0; i < length; i++) {
                    byte b = bytes[i];
                    if (b < 32 || b > 126) {
                        cnt += 3;
                    } else if (b == 92) {
                        cnt++;
                    }
                }
                byte[] data = new byte[cnt];
                for (int i = 0, j = 0; i < length; i++) {
                    byte b = bytes[i];
                    if (b < 32 || b > 126) {
                        data[j++] = '\\';
                        data[j++] = (byte) (((b >>> 6) & 3) + '0');
                        data[j++] = (byte) (((b >>> 3) & 7) + '0');
                        data[j++] = (byte) ((b & 7) + '0');
                    } else if (b == 92) {
                        data[j++] = '\\';
                        data[j++] = '\\';
                    } else {
                        data[j++] = b;
                    }
                }
                writeInt(data.length);
                write(data);
                break;
            }
            case PgServer.PG_TYPE_INT2_ARRAY:
            case PgServer.PG_TYPE_INT4_ARRAY:
            case PgServer.PG_TYPE_VARCHAR_ARRAY:
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                baos.write('{');
                Value[] values = ((ValueArray) v).getList();
                Charset encoding = getEncoding();
                for (int i = 0; i < values.length; i++) {
                    if (i > 0) {
                        baos.write(',');
                    }
                    String s = values[i].getString();
                    if (SHOULD_QUOTE.matcher(s).matches()) {
                        List<String> ss = new ArrayList<>();
                        for (String s0 : s.split("\\\\")) {
                            ss.add(s0.replace("\"", "\\\""));
                        }
                        s = "\"" + String.join("\\\\", ss) + "\"";
                    }
                    baos.write(s.getBytes(encoding));
                }
                baos.write('}');
                writeInt(baos.size());
                write(baos);
                break;
            default:
                byte[] data = v.getString().getBytes(getEncoding());
                writeInt(data.length);
                write(data);
            }
        } else {
            // binary
            switch (pgType) {
            case PgServer.PG_TYPE_BOOL:
                writeInt(1);
                dataOut.writeByte(v.getBoolean() ? 1 : 0);
                break;
            case PgServer.PG_TYPE_INT2:
                writeInt(2);
                writeShort(v.getShort());
                break;
            case PgServer.PG_TYPE_INT4:
                writeInt(4);
                writeInt(v.getInt());
                break;
            case PgServer.PG_TYPE_INT8:
                writeInt(8);
                dataOut.writeLong(v.getLong());
                break;
            case PgServer.PG_TYPE_FLOAT4:
                writeInt(4);
                dataOut.writeFloat(v.getFloat());
                break;
            case PgServer.PG_TYPE_FLOAT8:
                writeInt(8);
                dataOut.writeDouble(v.getDouble());
                break;
            case PgServer.PG_TYPE_NUMERIC:
                writeNumericBinary(v.getBigDecimal());
                break;
            case PgServer.PG_TYPE_BYTEA: {
                byte[] data = v.getBytesNoCopy();
                writeInt(data.length);
                write(data);
                break;
            }
            case PgServer.PG_TYPE_DATE:
                writeInt(4);
                writeInt((int) toPostgreDays(((ValueDate) v).getDateValue()));
                break;
            case PgServer.PG_TYPE_TIME:
                writeTimeBinary(((ValueTime) v).getNanos(), 8);
                break;
            case PgServer.PG_TYPE_TIMETZ: {
                ValueTimeTimeZone t = (ValueTimeTimeZone) v;
                long m = t.getNanos();
                writeTimeBinary(m, 12);
                dataOut.writeInt(-t.getTimeZoneOffsetSeconds());
                break;
            }
            case PgServer.PG_TYPE_TIMESTAMP: {
                ValueTimestamp t = (ValueTimestamp) v;
                long m = toPostgreDays(t.getDateValue()) * 86_400;
                long nanos = t.getTimeNanos();
                writeTimestampBinary(m, nanos);
                break;
            }
            case PgServer.PG_TYPE_TIMESTAMPTZ: {
                ValueTimestampTimeZone t = (ValueTimestampTimeZone) v;
                long m = toPostgreDays(t.getDateValue()) * 86_400;
                long nanos = t.getTimeNanos() - t.getTimeZoneOffsetSeconds() * 1_000_000_000L;
                if (nanos < 0L) {
                    m--;
                    nanos += DateTimeUtils.NANOS_PER_DAY;
                }
                writeTimestampBinary(m, nanos);
                break;
            }
            default: throw new IllegalStateException("output binary format is undefined");
            }
        }
    }

    private static final int[] POWERS10 = {1, 10, 100, 1000, 10000};
    private static final int MAX_GROUP_SCALE = 4;
    private static final int MAX_GROUP_SIZE = POWERS10[4];
    private static final short NUMERIC_POSITIVE = 0x0000;
    private static final short NUMERIC_NEGATIVE = 0x4000;
    private static final short NUMERIC_NAN = (short) 0xC000;
    private static final BigInteger NUMERIC_CHUNK_MULTIPLIER = BigInteger.valueOf(10_000L);

    private static int divide(BigInteger[] unscaled, int divisor) {
        BigInteger[] bi = unscaled[0].divideAndRemainder(BigInteger.valueOf(divisor));
        unscaled[0] = bi[0];
        return bi[1].intValue();
    }

    // https://www.npgsql.org/dev/types.html
    // https://github.com/npgsql/npgsql/blob/8a479081f707784b5040747b23102c3d6371b9d3/
    //         src/Npgsql/TypeHandlers/NumericHandlers/NumericHandler.cs#L166
    private void writeNumericBinary(BigDecimal value) throws IOException {
        int weight = 0;
        List<Integer> groups = new ArrayList<>();
        int scale = value.scale();
        int signum = value.signum();
        if (signum != 0) {
            BigInteger[] unscaled = {null};
            if (scale < 0) {
                unscaled[0] = value.setScale(0).unscaledValue();
                scale = 0;
            } else {
                unscaled[0] = value.unscaledValue();
            }
            if (signum < 0) {
                unscaled[0] = unscaled[0].negate();
            }
            weight = -scale / MAX_GROUP_SCALE - 1;
            int remainder = 0;
            int scaleChunk = scale % MAX_GROUP_SCALE;
            if (scaleChunk > 0) {
                remainder = divide(unscaled, POWERS10[scaleChunk]) * POWERS10[MAX_GROUP_SCALE - scaleChunk];
                if (remainder != 0) {
                    weight--;
                }
            }
            if (remainder == 0) {
                while ((remainder = divide(unscaled, MAX_GROUP_SIZE)) == 0) {
                    weight++;
                }
            }
            groups.add(remainder);
            while (unscaled[0].signum() != 0) {
                groups.add(divide(unscaled, MAX_GROUP_SIZE));
            }
        }
        int groupCount = groups.size();
        if (groupCount + weight > Short.MAX_VALUE || scale > Short.MAX_VALUE) {
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, value.toString());
        }
        writeInt(8 + groupCount * 2);
        writeShort(groupCount);
        writeShort(groupCount + weight);
        writeShort(signum < 0 ? NUMERIC_NEGATIVE : NUMERIC_POSITIVE);
        writeShort(scale);
        for (int i = groupCount - 1; i >= 0; i--) {
            writeShort(groups.get(i));
        }
    }

    private void writeTimeBinary(long m, int numBytes) throws IOException {
        writeInt(numBytes);
        if (INTEGER_DATE_TYPES) {
            // long format
            m /= 1_000;
        } else {
            // double format
            m = Double.doubleToLongBits(m * 0.000_000_001);
        }
        dataOut.writeLong(m);
    }

    private void writeTimestampBinary(long m, long nanos) throws IOException {
        writeInt(8);
        if (INTEGER_DATE_TYPES) {
            // long format
            m = m * 1_000_000 + nanos / 1_000;
        } else {
            // double format
            m = Double.doubleToLongBits(m + nanos * 0.000_000_001);
        }
        dataOut.writeLong(m);
    }

    private Charset getEncoding() {
        if ("UNICODE".equals(clientEncoding)) {
            return StandardCharsets.UTF_8;
        }
        return Charset.forName(clientEncoding);
    }

    private void setParameter(ArrayList<? extends ParameterInterface> parameters, int pgType, int i, int[] formatCodes)
            throws IOException {
        boolean text = true;
        if (formatCodes.length == 1) {
            text = formatCodes[0] == 0;
        } else if (i < formatCodes.length) {
            text = formatCodes[i] == 0;
        }
        int paramLen = readInt();
        Value value;
        if (paramLen == -1) {
            value = ValueNull.INSTANCE;
        } else if (text) {
            // plain text
            byte[] data = Utils.newBytes(paramLen);
            readFully(data);
            String str = new String(data, getEncoding());
            switch (pgType) {
            case PgServer.PG_TYPE_DATE: {
                // Strip timezone offset
                int idx = str.indexOf(' ');
                if (idx > 0) {
                    str = str.substring(0, idx);
                }
                break;
            }
            case PgServer.PG_TYPE_TIME: {
                // Strip timezone offset
                int idx = str.indexOf('+');
                if (idx <= 0) {
                    idx = str.indexOf('-');
                }
                if (idx > 0) {
                    str = str.substring(0, idx);
                }
                break;
            }
            }
            value = ValueVarchar.get(str, session);
        } else {
            // binary
            switch (pgType) {
            case PgServer.PG_TYPE_INT2:
                checkParamLength(2, paramLen);
                value = ValueSmallint.get(readShort());
                break;
            case PgServer.PG_TYPE_INT4:
                checkParamLength(4, paramLen);
                value = ValueInteger.get(readInt());
                break;
            case PgServer.PG_TYPE_INT8:
                checkParamLength(8, paramLen);
                value = ValueBigint.get(dataIn.readLong());
                break;
            case PgServer.PG_TYPE_FLOAT4:
                checkParamLength(4, paramLen);
                value = ValueReal.get(dataIn.readFloat());
                break;
            case PgServer.PG_TYPE_FLOAT8:
                checkParamLength(8, paramLen);
                value = ValueDouble.get(dataIn.readDouble());
                break;
            case PgServer.PG_TYPE_BYTEA: {
                byte[] d = Utils.newBytes(paramLen);
                readFully(d);
                value = ValueVarbinary.getNoCopy(d);
                break;
            }
            case PgServer.PG_TYPE_NUMERIC:
                value = readNumericBinary(paramLen);
                break;
            default:
                server.trace("Binary format for type: "+pgType+" is unsupported");
                byte[] d = Utils.newBytes(paramLen);
                readFully(d);
                value = ValueVarchar.get(new String(d, getEncoding()), session);
            }
        }
        parameters.get(i).setValue(value, true);
    }

    private static void checkParamLength(int expected, int got) {
        if (expected != got) {
            throw DbException.getInvalidValueException("paramLen", got);
        }
    }

    private Value readNumericBinary(int paramLen) throws IOException {
        if (paramLen < 8) {
            throw DbException.getInvalidValueException("numeric binary length", paramLen);
        }
        short len = readShort();
        short weight = readShort();
        short sign = readShort();
        short scale = readShort();
        if (len * 2 + 8 != paramLen) {
            throw DbException.getInvalidValueException("numeric binary length", paramLen);
        }
        if (sign == NUMERIC_NAN) {
            return ValueDecfloat.NAN;
        }
        if (sign != NUMERIC_POSITIVE && sign != NUMERIC_NEGATIVE) {
            throw DbException.getInvalidValueException("numeric sign", sign);
        }
        if ((scale & 0x3FFF) != scale) {
            throw DbException.getInvalidValueException("numeric scale", scale);
        }
        if (len == 0) {
            return scale == 0 ? ValueNumeric.ZERO : ValueNumeric.get(new BigDecimal(BigInteger.ZERO, scale));
        }
        BigInteger n = BigInteger.ZERO;
        for (int i = 0; i < len; i++) {
            short c = readShort();
            if (c < 0 || c > 9_999) {
                throw DbException.getInvalidValueException("numeric chunk", c);
            }
            n = n.multiply(NUMERIC_CHUNK_MULTIPLIER).add(BigInteger.valueOf(c));
        }
        if (sign != NUMERIC_POSITIVE) {
            n = n.negate();
        }
        return ValueNumeric.get(new BigDecimal(n, (len - weight - 1) * 4).setScale(scale));
    }

    private void sendErrorOrCancelResponse(Exception e) throws IOException {
        if (e instanceof DbException && ((DbException) e).getErrorCode() == ErrorCode.STATEMENT_WAS_CANCELED) {
            sendCancelQueryResponse();
        } else {
            sendErrorResponse(e);
        }
    }

    private void sendErrorResponse(Exception re) throws IOException {
        SQLException e = DbException.toSQLException(re);
        server.traceError(e);
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        writeString(e.getSQLState());
        write('M');
        writeString(e.getMessage());
        write('D');
        writeString(e.toString());
        write(0);
        sendMessage();
    }

    private void sendCancelQueryResponse() throws IOException {
        server.trace("CancelSuccessResponse");
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        writeString("57014");
        write('M');
        writeString("canceling statement due to user request");
        write(0);
        sendMessage();
    }

    private void sendParameterDescription(ArrayList<? extends ParameterInterface> parameters, int[] paramTypes)
            throws Exception {
        int count = parameters.size();
        startMessage('t');
        writeShort(count);
        for (int i = 0; i < count; i++) {
            int type;
            if (paramTypes != null && paramTypes[i] != 0) {
                type = paramTypes[i];
            } else {
                type = PgServer.PG_TYPE_VARCHAR;
            }
            server.checkType(type);
            writeInt(type);
        }
        sendMessage();
    }

    private void sendNoData() throws IOException {
        startMessage('n');
        sendMessage();
    }

    private void sendRowDescription(ResultInterface result, int[] formatCodes) throws IOException {
        if (result == null) {
            sendNoData();
        } else {
            int columns = result.getVisibleColumnCount();
            int[] oids = new int[columns];
            int[] attnums = new int[columns];
            int[] types = new int[columns];
            int[] precision = new int[columns];
            String[] names = new String[columns];
            Database database = session.getDatabase();
            for (int i = 0; i < columns; i++) {
                String name = result.getColumnName(i);
                Schema schema = database.findSchema(result.getSchemaName(i));
                if (schema != null) {
                    Table table = schema.findTableOrView(session, result.getTableName(i));
                    if (table != null) {
                        oids[i] = table.getId();
                        Column column = table.findColumn(name);
                        if (column != null) {
                            attnums[i] = column.getColumnId() + 1;
                        }
                    }
                }
                names[i] = name;
                TypeInfo type = result.getColumnType(i);
                int pgType = PgServer.convertType(type);
                // the ODBC client needs the column pg_catalog.pg_index
                // to be of type 'int2vector'
                // if (name.equalsIgnoreCase("indkey") &&
                //         "pg_index".equalsIgnoreCase(
                //         meta.getTableName(i + 1))) {
                //     type = PgServer.PG_TYPE_INT2VECTOR;
                // }
                precision[i] = type.getDisplaySize();
                if (type.getValueType() != Value.NULL) {
                    server.checkType(pgType);
                }
                types[i] = pgType;
            }
            startMessage('T');
            writeShort(columns);
            for (int i = 0; i < columns; i++) {
                writeString(StringUtils.toLowerEnglish(names[i]));
                // object ID
                writeInt(oids[i]);
                // attribute number of the column
                writeShort(attnums[i]);
                // data type
                writeInt(types[i]);
                // pg_type.typlen
                writeShort(getTypeSize(types[i], precision[i]));
                // pg_attribute.atttypmod
                writeInt(-1);
                // the format type: text = 0, binary = 1
                writeShort(formatAsText(types[i], formatCodes, i) ? 0 : 1);
            }
            sendMessage();
        }
    }

    /**
     * Check whether the given type should be formatted as text.
     *
     * @param pgType data type
     * @param formatCodes format codes, or {@code null}
     * @param column 0-based column number
     * @return true for text
     */
    private static boolean formatAsText(int pgType, int[] formatCodes, int column) {
        boolean text = true;
        if (formatCodes != null && formatCodes.length > 0) {
            if (formatCodes.length == 1) {
                text = formatCodes[0] == 0;
            } else if (column < formatCodes.length) {
                text = formatCodes[column] == 0;
            }
        }
        return text;
    }

    private static int getTypeSize(int pgType, int precision) {
        switch (pgType) {
        case PgServer.PG_TYPE_BOOL:
            return 1;
        case PgServer.PG_TYPE_VARCHAR:
            return Math.max(255, precision + 10);
        default:
            return precision + 4;
        }
    }

    private void sendErrorResponse(String message) throws IOException {
        server.trace("Exception: " + message);
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        // PROTOCOL VIOLATION
        writeString("08P01");
        write('M');
        writeString(message);
        sendMessage();
    }

    private void sendParseComplete() throws IOException {
        startMessage('1');
        sendMessage();
    }

    private void sendBindComplete() throws IOException {
        startMessage('2');
        sendMessage();
    }

    private void sendCloseComplete() throws IOException {
        startMessage('3');
        sendMessage();
    }

    private void initDb() {
        session.setTimeZone(timeZone);
        try (CommandInterface command = session.prepareLocal("set search_path = public, pg_catalog")) {
            command.executeUpdate(null);
        }
        HashSet<Integer> typeSet = server.getTypeSet();
        if (typeSet.isEmpty()) {
            try (CommandInterface command = session.prepareLocal("select oid from pg_catalog.pg_type");
                    ResultInterface result = command.executeQuery(0, false)) {
                while (result.next()) {
                    typeSet.add(result.currentRow()[0].getInt());
                }
            }
        }
    }

    /**
     * Close this connection.
     */
    void close() {
        for (Prepared prep : prepared.values()) {
            prep.close();
        }
        try {
            stop = true;
            try {
                session.close();
            } catch (Exception e) {
                // Ignore
            }
            if (socket != null) {
                socket.close();
            }
            server.trace("Close");
        } catch (Exception e) {
            server.traceError(e);
        }
        session = null;
        socket = null;
        server.remove(this);
    }

    private void sendAuthenticationCleartextPassword() throws IOException {
        startMessage('R');
        writeInt(3);
        sendMessage();
    }

    private void sendAuthenticationOk() throws IOException {
        startMessage('R');
        writeInt(0);
        sendMessage();
        sendParameterStatus("client_encoding", clientEncoding);
        sendParameterStatus("DateStyle", dateStyle);
        sendParameterStatus("is_superuser", "off");
        sendParameterStatus("server_encoding", "SQL_ASCII");
        sendParameterStatus("server_version", Constants.PG_VERSION);
        sendParameterStatus("session_authorization", userName);
        sendParameterStatus("standard_conforming_strings", "off");
        sendParameterStatus("TimeZone", pgTimeZone(timeZone.getId()));
        // Don't inline, see https://bugs.eclipse.org/bugs/show_bug.cgi?id=569498
        String value = INTEGER_DATE_TYPES ? "on" : "off";
        sendParameterStatus("integer_datetimes", value);
        sendBackendKeyData();
        sendReadyForQuery();
    }

    private void sendReadyForQuery() throws IOException {
        startMessage('Z');
        write((byte) (session.getAutoCommit() ? /* idle */ 'I' : /* in a transaction block */ 'T'));
        sendMessage();
    }

    private void sendBackendKeyData() throws IOException {
        startMessage('K');
        writeInt(processId);
        writeInt(secret);
        sendMessage();
    }

    private void writeString(String s) throws IOException {
        writeStringPart(s);
        write(0);
    }

    private void writeStringPart(String s) throws IOException {
        write(s.getBytes(getEncoding()));
    }

    private void writeInt(int i) throws IOException {
        dataOut.writeInt(i);
    }

    private void writeShort(int i) throws IOException {
        dataOut.writeShort(i);
    }

    private void write(byte[] data) throws IOException {
        dataOut.write(data);
    }

    private void write(ByteArrayOutputStream baos) throws IOException {
        baos.writeTo(dataOut);
    }

    private void write(int b) throws IOException {
        dataOut.write(b);
    }

    private void startMessage(int newMessageType) {
        this.messageType = newMessageType;
        if (outBuffer.size() <= 65_536) {
            outBuffer.reset();
        } else {
            outBuffer = new ByteArrayOutputStream();
        }
        dataOut = new DataOutputStream(outBuffer);
    }

    private void sendMessage() throws IOException {
        dataOut.flush();
        dataOut = new DataOutputStream(out);
        write(messageType);
        writeInt(outBuffer.size() + 4);
        write(outBuffer);
        dataOut.flush();
    }

    private void sendParameterStatus(String param, String value)
            throws IOException {
        startMessage('S');
        writeString(param);
        writeString(value);
        sendMessage();
    }

    void setThread(Thread thread) {
        this.thread = thread;
    }

    Thread getThread() {
        return thread;
    }

    void setProcessId(int id) {
        this.processId = id;
    }

    int getProcessId() {
        return this.processId;
    }

    private synchronized void setActiveRequest(CommandInterface statement) {
        activeRequest = statement;
    }

    /**
     * Kill a currently running query on this thread.
     */
    private synchronized void cancelRequest() {
        if (activeRequest != null) {
            activeRequest.cancel();
            activeRequest = null;
        }
    }

    /**
     * Represents a PostgreSQL Prepared object.
     */
    static class Prepared {

        /**
         * The object name.
         */
        String name;

        /**
         * The SQL statement.
         */
        String sql;

        /**
         * The prepared statement.
         */
        CommandInterface prep;

        /**
         * The current result (for suspended portal).
         */
        ResultInterface result;

        /**
         * The list of parameter types (if set).
         */
        int[] paramType;

        /**
         * Closes prepared statement and result, if any.
         */
        void close() {
            try {
                closeResult();
                prep.close();
            } catch (Exception e) {
                // Ignore
            }
        }

        /**
         * Closes the result, if any.
         */
        void closeResult() {
            ResultInterface result = this.result;
            if (result != null) {
                this.result = null;
                result.close();
            }
        }
    }

    /**
     * Represents a PostgreSQL Portal object.
     */
    static class Portal {

        /**
         * The portal name.
         */
        String name;

        /**
         * The format used in the result set columns (if set).
         */
        int[] resultColumnFormat;

        /**
         * The prepared object.
         */
        Prepared prep;
    }
}
