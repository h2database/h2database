/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import static org.h2.util.Bits.LONG_VH_BE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Properties;

import javax.naming.Context;
import javax.sql.DataSource;

import org.h2.api.ErrorCode;
import org.h2.api.JavaObjectSerializer;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.message.DbException;
import org.h2.tools.SimpleResultSet;
import org.h2.util.Utils.ClassFactory;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueToObjectConverter;
import org.h2.value.ValueUuid;

/**
 * This is a utility class with JDBC helper functions.
 */
public class JdbcUtils {

    /**
     * The serializer to use.
     */
    public static JavaObjectSerializer serializer;

    private static final String[] DRIVERS = {
        "h2:", "org.h2.Driver",
        "Cache:", "com.intersys.jdbc.CacheDriver",
        "daffodilDB://", "in.co.daffodil.db.rmi.RmiDaffodilDBDriver",
        "daffodil", "in.co.daffodil.db.jdbc.DaffodilDBDriver",
        "db2:", "com.ibm.db2.jcc.DB2Driver",
        "derby:net:", "org.apache.derby.client.ClientAutoloadedDriver",
        "derby://", "org.apache.derby.client.ClientAutoloadedDriver",
        "derby:", "org.apache.derby.iapi.jdbc.AutoloadedDriver",
        "FrontBase:", "com.frontbase.jdbc.FBJDriver",
        "firebirdsql:", "org.firebirdsql.jdbc.FBDriver",
        "hsqldb:", "org.hsqldb.jdbcDriver",
        "informix-sqli:", "com.informix.jdbc.IfxDriver",
        "jtds:", "net.sourceforge.jtds.jdbc.Driver",
        "microsoft:", "com.microsoft.jdbc.sqlserver.SQLServerDriver",
        "mimer:", "com.mimer.jdbc.Driver",
        "mysql:", "com.mysql.cj.jdbc.Driver",
        "mariadb:", "org.mariadb.jdbc.Driver",
        "odbc:", "sun.jdbc.odbc.JdbcOdbcDriver",
        "oracle:", "oracle.jdbc.driver.OracleDriver",
        "pervasive:", "com.pervasive.jdbc.v2.Driver",
        "pointbase:micro:", "com.pointbase.me.jdbc.jdbcDriver",
        "pointbase:", "com.pointbase.jdbc.jdbcUniversalDriver",
        "postgresql:", "org.postgresql.Driver",
        "sybase:", "com.sybase.jdbc3.jdbc.SybDriver",
        "sqlserver:", "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "teradata:", "com.ncr.teradata.TeraDriver",
    };

    private static final byte[] UUID_PREFIX =
            "\254\355\0\5sr\0\16java.util.UUID\274\231\3\367\230m\205/\2\0\2J\0\14leastSigBitsJ\0\13mostSigBitsxp"
            .getBytes(StandardCharsets.ISO_8859_1);

    private static boolean allowAllClasses;
    private static HashSet<String> allowedClassNames;

    /**
     *  In order to manage more than one class loader
     */
    private static final ArrayList<ClassFactory> userClassFactories = new ArrayList<>();

    private static String[] allowedClassNamePrefixes;

    private JdbcUtils() {
        // utility class
    }

    /**
     * Add a class factory in order to manage more than one class loader.
     *
     * @param classFactory An object that implements ClassFactory
     */
    public static void addClassFactory(ClassFactory classFactory) {
        userClassFactories.add(classFactory);
    }

    /**
     * Remove a class factory
     *
     * @param classFactory Already inserted class factory instance
     */
    public static void removeClassFactory(ClassFactory classFactory) {
        userClassFactories.remove(classFactory);
    }

    static {
        String clazz = SysProperties.JAVA_OBJECT_SERIALIZER;
        if (clazz != null) {
            try {
                serializer = (JavaObjectSerializer) loadUserClass(clazz).getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
    }

    /**
     * Load a class, but check if it is allowed to load this class first. To
     * perform access rights checking, the system property h2.allowedClasses
     * needs to be set to a list of class file name prefixes.
     *
     * @param <Z> generic return type
     * @param className the name of the class
     * @return the class object
     */
    @SuppressWarnings("unchecked")
    public static <Z> Class<Z> loadUserClass(String className) {
        if (allowedClassNames == null) {
            // initialize the static fields
            String s = SysProperties.ALLOWED_CLASSES;
            ArrayList<String> prefixes = new ArrayList<>();
            boolean allowAll = false;
            HashSet<String> classNames = new HashSet<>();
            for (String p : StringUtils.arraySplit(s, ',', true)) {
                if (p.equals("*")) {
                    allowAll = true;
                } else if (p.endsWith("*")) {
                    prefixes.add(p.substring(0, p.length() - 1));
                } else {
                    classNames.add(p);
                }
            }
            allowedClassNamePrefixes = prefixes.toArray(new String[0]);
            allowAllClasses = allowAll;
            allowedClassNames = classNames;
        }
        if (!allowAllClasses && !allowedClassNames.contains(className)) {
            boolean allowed = false;
            for (String s : allowedClassNamePrefixes) {
                if (className.startsWith(s)) {
                    allowed = true;
                    break;
                }
            }
            if (!allowed) {
                throw DbException.get(
                        ErrorCode.ACCESS_DENIED_TO_CLASS_1, className);
            }
        }
        // Use provided class factory first.
        for (ClassFactory classFactory : userClassFactories) {
            if (classFactory.match(className)) {
                try {
                    Class<?> userClass = classFactory.loadClass(className);
                    if (userClass != null) {
                        return (Class<Z>) userClass;
                    }
                } catch (Exception e) {
                    throw DbException.get(
                            ErrorCode.CLASS_NOT_FOUND_1, e, className);
                }
            }
        }
        // Use local ClassLoader
        try {
            return (Class<Z>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            try {
                return (Class<Z>) Class.forName(
                        className, true,
                        Thread.currentThread().getContextClassLoader());
            } catch (Exception e2) {
                throw DbException.get(
                        ErrorCode.CLASS_NOT_FOUND_1, e, className);
            }
        } catch (NoClassDefFoundError e) {
            throw DbException.get(
                    ErrorCode.CLASS_NOT_FOUND_1, e, className);
        } catch (Error e) {
            // UnsupportedClassVersionError
            throw DbException.get(
                    ErrorCode.GENERAL_ERROR_1, e, className);
        }
    }

    /**
     * Close a statement without throwing an exception.
     *
     * @param stat the statement or null
     */
    public static void closeSilently(Statement stat) {
        if (stat != null) {
            try {
                stat.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Close a connection without throwing an exception.
     *
     * @param conn the connection or null
     */
    public static void closeSilently(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Close a result set without throwing an exception.
     *
     * @param rs the result set or null
     */
    public static void closeSilently(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Open a new database connection with the given settings.
     *
     * @param driver the driver class name
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @return the database connection
     * @throws SQLException on failure
     */
    public static Connection getConnection(String driver, String url,
            String user, String password) throws SQLException {
        return getConnection(driver, url, user, password, null, false);
    }

    /**
     * Open a new database connection with the given settings.
     *
     * @param driver the driver class name
     * @param url the database URL
     * @param user the user name or {@code null}
     * @param password the password or {@code null}
     * @param networkConnectionInfo the network connection information, or {@code null}
     * @param forbidCreation whether database creation is forbidden
     * @return the database connection
     * @throws SQLException on failure
     */
    public static Connection getConnection(String driver, String url, String user, String password,
            NetworkConnectionInfo networkConnectionInfo, boolean forbidCreation) throws SQLException {
        if (url.startsWith(Constants.START_URL)) {
            JdbcConnection connection = new JdbcConnection(url, null, user, password, forbidCreation);
            if (networkConnectionInfo != null) {
                connection.getSession().setNetworkConnectionInfo(networkConnectionInfo);
            }
            return connection;
        }
        if (StringUtils.isNullOrEmpty(driver)) {
            JdbcUtils.load(url);
        } else {
            Class<?> d = loadUserClass(driver);
            try {
                if (java.sql.Driver.class.isAssignableFrom(d)) {
                    Driver driverInstance = (Driver) d.getDeclaredConstructor().newInstance();
                    Properties prop = new Properties();
                    if (user != null) {
                        prop.setProperty("user", user);
                    }
                    if (password != null) {
                        prop.setProperty("password", password);
                    }
                    /*
                     * fix issue #695 with drivers with the same jdbc
                     * subprotocol in classpath of jdbc drivers (as example
                     * redshift and postgresql drivers)
                     */
                    Connection connection = driverInstance.connect(url, prop);
                    if (connection != null) {
                        return connection;
                    }
                    throw new SQLException("Driver " + driver + " is not suitable for " + url, "08001");
                } else if (javax.naming.Context.class.isAssignableFrom(d)) {
                    if (!url.startsWith("java:")) {
                        throw new SQLException("Only java scheme is supported for JNDI lookups", "08001");
                    }
                    // JNDI context
                    Context context = (Context) d.getDeclaredConstructor().newInstance();
                    DataSource ds = (DataSource) context.lookup(url);
                    if (StringUtils.isNullOrEmpty(user) && StringUtils.isNullOrEmpty(password)) {
                        return ds.getConnection();
                    }
                    return ds.getConnection(user, password);
                }
            } catch (Exception e) {
                throw DbException.toSQLException(e);
            }
            // don't know, but maybe it loaded a JDBC Driver
        }
        return DriverManager.getConnection(url, user, password);
    }

    /**
     * Get the driver class name for the given URL, or null if the URL is
     * unknown.
     *
     * @param url the database URL
     * @return the driver class name
     */
    public static String getDriver(String url) {
        if (url.startsWith("jdbc:")) {
            url = url.substring("jdbc:".length());
            for (int i = 0; i < DRIVERS.length; i += 2) {
                String prefix = DRIVERS[i];
                if (url.startsWith(prefix)) {
                    return DRIVERS[i + 1];
                }
            }
        }
        return null;
    }

    /**
     * Load the driver class for the given URL, if the database URL is known.
     *
     * @param url the database URL
     */
    public static void load(String url) {
        String driver = getDriver(url);
        if (driver != null) {
            loadUserClass(driver);
        }
    }

    /**
     * Serialize the object to a byte array, using the serializer specified by
     * the connection info if set, or the default serializer.
     *
     * @param obj the object to serialize
     * @param javaObjectSerializer the object serializer (may be null)
     * @return the byte array
     */
    public static byte[] serialize(Object obj, JavaObjectSerializer javaObjectSerializer) {
        try {
            if (javaObjectSerializer != null) {
                return javaObjectSerializer.serialize(obj);
            }
            if (serializer != null) {
                return serializer.serialize(obj);
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(obj);
            return out.toByteArray();
        } catch (Throwable e) {
            throw DbException.get(ErrorCode.SERIALIZATION_FAILED_1, e, e.toString());
        }
    }

    /**
     * De-serialize the byte array to an object, eventually using the serializer
     * specified by the connection info.
     *
     * @param data the byte array
     * @param javaObjectSerializer the object serializer (may be null)
     * @return the object
     * @throws DbException if serialization fails
     */
    public static Object deserialize(byte[] data, JavaObjectSerializer javaObjectSerializer) {
        try {
            if (javaObjectSerializer != null) {
                return javaObjectSerializer.deserialize(data);
            }
            if (serializer != null) {
                return serializer.deserialize(data);
            }
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            ObjectInputStream is;
            if (SysProperties.USE_THREAD_CONTEXT_CLASS_LOADER) {
                final ClassLoader loader = Thread.currentThread().getContextClassLoader();
                is = new ObjectInputStream(in) {
                    @Override
                    protected Class<?> resolveClass(ObjectStreamClass desc)
                            throws IOException, ClassNotFoundException {
                        try {
                            return Class.forName(desc.getName(), true, loader);
                        } catch (ClassNotFoundException e) {
                            return super.resolveClass(desc);
                        }
                    }
                };
            } else {
                is = new ObjectInputStream(in);
            }
            return is.readObject();
        } catch (Throwable e) {
            throw DbException.get(ErrorCode.DESERIALIZATION_FAILED_1, e, e.toString());
        }
    }

    /**
     * De-serialize the byte array to a UUID object. This method is called on
     * the server side where regular de-serialization of user-supplied Java
     * objects may create a security hole if object was maliciously crafted.
     * Unlike {@link #deserialize(byte[], JavaObjectSerializer)}, this method
     * does not try to de-serialize instances of other classes.
     *
     * @param data the byte array
     * @return the UUID object
     * @throws DbException if serialization fails
     */
    public static ValueUuid deserializeUuid(byte[] data) {
        if (data.length == 80 && Arrays.mismatch(data, 0, 64, UUID_PREFIX, 0, 64) < 0) {
            return ValueUuid.get((long) LONG_VH_BE.get(data, 72), (long) LONG_VH_BE.get(data, 64));
        }
        throw DbException.get(ErrorCode.DESERIALIZATION_FAILED_1, "Is not a UUID");
    }

    /**
     * Set a value as a parameter in a prepared statement.
     *
     * @param prep the prepared statement
     * @param parameterIndex the parameter index
     * @param value the value
     * @param conn the own connection
     * @throws SQLException on failure
     */
    public static void set(PreparedStatement prep, int parameterIndex, Value value, JdbcConnection conn)
            throws SQLException {
        if (prep instanceof JdbcPreparedStatement) {
            if (value instanceof ValueLob) {
                setLob(prep, parameterIndex, (ValueLob) value);
            } else {
                prep.setObject(parameterIndex, value);
            }
        } else {
            setOther(prep, parameterIndex, value, conn);
        }
    }

    private static void setOther(PreparedStatement prep, int parameterIndex, Value value, JdbcConnection conn)
                throws SQLException {
        int valueType = value.getValueType();
        switch (valueType) {
        case Value.NULL:
            prep.setNull(parameterIndex, Types.NULL);
            break;
        case Value.BOOLEAN:
            prep.setBoolean(parameterIndex, value.getBoolean());
            break;
        case Value.TINYINT:
            prep.setByte(parameterIndex, value.getByte());
            break;
        case Value.SMALLINT:
            prep.setShort(parameterIndex, value.getShort());
            break;
        case Value.INTEGER:
            prep.setInt(parameterIndex, value.getInt());
            break;
        case Value.BIGINT:
            prep.setLong(parameterIndex, value.getLong());
            break;
        case Value.NUMERIC:
        case Value.DECFLOAT:
            prep.setBigDecimal(parameterIndex, value.getBigDecimal());
            break;
        case Value.DOUBLE:
            prep.setDouble(parameterIndex, value.getDouble());
            break;
        case Value.REAL:
            prep.setFloat(parameterIndex, value.getFloat());
            break;
        case Value.TIME:
            try {
                prep.setObject(parameterIndex, JSR310Utils.valueToLocalTime(value, null), Types.TIME);
            } catch (SQLException ignore) {
                prep.setTime(parameterIndex, LegacyDateTimeUtils.toTime(null, null, value));
            }
            break;
        case Value.DATE:
            try {
                prep.setObject(parameterIndex, JSR310Utils.valueToLocalDate(value, null), Types.DATE);
            } catch (SQLException ignore) {
                prep.setDate(parameterIndex, LegacyDateTimeUtils.toDate(null, null, value));
            }
            break;
        case Value.TIMESTAMP:
            try {
                prep.setObject(parameterIndex, JSR310Utils.valueToLocalDateTime(value, null), Types.TIMESTAMP);
            } catch (SQLException ignore) {
                prep.setTimestamp(parameterIndex, LegacyDateTimeUtils.toTimestamp(null, null, value));
            }
            break;
        case Value.VARBINARY:
        case Value.BINARY:
        case Value.GEOMETRY:
        case Value.JSON:
            prep.setBytes(parameterIndex, value.getBytesNoCopy());
            break;
        case Value.VARCHAR:
        case Value.VARCHAR_IGNORECASE:
        case Value.ENUM:
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            prep.setString(parameterIndex, value.getString());
            break;
        case Value.BLOB:
        case Value.CLOB:
            setLob(prep, parameterIndex, (ValueLob) value);
            break;
        case Value.ARRAY:
            prep.setArray(parameterIndex, prep.getConnection().createArrayOf("NULL",
                    (Object[]) ValueToObjectConverter.valueToDefaultObject(value, conn, true)));
            break;
        case Value.JAVA_OBJECT:
            prep.setObject(parameterIndex,
                    JdbcUtils.deserialize(value.getBytesNoCopy(), conn.getJavaObjectSerializer()),
                    Types.JAVA_OBJECT);
            break;
        case Value.UUID:
            prep.setBytes(parameterIndex, value.getBytes());
            break;
        case Value.CHAR:
            try {
                prep.setObject(parameterIndex, value.getString(), Types.CHAR);
            } catch (SQLException ignore) {
                prep.setString(parameterIndex, value.getString());
            }
            break;
        case Value.TIMESTAMP_TZ:
            try {
                prep.setObject(parameterIndex, JSR310Utils.valueToOffsetDateTime(value, null),
                        Types.TIMESTAMP_WITH_TIMEZONE);
                return;
            } catch (SQLException ignore) {
                prep.setString(parameterIndex, value.getString());
            }
            break;
        case Value.TIME_TZ:
            try {
                prep.setObject(parameterIndex, JSR310Utils.valueToOffsetTime(value, null), Types.TIME_WITH_TIMEZONE);
                return;
            } catch (SQLException ignore) {
                prep.setString(parameterIndex, value.getString());
            }
            break;
        default:
            throw DbException.getUnsupportedException(Value.getTypeName(valueType));
        }
    }

    private static void setLob(PreparedStatement prep, int parameterIndex, ValueLob value) throws SQLException {
        if (value.getValueType() == Value.BLOB) {
            long p = value.octetLength();
            prep.setBinaryStream(parameterIndex, value.getInputStream(), p > Integer.MAX_VALUE ? -1 : (int) p);
        } else {
            long p = value.charLength();
            prep.setCharacterStream(parameterIndex, value.getReader(), p > Integer.MAX_VALUE ? -1 : (int) p);
        }
    }

    /**
     * Get metadata from the database.
     *
     * @param conn the connection
     * @param sql the SQL statement
     * @return the metadata
     * @throws SQLException on failure
     */
    public static ResultSet getMetaResultSet(Connection conn, String sql)
            throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        if (isBuiltIn(sql, "@best_row_identifier")) {
            String[] p = split(sql);
            int scale = p[4] == null ? 0 : Integer.parseInt(p[4]);
            boolean nullable = Boolean.parseBoolean(p[5]);
            return meta.getBestRowIdentifier(p[1], p[2], p[3], scale, nullable);
        } else if (isBuiltIn(sql, "@catalogs")) {
            return meta.getCatalogs();
        } else if (isBuiltIn(sql, "@columns")) {
            String[] p = split(sql);
            return meta.getColumns(p[1], p[2], p[3], p[4]);
        } else if (isBuiltIn(sql, "@column_privileges")) {
            String[] p = split(sql);
            return meta.getColumnPrivileges(p[1], p[2], p[3], p[4]);
        } else if (isBuiltIn(sql, "@cross_references")) {
            String[] p = split(sql);
            return meta.getCrossReference(p[1], p[2], p[3], p[4], p[5], p[6]);
        } else if (isBuiltIn(sql, "@exported_keys")) {
            String[] p = split(sql);
            return meta.getExportedKeys(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@imported_keys")) {
            String[] p = split(sql);
            return meta.getImportedKeys(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@index_info")) {
            String[] p = split(sql);
            boolean unique = Boolean.parseBoolean(p[4]);
            boolean approx = Boolean.parseBoolean(p[5]);
            return meta.getIndexInfo(p[1], p[2], p[3], unique, approx);
        } else if (isBuiltIn(sql, "@primary_keys")) {
            String[] p = split(sql);
            return meta.getPrimaryKeys(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@procedures")) {
            String[] p = split(sql);
            return meta.getProcedures(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@procedure_columns")) {
            String[] p = split(sql);
            return meta.getProcedureColumns(p[1], p[2], p[3], p[4]);
        } else if (isBuiltIn(sql, "@schemas")) {
            return meta.getSchemas();
        } else if (isBuiltIn(sql, "@tables")) {
            String[] p = split(sql);
            String[] types = p[4] == null ? null : StringUtils.arraySplit(p[4], ',', false);
            return meta.getTables(p[1], p[2], p[3], types);
        } else if (isBuiltIn(sql, "@table_privileges")) {
            String[] p = split(sql);
            return meta.getTablePrivileges(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@table_types")) {
            return meta.getTableTypes();
        } else if (isBuiltIn(sql, "@type_info")) {
            return meta.getTypeInfo();
        } else if (isBuiltIn(sql, "@udts")) {
            String[] p = split(sql);
            int[] types;
            if (p[4] == null) {
                types = null;
            } else {
                String[] t = StringUtils.arraySplit(p[4], ',', false);
                types = new int[t.length];
                for (int i = 0; i < t.length; i++) {
                    types[i] = Integer.parseInt(t[i]);
                }
            }
            return meta.getUDTs(p[1], p[2], p[3], types);
        } else if (isBuiltIn(sql, "@version_columns")) {
            String[] p = split(sql);
            return meta.getVersionColumns(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@memory")) {
            SimpleResultSet rs = new SimpleResultSet();
            rs.addColumn("Type", Types.VARCHAR, 0, 0);
            rs.addColumn("KB", Types.VARCHAR, 0, 0);
            rs.addRow("Used Memory", Long.toString(Utils.getMemoryUsed()));
            rs.addRow("Free Memory", Long.toString(Utils.getMemoryFree()));
            return rs;
        } else if (isBuiltIn(sql, "@info")) {
            SimpleResultSet rs = new SimpleResultSet();
            rs.addColumn("KEY", Types.VARCHAR, 0, 0);
            rs.addColumn("VALUE", Types.VARCHAR, 0, 0);
            rs.addRow("conn.getCatalog", conn.getCatalog());
            rs.addRow("conn.getAutoCommit", Boolean.toString(conn.getAutoCommit()));
            rs.addRow("conn.getTransactionIsolation", Integer.toString(conn.getTransactionIsolation()));
            rs.addRow("conn.getWarnings", String.valueOf(conn.getWarnings()));
            String map;
            try {
                map = String.valueOf(conn.getTypeMap());
            } catch (SQLException e) {
                map = e.toString();
            }
            rs.addRow("conn.getTypeMap", map);
            rs.addRow("conn.isReadOnly", Boolean.toString(conn.isReadOnly()));
            rs.addRow("conn.getHoldability", Integer.toString(conn.getHoldability()));
            addDatabaseMetaData(rs, meta);
            return rs;
        } else if (isBuiltIn(sql, "@attributes")) {
            String[] p = split(sql);
            return meta.getAttributes(p[1], p[2], p[3], p[4]);
        } else if (isBuiltIn(sql, "@super_tables")) {
            String[] p = split(sql);
            return meta.getSuperTables(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@super_types")) {
            String[] p = split(sql);
            return meta.getSuperTypes(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@pseudo_columns")) {
            String[] p = split(sql);
            return meta.getPseudoColumns(p[1], p[2], p[3], p[4]);
        }
        return null;
    }

    private static void addDatabaseMetaData(SimpleResultSet rs,
            DatabaseMetaData meta) {
        Method[] methods = DatabaseMetaData.class.getDeclaredMethods();
        Arrays.sort(methods, Comparator.comparing(Method::toString));
        for (Method m : methods) {
            if (m.getParameterTypes().length == 0) {
                try {
                    Object o = m.invoke(meta);
                    rs.addRow("meta." + m.getName(), String.valueOf(o));
                } catch (InvocationTargetException e) {
                    rs.addRow("meta." + m.getName(), e.getTargetException().toString());
                } catch (Exception e) {
                    rs.addRow("meta." + m.getName(), e.toString());
                }
            }
        }
    }

    /**
     * Check is the SQL string starts with a prefix (case insensitive).
     *
     * @param sql the SQL statement
     * @param builtIn the prefix
     * @return true if yes
     */
    public static boolean isBuiltIn(String sql, String builtIn) {
        return sql.regionMatches(true, 0, builtIn, 0, builtIn.length());
    }

    /**
     * Split the string using the space separator into at least 10 entries.
     *
     * @param s the string
     * @return the array
     */
    public static String[] split(String s) {
        String[] t = StringUtils.arraySplit(s, ' ', true);
        String[] list = new String[Math.max(10, t.length)];
        System.arraycopy(t, 0, list, 0, t.length);
        for (int i = 0; i < list.length; i++) {
            if ("null".equals(list[i])) {
                list[i] = null;
            }
        }
        return list;
    }
}
